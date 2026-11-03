/*
 * Copyright 2023 Mariot Chauvin
 *
 * Mariot Chauvin licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package play.filters.brotli

import java.util.function.BiFunction
import java.util.Locale
import javax.inject.{ Provider, Inject, Singleton }

import org.apache.pekko.stream.{ OverflowStrategy, FlowShape, Materializer }
import org.apache.pekko.stream.scaladsl._
import org.apache.pekko.util.ByteString
import com.typesafe.config.ConfigMemorySize
import play.api.http.{ HttpProtocol, MediaType }
import play.api.inject.Module
import play.api.{ Environment, Configuration }
import play.api.mvc._
import play.api.libs.streams.BrotliFlow
import org.apache.pekko.stream.io.compression.brotli.BrotliCompressor
import scala.concurrent.{ Future, ExecutionContextExecutor}
import scala.concurrent.duration._
import play.api.http.{ HttpChunk, HttpEntity, Status }
import scala.jdk.FunctionConverters._

import com.aayushatharva.brotli4j.encoder.Encoder;

/**
 * A brotli filter.
 *
 * This filter may compress with brotli the responses for any requests that aren't HEAD requests and specify an accept encoding of brotli.
 *
 * It won't compresss under the following conditions:
 *
 * - The response code is 204 or 304 (these codes MUST NOT contain a body, and an empty compressed response is 20 bytes
 * long)
 * - The response already defines a Content-Encoding header
 * - The size of the response body is equal or smaller than a given threshold. If the body size cannot be determined,
 *   then it is assumed the response is over the threshold
 * - A custom shouldBrotli function is supplied and it returns false
 *
 * Since compressing changes the content length of the response, this filter may do some buffering - it will buffer any
 * streamed responses that define a content length less than the configured chunked threshold.  Responses that are
 * greater in length, or that don't define a content length, will not be buffered, but will be sent as chunked
 * responses.
 */
@Singleton
class BrotliFilter @Inject() (config: BrotliFilterConfig)(implicit mat: Materializer) extends EssentialFilter {


  {
    brotli4s.Brotli.ensureAvailability()
  }

  import play.api.http.HeaderNames._

  def this(quality: Int = 5,
    bufferSize: Int = 8192,
    chunkedThreshold: Int = 102400,
    shouldBrotli: (RequestHeader, Result) => Boolean = (_, _) => true,
    threshold: Int = 0,
    window: Int = -1,
    mode: Encoder.Mode = Encoder.Mode.GENERIC,
    flushInterval: FiniteDuration = 5.millis)(implicit mat: Materializer) = {
      this(BrotliFilterConfig(quality, bufferSize, chunkedThreshold, shouldBrotli, threshold, window, mode, flushInterval))
    }


  def apply(next: EssentialAction) = new EssentialAction {
    implicit val ec: ExecutionContextExecutor = mat.executionContext
    def apply(request: RequestHeader) = {
      if (mayCompress(request)) {
        next(request).mapFuture(result => handleResult(request, result))
      } else {
        next(request)
      }
    }
  }

  private def handleResult(request: RequestHeader, result: Result): Future[Result] = {
    if (shouldCompress(result) && config.shouldBrotli(request, result)) {
      implicit val ec = mat.executionContext
      val header = result.header.copy(headers = setupHeader(result.header.headers))

      result.body match {

        case HttpEntity.Strict(data, contentType) =>
          Future.successful(result.copy(header = header, body = compressStrictEntity(data, contentType)))


        case entity @ HttpEntity.Streamed(_, Some(contentLength), contentType) if contentLength <= config.chunkedThreshold =>
          // It's below the chunked threshold, so buffer then compress and send
          entity.consumeData.map { data =>
            result.copy(header = header, body = compressStrictEntity(data, contentType))
          }

        case HttpEntity.Streamed(data, _, contentType) if request.version == HttpProtocol.HTTP_1_0 =>
          // It's above the chunked threshold, but we can't chunk it because we're using HTTP 1.0.
          // Instead, we use a close delimited body (ie, regular body with no content length)
          val compressed = data.via(createBrotliFlow)
          Future.successful(
            result.copy(header = header, body = HttpEntity.Streamed(compressed, None, contentType))
          )

        case HttpEntity.Streamed(data, _, contentType) =>
          // It's above the chunked threshold, compress through the brotli flow, and send as chunked
          val compressed = data.via(createBrotliFlow).map(d => HttpChunk.Chunk(d))
          Future.successful(result.copy(header = header, body = HttpEntity.Chunked(compressed, contentType)))

        case HttpEntity.Chunked(chunks, contentType) =>
          val wrappedFlow = Flow.fromGraph(GraphDSL.create[FlowShape[HttpChunk, HttpChunk]]() { implicit builder =>
            import GraphDSL.Implicits._

            val extractChunks = Flow[HttpChunk] collect { case HttpChunk.Chunk(data) => data }

            val createChunks = Flow[ByteString].map[HttpChunk](HttpChunk.Chunk.apply)
            val filterLastChunk = Flow[HttpChunk]
              .filter(_.isInstanceOf[HttpChunk.LastChunk])
              // Since we're doing a merge by concatenating, the filter last chunk won't receive demand until the brotli
              // flow is finished. But the broadcast won't start broadcasting until both flows start demanding. So we
              // put a buffer of one in to ensure the filter last chunk flow demands from the broadcast.
              .buffer(1, OverflowStrategy.backpressure)

            val broadcast = builder.add(Broadcast[HttpChunk](2))
            val concat = builder.add(Concat[HttpChunk]())

            // Broadcast the stream through two separate flows, one that collects chunks and turns them into
            // ByteStrings, sends them through the Brotli flow as they come, and then turns them back into chunks,
            // the other that just allows the last chunk through. Then concat those two flows together.
            broadcast.out(0) ~> extractChunks ~> createBrotliFlow ~> createChunks ~> concat.in(0)
            broadcast.out(1) ~> filterLastChunk ~> concat.in(1)

            new FlowShape(broadcast.in, concat.out)
          })
          Future.successful(result.copy(header = header, body = HttpEntity.Chunked(chunks via wrappedFlow, contentType)))
      }
    } else {
      Future.successful(result)
    }
  }


  private def createBrotliFlow: Flow[ByteString, ByteString, _] =
    BrotliFlow.brotli(config.bufferSize, config.encoderParameters, config.flushInterval)

  private def compressStrictEntity(data: ByteString, contentType: Option[String]) =
    HttpEntity.Strict(BrotliCompressor.compress(data, config.encoderParameters), contentType)

  /**
   * Whether this request may be compressed.
   */
  private def mayCompress(request: RequestHeader) =
    request.method != "HEAD" && brotliIsAcceptedAndPreferredBy(request)

  // TODO modify
  private def brotliIsAcceptedAndPreferredBy(request: RequestHeader) = {
    val codings = play.api.mvc.RequestHeader.acceptHeader(request.headers, ACCEPT_ENCODING)
    def explicitQValue(coding: String) = codings collectFirst { case (q, c) if c equalsIgnoreCase coding => q }
    def defaultQValue(coding: String) = if (coding == "identity") 0.001d else 0d
    def qvalue(coding: String) = explicitQValue(coding) orElse explicitQValue("*") getOrElse defaultQValue(coding)

    qvalue("br") > 0d && qvalue("br") >= qvalue("identity")
  }

  /**
   * Whether this response should be compressed.  Responses that may not contain content won't be compressed, nor will
   * responses that already define a content encoding.  Empty responses also shouldn't be compressed, as they will
   * actually always get bigger.  Also responses whose body size are equal or lower than the given byte threshold won't
   * be compressed, because it's assumed they end up being bigger than the original body.
   */
  private def shouldCompress(result: Result) = isAllowedContent(result.header) &&
    isNotAlreadyCompressed(result.header) &&
    !result.body.isKnownEmpty &&
    result.body.contentLength.forall(_ > config.threshold)

  /**
   * Certain response codes are forbidden by the HTTP spec to contain content, but a gzipped response always contains
   * a minimum of 20 bytes, even for empty responses.
   */
  private def isAllowedContent(header: ResponseHeader) = header.status != Status.NO_CONTENT && header.status != Status.NOT_MODIFIED

  /**
   * Of course, we don't want to double compress responses
   */
  private def isNotAlreadyCompressed(header: ResponseHeader) = header.headers.get(CONTENT_ENCODING).isEmpty

  // TODO modify
  private def setupHeader(header: Map[String, String]): Map[String, String] = {
    header + (CONTENT_ENCODING -> "br") + addToVaryHeader(header, VARY, ACCEPT_ENCODING)
  }

  /**
   * There may be an existing Vary value, which we must add to (comma separated)
   */
  private def addToVaryHeader(existingHeaders: Map[String, String], headerName: String, headerValue: String): (String, String) = {
    existingHeaders.get(headerName) match {
      case None => (headerName, headerValue)
      case Some(existing) if existing.split(",").exists(_.trim.equalsIgnoreCase(headerValue)) => (headerName, existing)
      case Some(existing) => (headerName, s"$existing,$headerValue")
    }
  }
}

/**
 * Configuration for the brotli filter
 *
 * @param quality The compression-speed vs compression-density tradeoffs. The higher the quality, the slower the compression.
 *                Range is 0 to 11, or -1 for the default of the brotli encoder (11)
 * @param bufferSize The maximum amount of data to send to the brotli compressor in one go. Use `0` or `Int.MaxValue` to
 *                   disable the buffering.
 * @param chunkedThreshold The content length threshold, after which the filter will switch to chunking the result.
 * @param shouldBrotli Whether the given request/result should be compressed with brotli.  This can be used, for example, to implement
 *                   black/white lists for compressing by content type.
 * @param threshold The byte threshold for the response body size which controls if a response should be compressed.
 *                  Responses of this size or smaller are not compressed. If the body size cannot be determined, it is
 *                  assumed to be over the threshold.
 * @param window The log2 of the sliding window size. Range is 10 to 24, or -1 for the default of the brotli encoder.
 * @param mode The kind of content the encoder is tuned for: generic, text or font.
 * @param flushInterval The longest time that compressed data of a streamed or chunked response waits in the encoder. The
 *                      encoder is flushed only when its oldest data has waited that long, and at the end of the response:
 *                      flushing after every chunk, as the gzip filter does, costs compressed size and CPU when the chunks are
 *                      small. Zero flushes after every chunk.
 */
case class BrotliFilterConfig(quality: Int = 5,
    bufferSize: Int = 8192,
    chunkedThreshold: Int = 102400,
    shouldBrotli: (RequestHeader, Result) => Boolean = (_, _) => true,
    threshold: Int = 0,
    window: Int = -1,
    mode: Encoder.Mode = Encoder.Mode.GENERIC,
    flushInterval: FiniteDuration = 5.millis) {

  if (quality < -1 || quality > 11) {
    throw new IllegalArgumentException(
      s"Invalid brotli quality $quality: it must be between 0 and 11, or -1 for the encoder default")
  }
  if (window != -1 && (window < 10 || window > 24)) {
    throw new IllegalArgumentException(
      s"Invalid brotli window $window: it must be between 10 and 24, or -1 for the encoder default")
  }
  if (bufferSize < 0) {
    throw new IllegalArgumentException(s"Invalid brotli bufferSize $bufferSize: it must not be negative")
  }
  if (threshold < 0) {
    throw new IllegalArgumentException(s"Invalid brotli threshold $threshold: it must not be negative")
  }
  if (flushInterval.length < 0) {
    throw new IllegalArgumentException(s"Invalid brotli flushInterval $flushInterval: it must not be negative")
  }

  // alternate constructor and builder methods for Java
  def this() = this(shouldBrotli = (_, _) => true)

  def withShouldBrotli(shouldBrotli: (RequestHeader, Result) => Boolean): BrotliFilterConfig = copy(shouldBrotli = shouldBrotli)

  def withShouldBrotli(shouldBrotli: BiFunction[play.mvc.Http.RequestHeader, play.mvc.Result, Boolean]): BrotliFilterConfig =
    withShouldBrotli((req: RequestHeader, res: Result) => shouldBrotli.asScala(req.asJava, res.asJava))

  def withChunkedThreshold(threshold: Int): BrotliFilterConfig = copy(chunkedThreshold = threshold)

  def withBufferSize(size: Int): BrotliFilterConfig = copy(bufferSize = size)

  def withQuality(q: Int): BrotliFilterConfig = copy(quality = q)

  def withThreshold(threshold: Int): BrotliFilterConfig = copy(threshold = threshold)

  def withWindow(window: Int): BrotliFilterConfig = copy(window = window)

  def withMode(mode: Encoder.Mode): BrotliFilterConfig = copy(mode = mode)

  def withFlushInterval(interval: FiniteDuration): BrotliFilterConfig = copy(flushInterval = interval)

  /**
   * The parameters for the brotli encoder. brotli4j parameters are mutable, so each call returns a new instance.
   */
  def encoderParameters: Encoder.Parameters =
    new Encoder.Parameters().setQuality(quality).setWindow(window).setMode(mode)
}

object BrotliFilterConfig {

  def fromConfiguration(conf: Configuration): BrotliFilterConfig = {

    val config = conf.get[Configuration]("play.filters.brotli")

    BrotliFilterConfig(
      quality = config.get[Int]("quality"),
      bufferSize = memorySize(config, "bufferSize"),
      chunkedThreshold = memorySize(config, "chunkedThreshold"),
      shouldBrotli = contentTypeFilter(
        parseMediaTypes(config, "contentType.whiteList"),
        parseMediaTypes(config, "contentType.blackList")
      ),
      threshold = memorySize(config, "threshold"),
      window = config.get[Int]("window"),
      mode = parseMode(config.get[String]("mode")),
      flushInterval = config.get[FiniteDuration]("flushInterval")
    )
  }

  private def memorySize(config: Configuration, key: String): Int = {
    val bytes = config.get[ConfigMemorySize](key).toBytes
    if (bytes > Int.MaxValue) {
      throw new IllegalArgumentException(s"Invalid brotli $key $bytes bytes: it must not be larger than ${Int.MaxValue} bytes")
    }
    bytes.toInt
  }

  private def parseMode(value: String): Encoder.Mode = {
    val modes = Encoder.Mode.values().toSeq
    modes.find(_.name.equalsIgnoreCase(value)).getOrElse {
      val allowed = modes.map(_.name.toLowerCase(Locale.ROOT)).mkString(", ")
      throw new IllegalArgumentException(s"Invalid brotli mode '$value': it must be one of $allowed")
    }
  }

  private def parseMediaTypes(config: Configuration, key: String): Seq[MediaType] =
    config.get[Seq[String]](key).map {
      case MediaType.parse(mediaType) => mediaType
      case invalid =>
        throw new IllegalArgumentException(
          s"Invalid media type '$invalid' in play.filters.brotli.$key: expected 'type/subtype', 'type/*' or '*/*'")
    }

  /**
   * Whether a response is compressed, from its content type. When the white list is not empty, only the content types
   * it matches are compressed, and a response without a content type is not. Otherwise every content type is
   * compressed except those matched by the black list. The matching ignores the case and the parameters of the
   * content type, and a `*` in the type or the subtype of a configured media type matches anything.
   */
  private[brotli] def contentTypeFilter(whiteList: Seq[MediaType], blackList: Seq[MediaType]): (RequestHeader, Result) => Boolean = {
    def captured(value: String, mask: String): Boolean = mask == "*" || value.equalsIgnoreCase(mask)
    def matches(outgoing: MediaType, mask: MediaType): Boolean =
      captured(outgoing.mediaType, mask.mediaType) && captured(outgoing.mediaSubType, mask.mediaSubType)

    (_, result) => {
      val outgoing = result.body.contentType.flatMap {
        case MediaType.parse(mediaType) => Some(mediaType)
        case _                          => None
      }
      if (whiteList.nonEmpty) outgoing.exists(mediaType => whiteList.exists(matches(mediaType, _)))
      else outgoing.forall(mediaType => !blackList.exists(matches(mediaType, _)))
    }
  }
}

/**
 * The brotli filter configuration provider.
 */
@Singleton
class BrotliFilterConfigProvider @Inject() (config: Configuration) extends Provider[BrotliFilterConfig] {
  lazy val get = BrotliFilterConfig.fromConfiguration(config)
}


/**
 * The brotli filter module.
 */
class BrotliFilterModule extends Module {

  def bindings(environment: Environment, configuration: Configuration) = {
    Seq(
      bind[BrotliFilterConfig].toProvider[BrotliFilterConfigProvider],
      bind[BrotliFilter].toSelf
    )
  }
}

/**
 * The brotli filter components.
 */
trait BrotliFilterComponents {
  def configuration: Configuration
  def materializer: Materializer

  lazy val brotliFilterConfig: BrotliFilterConfig = BrotliFilterConfig.fromConfiguration(configuration)
  lazy val brotliFilter: BrotliFilter = new BrotliFilter(brotliFilterConfig)(materializer)
}
