/*
 * Copyright (C) 2009-2016 Lightbend Inc. <https://www.lightbend.com> and Mariot Chauvin <mariot.chauvin@gmail.com>
 */
package play.filters.brotli

import javax.inject.Inject

import akka.stream.Materializer
import akka.stream.scaladsl.{ Sink, Source }
import akka.util.ByteString
import play.api.Application
import play.api.http.{ HttpChunk, HttpEntity, HttpFilters, HttpProtocol }
import play.api.inject._
import play.api.inject.guice.GuiceApplicationBuilder
import play.api.routing.{Router, SimpleRouterImpl}
import play.api.test._
import play.api.mvc.{ AnyContentAsEmpty, Action, DefaultActionBuilder, Headers, Result }
import play.api.mvc.Results._
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.file.Path
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import akka.stream.scaladsl.FileIO
import org.apache.commons.io.IOUtils
import java.util.concurrent.{ LinkedBlockingQueue, TimeUnit }

import scala.concurrent.{ Await, Future, Promise }
import scala.concurrent.duration._
import scala.util.Random
import org.specs2.matcher.{DataTables, MatchResult}

import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import play.api.mvc.Cookie
import play.api.libs.streams.BrotliFlow
import akka.stream.io.compression.brotli.BrotliCompressor

object BrotliFilterSpec extends PlaySpecification with DataTables {

  sequential

  "The BrotliFilter" should {

    "compress responses with brotli" in withApplication(Ok("hello")) { implicit app =>
      checkCompressedBody(makeBrotliRequest(app), "hello")(app.materializer)
    }

    """compress a response with brotli if (and only if) it is accepted and preferred by the request.
      |Although not explicitly mentioned in RFC 2616 sect. 14.3, the default qvalue
      |is assumed to be 1 for all mentioned codings. If no "*" is present, unmentioned
      |codings are assigned a qvalue of 0, except the identity coding which gets q=0.001,
      |which is the lowest possible acceptable qvalue.
      |This seems to be the most consistent behaviour with respect to the other "accept"
      |header fields described in sect 14.1-5.""".stripMargin in withApplication(Ok("meep")) { implicit app =>

      val (plain, bred) = (None, Some("br"))

      "Accept-Encoding of request" || "Response" |
        //------------------------------------++------------+
        "br" !! bred |
        "compress,br" !! bred |
        "compress, br" !! bred |
        "br,compress" !! bred |
        "deflate, br,compress" !! bred |
        "br, compress" !! bred |
        "identity, br, compress" !! bred |
        "*" !! bred |
        "*;q=0" !! plain |
        "*; q=0" !! plain |
        "*;q=0.000" !! plain |
        "br;q=0" !! plain |
        "br; q=0.00" !! plain |
        "*;q=0, br" !! bred |
        "compress;q=0.1, *;q=0, br" !! bred |
        "compress;q=0.1, *;q=0, br;q=0.005" !! bred |
        "compress, br;q=0.001" !! bred |
        "compress, br;q=0.002" !! bred |
        "compress;q=1, *;q=0, br;q=0.000" !! plain |
        "compress;q=1, *;q=0" !! plain |
        "identity" !! plain |
        "br;q=0.5, identity" !! plain |
        "br;q=0.5, identity;q=1" !! plain |
        "br;q=0.6, identity;q=0.5" !! bred |
        "*;q=0.7, br;q=0.6, identity;q=0.4" !! bred |
        "" !! plain |> {

          (codings, expectedEncoding) =>
            header(CONTENT_ENCODING, requestAccepting(app, codings)) must be equalTo (expectedEncoding)
        }
    }

    "not brotli empty responses" in withApplication(Ok) { implicit app =>
      checkNotCompressed(makeBrotliRequest(app), "")(app.materializer)
    }

    "not brotli responses when not requested" in withApplication(Ok("hello")) { implicit app =>
      checkNotCompressed(route(app, FakeRequest()).get, "hello")(app.materializer)
    }

    "not brotli HEAD requests" in withApplication(Ok) { implicit app =>
      checkNotCompressed(route(app, FakeRequest("HEAD", "/").withHeaders(ACCEPT_ENCODING -> "br")).get, "")(app.materializer)
    }

    "not brotli no content responses" in withApplication(NoContent) { implicit app =>
      checkNotCompressed(makeBrotliRequest(app), "")(app.materializer)
    }

    "not brotli not modified responses" in withApplication(NotModified) { implicit app =>
      checkNotCompressed(makeBrotliRequest(app), "")(app.materializer)
    }

    "brotli chunked responses" in withApplication(Ok.chunked(Source(List("foo", "bar")))) { implicit app =>
      val result = makeBrotliRequest(app)

      checkCompressedBody(result, "foobar")(app.materializer)
      await(result).body must beAnInstanceOf[HttpEntity.Chunked]
    }

    "hold the compressed data of chunks that arrive quickly until the flush interval, and flush every chunk for an interval of 0" in {
      def dataChunks(flushInterval: String): Int = {
        val chunks = Source(List("one ", "two ", "three ", "four ")) // all there at once
        withApplication(Ok.chunked(chunks), settings = Seq("play.filters.brotli.flushInterval" -> flushInterval)) { app =>
          implicit val mat: Materializer = app.materializer
          val body = await(makeBrotliRequest(app)).body
          val received = Await.result(body.dataStream.runWith(Sink.seq), 5.seconds)
          uncompress(received.foldLeft(ByteString.empty)(_ ++ _)) must_== "one two three four "
          received.count(_.nonEmpty)
        }
      }
      // a long interval: the encoder keeps the data until the end of the stream, so there is little to send before it
      val held = dataChunks("10 seconds")
      val flushed = dataChunks("0")
      (flushed must be_>(held)) and (held must be_<=(2)) and (flushed must be_>=(4))
    }

    "stream a chunked response, compressing its chunks as they come instead of when the response ends" in {
      val secondChunk = Promise[String]()
      val chunks = Source.single("first chunk ").concat(Source.future(secondChunk.future))
      withApplication(Ok.chunked(chunks)) { app =>
        implicit val mat: Materializer = app.materializer
        val received = new LinkedBlockingQueue[ByteString]()
        val done = await(makeBrotliRequest(app)).body.dataStream.runForeach(chunk => received.add(chunk))
        // The second chunk does not exist yet: the compressed first chunk must be there anyway
        val first = Option(received.poll(5, TimeUnit.SECONDS))
        first must beSome
        secondChunk.success("second chunk")
        Await.result(done, 5.seconds)
        val rest = Iterator.continually(received.poll()).takeWhile(_ != null).toList
        val all = rest.foldLeft(first.get)(_ ++ _)
        uncompress(all) must_== "first chunk second chunk"
      }
    }

    val body = Random.nextString(1000)

    "not buffer more than the configured threshold" in withApplication(
      Ok.sendEntity(HttpEntity.Streamed(Source.single(ByteString(body)), Some(1000), None)), chunkedThreshold = 512) { implicit app =>
        val result = makeBrotliRequest(app)
        checkCompressedBody(result, body)(app.materializer)
        await(result).body must beAnInstanceOf[HttpEntity.Chunked]
      }
    

    "brotli a strict body even if it exceeds the threshold" in withApplication(Ok(body), chunkedThreshold = 512) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressedBody(result, body)(app.materializer)
      await(result).body must beAnInstanceOf[HttpEntity.Strict]
    }

    // Copy the resource to a temp file: with sbt 2 it is inside a jar, so it has no file system path.
    val path: Path = {
      val tmp = Files.createTempFile("bootstrap", ".min.css")
      tmp.toFile.deleteOnExit()
      val in = getClass.getResourceAsStream("/bootstrap.min.css")
      try Files.copy(in, tmp, StandardCopyOption.REPLACE_EXISTING)
      finally in.close()
      tmp
    }
    val source = akka.stream.scaladsl.FileIO.fromPath(path)
    val contentLength = Files.size(path)

    "brotli entire content for large files" in withApplication(
      Ok.sendEntity(HttpEntity.Streamed(source, Some(contentLength), Some("text/css"))), chunkedThreshold = 512) { implicit app =>
        val result = makeBrotliRequest(app)
        checkCompressedBodyLength(result, contentLength)(app.materializer)
        await(result).body must beAnInstanceOf[HttpEntity.Chunked]
    }

    "fit the window to a streamed body of known length, and keep the default window when the length is not known" in {
      def windowOf(entity: HttpEntity): Int =
        withApplication(Ok.sendEntity(entity), chunkedThreshold = 512) { app =>
          windowBits(contentAsBytes(makeBrotliRequest(app))(defaultAwaitTimeout, app.materializer))
        }
      val known = windowOf(HttpEntity.Streamed(source, Some(contentLength), Some("text/css")))
      val unknown = windowOf(HttpEntity.Streamed(source, None, Some("text/css")))
      (known must_== BrotliCompressor.windowFor(contentLength.toInt)) and (unknown must_== 22)
    }

    "send a streamed body above the threshold to an HTTP/1.0 client close delimited, with the window that fits it" in
      withApplication(Ok.sendEntity(HttpEntity.Streamed(source, Some(contentLength), Some("text/css"))), chunkedThreshold = 512) { app =>
        implicit val mat: Materializer = app.materializer
        val result = await(route(app, brotliRequest.withVersion(HttpProtocol.HTTP_1_0)).get)
        val bytes = contentAsBytes(Future.successful(result))
        (result.body must beAnInstanceOf[HttpEntity.Streamed]) and (result.body.contentLength must beNone) and
          (windowBits(bytes) must_== BrotliCompressor.windowFor(contentLength.toInt)) and
          (uncompress(bytes).length must_== contentLength.toInt)
      }

    "send the last chunk of a chunked response, with its trailers, after the compressed data, once" in {
      val chunks = Source(List(HttpChunk.Chunk(ByteString("foo")), HttpChunk.Chunk(ByteString("bar")), HttpChunk.LastChunk(Headers("X-Trailer" -> "yes"))))
      withApplication(Ok.sendEntity(HttpEntity.Chunked(chunks, None))) { app =>
        implicit val mat: Materializer = app.materializer
        val body = await(makeBrotliRequest(app)).body
        val received = Await.result(body.asInstanceOf[HttpEntity.Chunked].chunks.runWith(Sink.seq), 5.seconds)
        val data = received.collect { case HttpChunk.Chunk(d) => d }.foldLeft(ByteString.empty)(_ ++ _)
        (uncompress(data) must_== "foobar") and
          (received.last must_== HttpChunk.LastChunk(Headers("X-Trailer" -> "yes"))) and
          (received.count(_.isInstanceOf[HttpChunk.LastChunk]) must_== 1)
      }
    }

    "end a chunked response with its last chunk, even if the source does not complete after it" in {
      // A source that sends its last chunk and then stays open: the response must end with the last chunk
      val chunks = Source(List[HttpChunk](HttpChunk.Chunk(ByteString("foo")), HttpChunk.Chunk(ByteString("bar")), HttpChunk.LastChunk(Headers())))
        .concat(Source.maybe[HttpChunk])
      withApplication(Ok.sendEntity(HttpEntity.Chunked(chunks, None))) { app =>
        implicit val mat: Materializer = app.materializer
        val received = Await.result(await(makeBrotliRequest(app)).body.asInstanceOf[HttpEntity.Chunked].chunks.runWith(Sink.seq), 5.seconds)
        val data = received.collect { case HttpChunk.Chunk(d) => d }.foldLeft(ByteString.empty)(_ ++ _)
        (uncompress(data) must_== "foobar") and (received.last must_== HttpChunk.LastChunk(Headers()))
      }
    }

    "end a chunked response that has no last chunk" in {
      val chunks = Source(List(HttpChunk.Chunk(ByteString("foo")), HttpChunk.Chunk(ByteString("bar"))))
      withApplication(Ok.sendEntity(HttpEntity.Chunked(chunks, None))) { app =>
        implicit val mat: Materializer = app.materializer
        val received = Await.result(await(makeBrotliRequest(app)).body.asInstanceOf[HttpEntity.Chunked].chunks.runWith(Sink.seq), 5.seconds)
        val data = received.collect { case HttpChunk.Chunk(d) => d }.foldLeft(ByteString.empty)(_ ++ _)
        (uncompress(data) must_== "foobar") and (received.exists(_.isInstanceOf[HttpChunk.LastChunk]) must beFalse)
      }
    }

    "keep a configured window for a streamed body of known length" in withApplication(
      Ok.sendEntity(HttpEntity.Streamed(source, Some(contentLength), Some("text/css"))),
      chunkedThreshold = 512,
      settings = Seq("play.filters.brotli.window" -> 16)) { app =>
        windowBits(contentAsBytes(makeBrotliRequest(app))(defaultAwaitTimeout, app.materializer)) must_== 16
    }

    "compress many different responses concurrently through one filter, each to its own body" in {
      val application = new GuiceApplicationBuilder()
        .configure("play.filters.brotli.chunkedThreshold" -> 1024)
        .overrides(bind[Router].to[PathRouter], bind[HttpFilters].to[Filters])
        .build()
      running(application) {
        implicit val mat: Materializer = application.materializer
        implicit val ec: scala.concurrent.ExecutionContext = application.actorSystem.dispatcher
        val paths = concurrentTexts.keys.toVector.sorted
        def check(i: Int): Future[Boolean] = {
          val path = paths(i % paths.size)
          route(application, FakeRequest(GET, path).withHeaders(ACCEPT_ENCODING -> "br")).get.flatMap { result =>
            result.body.consumeData.map { bytes =>
              result.header.headers.get(CONTENT_ENCODING).contains("br") && uncompress(bytes) == concurrentTexts(path)
            }
          }
        }
        // 24 waves of 25 requests that run at the same time
        val all = (0 until 600).grouped(25).toList.flatMap(wave => Await.result(Future.sequence(wave.map(check)), 60.seconds))
        (all.size must_== 600) and (all.count(identity) must_== 600)
      }
    }

    "preserve original headers" in withApplication(Ok("hello").withHeaders(SERVER -> "Play")) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressed(result)
      header(SERVER, result) must beSome("Play")
    }

    "preserve original cookies" in withApplication(Ok("hello").withCookies(Cookie("foo", "bar"))) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressed(result)
      cookies(result).get("foo") must beSome(Cookie("foo", "bar"))
    }

    "preserve original session" in withApplication(Ok("hello").withSession("foo" -> "bar")) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressed(result)
      session(result).get("foo") must beSome("bar")
    }

    "preserve original Vary header values" in withApplication(Ok("hello").withHeaders(VARY -> "original")) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressed(result)
      header(VARY, result) must beSome[String].which(header => header.contains("original,"))
    }

    "preserve original Vary header values and not duplicate case-insensitive ACCEPT-ENCODING" in withApplication(Ok("hello").withHeaders(VARY -> "original,ACCEPT-encoding")) { implicit app =>
      val result = makeBrotliRequest(app)
      checkCompressed(result)
      header(VARY, result) must beSome[String].which(header => header.split(",").filter(_.toLowerCase(java.util.Locale.ENGLISH) == ACCEPT_ENCODING.toLowerCase(java.util.Locale.ENGLISH)).size == 1)
    }
  }

  "The BrotliFilter settings" should {

    val prefix = "play.filters.brotli"

    // Compressible text from a small vocabulary: the quality changes its compressed size a lot
    val sample: String = {
      val random = new Random(42)
      val vocabulary = Vector("alpha", "beta", "gamma", "delta", "play", "brotli", "filter", "stream")
      Iterator.continually(vocabulary(random.nextInt(vocabulary.size))).take(8000).mkString(" ")
    }

    // High entropy text: it can only be compressed by repeating it, which needs a large enough window
    val block: String = {
      val random = new Random(7)
      val alphabet = (('a' to 'z') ++ ('0' to '9')).toVector
      Iterator.continually(alphabet(random.nextInt(alphabet.size))).take(3000).mkString
    }

    def streamed(text: String): Result =
      Ok.sendEntity(HttpEntity.Streamed(Source.single(ByteString(text)), None, None))

    def compressedBytes(result: Result, settings: (String, Any)*): ByteString =
      withApplication(result, settings = settings) { app =>
        contentAsBytes(makeBrotliRequest(app))(defaultAwaitTimeout, app.materializer)
      }

    def notCompressed(result: Result, expectedBody: String, settings: (String, Any)*) =
      withApplication(result, settings = settings) { implicit app =>
        checkNotCompressed(makeBrotliRequest(app), expectedBody)(app.materializer)
      }

    def compressed(result: Result, expectedBody: String, settings: (String, Any)*) =
      withApplication(result, settings = settings) { implicit app =>
        checkCompressedBody(makeBrotliRequest(app), expectedBody)(app.materializer)
      }

    "produce a smaller strict body with a higher quality" in {
      val fast = compressedBytes(Ok(sample), s"$prefix.quality" -> 0)
      val best = compressedBytes(Ok(sample), s"$prefix.quality" -> 11)
      (best.length must be_<(fast.length)) and (uncompress(fast) must_== sample) and (uncompress(best) must_== sample)
    }

    "produce a smaller streamed body with a higher quality" in {
      val fast = compressedBytes(streamed(sample), s"$prefix.quality" -> 0)
      val best = compressedBytes(streamed(sample), s"$prefix.quality" -> 11)
      (best.length must be_<(fast.length)) and (uncompress(fast) must_== sample) and (uncompress(best) must_== sample)
    }

    "use the encoder default for a quality of -1, for strict and streamed bodies" in {
      (compressedBytes(Ok(sample), s"$prefix.quality" -> -1) must_== compressedBytes(Ok(sample), s"$prefix.quality" -> 11)) and
        (compressedBytes(streamed(sample), s"$prefix.quality" -> -1) must_== compressedBytes(streamed(sample), s"$prefix.quality" -> 11))
    }

    "use the window for a strict body" in {
      val small = compressedBytes(Ok(block + block), s"$prefix.window" -> 10)
      val large = compressedBytes(Ok(block + block), s"$prefix.window" -> 16)
      (small.length must be_>(large.length * 3 / 2)) and (uncompress(small) must_== block + block) and
        (uncompress(large) must_== block + block)
    }

    "use the window for a streamed body" in {
      val small = compressedBytes(streamed(block + block), s"$prefix.window" -> 10)
      val large = compressedBytes(streamed(block + block), s"$prefix.window" -> 16)
      (small.length must be_>(large.length * 3 / 2)) and (uncompress(small) must_== block + block) and
        (uncompress(large) must_== block + block)
    }

    "compress strict and streamed bodies with every mode" in {
      Seq("generic", "text", "font").map { mode =>
        (uncompress(compressedBytes(Ok(sample), s"$prefix.mode" -> mode)) must_== sample) and
          (uncompress(compressedBytes(streamed(sample), s"$prefix.mode" -> mode)) must_== sample)
      }.reduce(_ and _)
    }

    "compress a streamed body with each buffer size" in {
      val chunks = Source(List("foo ", "bar ", "baz").map(ByteString(_)))
      Seq("0", "1", "16k", Int.MaxValue.toString).map { bufferSize =>
        val result = Ok.sendEntity(HttpEntity.Streamed(chunks, None, None))
        uncompress(compressedBytes(result, s"$prefix.bufferSize" -> bufferSize)) must_== "foo bar baz"
      }.reduce(_ and _)
    }

    "not compress a body that is not above the threshold" in {
      notCompressed(Ok("x" * 100), "x" * 100, s"$prefix.threshold" -> "1k") and
        notCompressed(Ok("x" * 1024), "x" * 1024, s"$prefix.threshold" -> "1k")
    }

    "compress a body that is above the threshold" in {
      compressed(Ok("x" * 1025), "x" * 1025, s"$prefix.threshold" -> "1k")
    }

    "compress a body of unknown length, which is assumed to be above the threshold" in {
      compressed(streamed("tiny"), "tiny", s"$prefix.threshold" -> "1k")
    }

    "compress bodies of any size by default" in {
      compressed(Ok("x"), "x")
    }

    "fail to start the application with an invalid setting, naming it" in {
      def startFailure(settings: (String, Any)*): String =
        try {
          withApplication(Ok("x"), settings = settings)(_ => ())
          "the application started"
        } catch {
          case e: Throwable => Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).map(_.getMessage).mkString(" | ")
        }
      (startFailure(s"$prefix.quality" -> 12) must contain("Invalid brotli quality 12")) and
        (startFailure(s"$prefix.mode" -> "bogus") must contain("Invalid brotli mode 'bogus'")) and
        (startFailure(s"$prefix.contentType.whiteList" -> Seq("bogus")) must contain("play.filters.brotli.contentType.whiteList"))
    }

    "only compress the white listed content types" in {
      val settings = Seq(s"$prefix.contentType.whiteList" -> Seq("text/html"))
      compressed(Ok("<p>hi</p>").as("text/html"), "<p>hi</p>", settings: _*) and
        notCompressed(Ok("{}").as("application/json"), "{}", settings: _*) and
        notCompressed(Ok("plain"), "plain", settings: _*)
    }

    "not compress the black listed content types" in {
      val settings = Seq(s"$prefix.contentType.blackList" -> Seq("application/json"))
      notCompressed(Ok("{}").as("application/json"), "{}", settings: _*) and
        compressed(Ok("<p>hi</p>").as("text/html"), "<p>hi</p>", settings: _*) and
        compressed(Ok("plain"), "plain", settings: _*)
    }
  }

  "The BrotliFlow" should {

    "compress a stream when the buffer is disabled with a buffer size of 0 or Int.MaxValue" in withApplication(Ok("unused")) { implicit app =>
      implicit val mat: Materializer = app.materializer
      val input = List("hello ", "brotli ", "world").map(ByteString(_))
      val results = Seq(0, Int.MaxValue).map { bufferSize =>
        val compressed = Await.result(Source(input).via(BrotliFlow.brotli(bufferSize)).runFold(ByteString.empty)(_ ++ _), 5.seconds)
        uncompress(compressed) must_== "hello brotli world"
      }
      results.reduce(_ and _)
    }
  }

  // The window is the first thing in a brotli stream (RFC 7932, section 9.1)
  def windowBits(compressed: ByteString): Int = {
    val first = compressed(0) & 0xff
    if ((first & 1) == 0) 16
    else if (((first >> 1) & 7) != 0) 17 + ((first >> 1) & 7)
    else {
      val m = (first >> 4) & 7
      if (m == 0) 17 else 8 + m
    }
  }

  // The responses of the concurrency test: different sizes and shapes, each with a text of its own
  private def text(seed: Int, length: Int): String = {
    val random = new Random(seed)
    val vocabulary = Vector("alpha", "beta", "gamma", "delta", "play", "brotli", "filter", "stream", "window", "flush")
    // every word is 4 characters or more, with the space: length / 4 + 1 words are enough
    Iterator.continually(vocabulary(random.nextInt(vocabulary.size))).take(length / 4 + 1).mkString(" ").take(length)
  }

  private val concurrentTexts: Map[String, String] = Map(
    "/small" -> text(1, 2000),
    "/large" -> text(2, 150000),
    "/streamed" -> text(3, 120000),
    "/unknown" -> text(4, 80000),
    "/chunked" -> text(5, 100000),
    "/tiny" -> text(6, 30000)
  )

  private def pieces(text: String, size: Int): List[ByteString] = text.getBytes("UTF-8").grouped(size).map(ByteString(_)).toList

  private val concurrentResults: Map[String, Result] = Map(
    "/small" -> Ok(concurrentTexts("/small")),
    "/large" -> Ok(concurrentTexts("/large")),
    "/streamed" -> Ok.sendEntity(HttpEntity.Streamed(Source(pieces(concurrentTexts("/streamed"), 8192)), Some(concurrentTexts("/streamed").length.toLong), None)),
    "/unknown" -> Ok.sendEntity(HttpEntity.Streamed(Source(pieces(concurrentTexts("/unknown"), 8192)), None, None)),
    "/chunked" -> Ok.chunked(Source(pieces(concurrentTexts("/chunked"), 8192))),
    "/tiny" -> Ok.chunked(Source(pieces(concurrentTexts("/tiny"), 256)))
  )

  class PathRouter @Inject() (action: DefaultActionBuilder) extends SimpleRouterImpl({ case request => action(concurrentResults(request.path)) })

  class Filters @Inject() (brotliFilter: BrotliFilter) extends HttpFilters {
    def filters = Seq(brotliFilter)
  }

  class ResultRouter @Inject() (action: DefaultActionBuilder, result: Result) extends SimpleRouterImpl({ case _ => action(result) })

  def withApplication[T](result: Result, quality: Int = 5, chunkedThreshold: Int = 1024, settings: Seq[(String, Any)] = Nil)(block: Application => T): T = {
    val application = new GuiceApplicationBuilder()
      .configure(
        "akka.loglevel" -> "OFF",
        "play.filters.brotli.quality" -> quality,
        "play.filters.brotli.chunkedThreshold" -> chunkedThreshold
      ).configure(settings: _*).overrides(
          bind[Result].to(result),
          bind[Router].to[ResultRouter],
          bind[HttpFilters].to[Filters]
        ).build()
    running(application)(block(application))
  }

  def brotliRequest: FakeRequest[AnyContentAsEmpty.type] = FakeRequest().withHeaders(ACCEPT_ENCODING -> "br")

  def makeBrotliRequest(app: Application): Future[Result] = route(app, brotliRequest).get

  def requestAccepting(app: Application, codings: String): Future[Result] = route(app, FakeRequest().withHeaders(ACCEPT_ENCODING -> codings)).get

  def uncompress(bytes: ByteString): String = {
    val is = new BrotliInputStream(new ByteArrayInputStream(bytes.toArray))
    val result = IOUtils.toString(is, "UTF-8")
    is.close()
    result
  }

  def uncompressBytes(bytes: ByteString): Array[Byte] = {
    val is = new BrotliInputStream(new ByteArrayInputStream(bytes.toArray))
    val result = IOUtils.toByteArray(is)
    is.close()
    result
  }
  def checkCompressed(result: Future[Result]): MatchResult[Option[String]] = {
    header(CONTENT_ENCODING, result) aka "Content encoding header" must beSome("br")
  }

  def checkCompressedBodyLength(result: Future[Result], contentLength: Long)(implicit mat: Materializer): MatchResult[Any] = {
    checkCompressed(result)
    val resultBody = contentAsBytes(result)
    await(result).body.contentLength.foreach { cl =>
      resultBody.length must_== cl
    }
    uncompressBytes(resultBody).length must_== contentLength
  }
  def checkCompressedBody(result: Future[Result], body: String)(implicit mat: Materializer): MatchResult[Any] = {
    checkCompressed(result)
    val resultBody = contentAsBytes(result)
    await(result).body.contentLength.foreach { cl =>
      resultBody.length must_== cl
    }
    uncompress(resultBody) must_== body
  }

  def checkNotCompressed(result: Future[Result], body: String)(implicit mat: Materializer): MatchResult[Any] = {
    header(CONTENT_ENCODING, result) must beNone
    contentAsString(result) must_== body
  }
}
