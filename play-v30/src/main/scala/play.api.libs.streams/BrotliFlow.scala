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

package play.api.libs.streams

import scala.concurrent.duration.{ Duration, FiniteDuration }

import com.aayushatharva.brotli4j.encoder.Encoder

import org.apache.pekko.NotUsed
import org.apache.pekko.stream.io.compression.brotli.BrotliFlushingLogic
import org.apache.pekko.stream.scaladsl.BrotliCompression
import org.apache.pekko.stream.scaladsl.Flow
import org.apache.pekko.stream.stage._
import org.apache.pekko.stream._
import org.apache.pekko.util.ByteString
import play.api.http.HttpChunk

/**
 * Brotli flows for the Play filter.
 *
 * `brotli` compresses a stream of data. `brotliChunks` compresses a chunked response and `brotliToChunks` turns a
 * stream of data into a compressed chunked response, each in a single stage: the chunks are not taken apart,
 * compressed and put together in a stage each, which is what a response in many small chunks pays for the most.
 */
object BrotliFlow {

  /** The compression level used when none is given. */
  val DefaultCompressionLevel: Int = 9

  /**
   * Create a Brotli Flow with the given buffer size. The bufferSize controls how much data is sent to the Brotli compressor in
   * one go. You can use `0` or `Int.MaxValue` to disable the buffer completely.
   *
   * The compressor gives the encoder its input in pieces of 16 KB whatever the buffer size, so a smaller buffer only splits
   * the input further, at the cost of a slice and a native call per piece, and in this flow a stage that every element
   * goes through.
   */
  def brotli(
      bufferSize: Int = 512,
      compressionLevel: Int = DefaultCompressionLevel
  ): Flow[ByteString, ByteString, _] =
    brotli(bufferSize, new Encoder.Parameters().setQuality(compressionLevel))

  /**
   * Create a Brotli Flow with the given buffer size and encoder parameters, which set the quality, the window and
   * the mode. See the other `brotli` method for the buffer size.
   */
  def brotli(bufferSize: Int, parameters: Encoder.Parameters): Flow[ByteString, ByteString, _] =
    brotli(bufferSize, parameters, Duration.Zero)

  /**
   * Create a Brotli Flow like the one above, that flushes the encoder only at the end of the stream and when the oldest
   * data it holds has waited for `flushInterval`, instead of after every element. A flush ends a block: it costs
   * compressed size and CPU when the elements are small, and elements that arrive within the interval share a block. A
   * `flushInterval` of zero flushes after every element.
   */
  def brotli(bufferSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration): Flow[ByteString, ByteString, _] = {
    Flow[ByteString]
      .via(chunkerIfNeeded(bufferSize))
      .via(BrotliCompression.brotli(parameters, flushInterval))
  }

  /**
   * A flow that compresses the data of the chunks of a chunked response, and passes the last chunk, with its trailers,
   * after the last of the compressed data. The encoder is flushed as `brotli(bufferSize, parameters, flushInterval)`
   * does, and `bufferSize` is the most data it is given in one call (`0` or `Int.MaxValue` for no limit).
   */
  def brotliChunks(bufferSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration): Flow[HttpChunk, HttpChunk, NotUsed] =
    Flow.fromGraph(new ChunkCompressor(pieceSize(bufferSize), parameters, flushInterval)).named("brotliChunks")

  /**
   * A flow that compresses a stream of data into the chunks of a chunked response, without a last chunk. The encoder is
   * flushed as `brotli(bufferSize, parameters, flushInterval)` does, and `bufferSize` is the most data it is given in
   * one call (`0` or `Int.MaxValue` for no limit).
   */
  def brotliToChunks(bufferSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration): Flow[ByteString, HttpChunk, NotUsed] =
    Flow.fromGraph(new ToChunksCompressor(pieceSize(bufferSize), parameters, flushInterval)).named("brotliToChunks")

  private def pieceSize(bufferSize: Int): Int = if (bufferSize > 0 && bufferSize < Int.MaxValue) bufferSize else 0

  private def chunkerIfNeeded(bufferSize: Int): Flow[ByteString, ByteString, Any] =
    if (pieceSize(bufferSize) > 0) Flow.fromGraph(new Chunker(bufferSize))
    else Flow[ByteString]

  /** The logic of the stages below: the data of an element goes to the encoder in pieces of at most `pieceSize` (0 for no limit). */
  private abstract class ChunkLogic[In](shape: FlowShape[In, HttpChunk], pieceSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration)
      extends BrotliFlushingLogic[In, HttpChunk](shape, parameters, flushInterval) {
    override protected def element(compressed: ByteString): HttpChunk = HttpChunk.Chunk(compressed)

    protected final def compressPieces(data: ByteString): Unit =
      if (pieceSize == 0 || data.length <= pieceSize) compress(data)
      else data.grouped(pieceSize).foreach(compress)
  }

  private final class ChunkCompressor(pieceSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration)
      extends GraphStage[FlowShape[HttpChunk, HttpChunk]] {
    private val in = Inlet[HttpChunk]("BrotliChunks.in")
    private val out = Outlet[HttpChunk]("BrotliChunks.out")
    override val shape: FlowShape[HttpChunk, HttpChunk] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new ChunkLogic[HttpChunk](shape, pieceSize, parameters, flushInterval) {
        override def onPush(): Unit = grab(in) match {
          case HttpChunk.Chunk(data)    => compressPieces(data)
          case last: HttpChunk.LastChunk => finish(Some(last))
        }
      }
  }

  private final class ToChunksCompressor(pieceSize: Int, parameters: Encoder.Parameters, flushInterval: FiniteDuration)
      extends GraphStage[FlowShape[ByteString, HttpChunk]] {
    private val in = Inlet[ByteString]("BrotliToChunks.in")
    private val out = Outlet[HttpChunk]("BrotliToChunks.out")
    override val shape: FlowShape[ByteString, HttpChunk] = FlowShape(in, out)

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new ChunkLogic[ByteString](shape, pieceSize, parameters, flushInterval) {
        override def onPush(): Unit = compressPieces(grab(in))
      }
  }

  // http://doc.akka.io/docs/akka/2.4.14/scala/stream/stream-cookbook.html#Chunking_up_a_stream_of_ByteStrings_into_limited_size_ByteStrings
  private class Chunker(val chunkSize: Int) extends GraphStage[FlowShape[ByteString, ByteString]] {
    private val in  = Inlet[ByteString]("Chunker.in")
    private val out = Outlet[ByteString]("Chunker.out")

    override val shape: FlowShape[ByteString, ByteString] = FlowShape.of(in, out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) {
      private var buffer = ByteString.empty

      setHandler(out, new OutHandler {
        override def onPull(): Unit = emitChunk()
      })
      setHandler(
        in,
        new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            buffer ++= elem
            emitChunk()
          }

          override def onUpstreamFinish(): Unit = {
            if (buffer.isEmpty) completeStage()
            else {
              // There are elements left in buffer, so
              // we keep accepting downstream pulls and push from buffer until emptied.
              //
              // It might be though, that the upstream finished while it was pulled, in which
              // case we will not get an onPull from the downstream, because we already had one.
              // In that case we need to emit from the buffer.
              if (isAvailable(out)) emitChunk()
            }
          }
        }
      )

      private def emitChunk(): Unit = {
        if (buffer.isEmpty) {
          if (isClosed(in)) completeStage()
          else pull(in)
        } else {
          val (chunk, nextBuffer) = buffer.splitAt(chunkSize)
          buffer = nextBuffer
          push(out, chunk)
        }
      }
    }
  }
}
