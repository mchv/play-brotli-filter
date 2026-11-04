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

package org.apache.pekko.stream.io.compression.brotli

import java.io.IOException

import com.aayushatharva.brotli4j.encoder.{ Encoder, EncoderJNI }

import org.apache.pekko.stream.impl.io.compression.Compressor
import org.apache.pekko.util.ByteString

object BrotliCompressor {
  val MinQuality: Int = 0
  val MaxQuality: Int = 11
  val DefaultQuality: Int = MaxQuality

  /** The window, as the log2 of its size, goes from 10 (1 KiB) to 24 (16 MiB). */
  val MinWindow: Int = 10
  val MaxWindow: Int = 24

  /** The size of the input buffer of the native encoder, which is also the size of the pieces that it is fed with. */
  private val InputBufferSize = 16 * 1024

  /**
   * The smallest window, as the log2 of its size, that lets the encoder match anything in a body of the given length: the
   * largest distance of a match is 2^window - 16, and the window goes from 10 to 24.
   */
  def windowFor(length: Int): Int =
    math.max(MinWindow, math.min(MaxWindow, 32 - Integer.numberOfLeadingZeros(math.max(length, 0) + 15)))

  /**
   * Compresses a whole body in one call, which is faster than a [[BrotliCompressor]] for a body that is already in
   * memory: no stream is created, and the body is not copied if it is a compact [[ByteString]].
   *
   * When the parameters do not ask for a window, the window is the smallest one that fits the body (see `windowFor`).
   * The default window of 4 MiB makes the encoder set up tables that a small body does not use, which costs more than
   * compressing the body: in the benchmarks, fitting the window makes a body of 1 to 16 KB two to three times faster
   * at quality 5, for the same compressed size.
   *
   * @param parameters the quality, the window and the mode of the encoder. They are only read.
   */
  def compress(data: ByteString, parameters: Encoder.Parameters): ByteString = {
    val fitted =
      if (parameters.lgwin() == -1) new Encoder.Parameters().setQuality(parameters.quality()).setMode(parameters.mode()).setWindow(windowFor(data.length))
      else parameters
    ByteString.fromArrayUnsafe(Encoder.compress(data.toArrayUnsafe(), fitted))
  }
}

/**
 * Compresses with brotli using the given encoder parameters (quality, window and mode).
 *
 * The compression is incremental: the input is given to the native encoder as it arrives and the compressed data is
 * returned as soon as the encoder produces it, so the memory used does not depend on the size of the stream.
 * `compress` leaves the encoder to decide when to emit, while `flush` and `compressAndFlush` end the current block so
 * that everything given so far can be decoded, at a small cost in compression ratio when the pieces are small.
 *
 * The native encoder is released by `finish`, or by `close` when the stream is aborted. A compressor must not be
 * used from several threads.
 *
 * The parameters are only read, never modified, so they can be shared between compressors.
 */
class BrotliCompressor(val params: Encoder.Parameters) extends Compressor {
  import BrotliCompressor.InputBufferSize

  /**
   * Compresses with the given quality and the default window and mode.
   *
   * @param level The quality, from 0 (fastest) to 11 (smallest output), or -1 for the encoder default (11)
   */
  def this(level: Int = BrotliCompressor.DefaultQuality) = this(new Encoder.Parameters().setQuality(level))

  private val encoder = new EncoderJNI.Wrapper(InputBufferSize, params.quality(), params.lgwin(), params.mode())
  private var closed = false

  /** Compresses the input, and returns the compressed data that the encoder has produced so far, if any. */
  override final def compress(input: ByteString): ByteString = feed(input, EncoderJNI.Operation.PROCESS)

  /** Ends the current block and returns all the compressed data that is pending. */
  override final def flush(): ByteString = run(EncoderJNI.Operation.FLUSH, 0)

  /** Ends the stream and returns the compressed data that is pending. Releases the native encoder. */
  override final def finish(): ByteString =
    try run(EncoderJNI.Operation.FINISH, 0)
    finally close()

  /** Combines `compress` + `flush` */
  override final def compressAndFlush(input: ByteString): ByteString = feed(input, EncoderJNI.Operation.FLUSH)

  /** Combines `compress` + `finish` */
  override final def compressAndFinish(input: ByteString): ByteString = compress(input) ++ finish()

  /** Releases the native encoder, if `finish` has not done it. Can be called more than once. */
  override final def close(): Unit =
    if (!closed) {
      closed = true
      encoder.destroy()
    }

  /** Gives the input to the encoder in pieces of the size of its input buffer; `last` applies to the last piece. */
  private def feed(input: ByteString, last: EncoderJNI.Operation): ByteString =
    if (closed) {
      // The input buffer is native memory that is freed with the encoder: never write to it after that
      throw new IllegalStateException("The Brotli compressor is closed")
    } else if (input.isEmpty) {
      if (last == EncoderJNI.Operation.PROCESS) ByteString.empty else run(last, 0)
    } else {
      val buffer = encoder.getInputBuffer
      var output = ByteString.empty
      var rest = input
      while (rest.nonEmpty) {
        buffer.clear()
        val size = rest.copyToBuffer(buffer)
        rest = rest.drop(size)
        output ++= run(if (rest.isEmpty) last else EncoderJNI.Operation.PROCESS, size)
      }
      output
    }

  /** Runs the operation on the `length` bytes of the input buffer and returns the compressed data it produces. */
  private def run(operation: EncoderJNI.Operation, length: Int): ByteString = {
    var output = ByteString.empty
    encoder.push(operation, length)
    var running = true
    while (running) {
      if (!encoder.isSuccess) throw new IOException("Brotli encoding failed")
      else if (encoder.hasMoreOutput) {
        val buffer = encoder.pull()
        val bytes = new Array[Byte](buffer.remaining())
        buffer.get(bytes)
        output ++= ByteString.fromArrayUnsafe(bytes)
      } else if (encoder.hasRemainingInput) encoder.push(operation, 0)
      else if (operation == EncoderJNI.Operation.FINISH && !encoder.isFinished) encoder.push(operation, 0)
      else running = false
    }
    output
  }
}
