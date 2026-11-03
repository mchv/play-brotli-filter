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

package akka.stream.scaladsl

import akka.NotUsed
import akka.stream.impl.io.compression._
import akka.stream.scaladsl.Flow
import akka.util.ByteString

import scala.concurrent.duration.FiniteDuration

import com.aayushatharva.brotli4j.encoder.Encoder
import akka.stream.io.compression.brotli.{BrotliCompressor, BrotliDecompressor, BrotliFlushingCompressor}

object BrotliCompression {

  val DefaultQuality: Int = BrotliCompressor.DefaultQuality

  /**
   * Creates a flow that brotli-compresses a stream of ByteStrings with the default quality (11).
   * See `brotli(level)`.
   */
  def brotli: Flow[ByteString, ByteString, NotUsed] =
    CompressionUtils.compressorFlow(() => new BrotliCompressor())

  /**
   * Creates a flow that brotli-compresses a stream of ByteStrings. Note that the compressor
   * will flush after every [[ByteString]] so that it is guaranteed that every [[ByteString]]
   * coming out of the flow can be fully decompressed without waiting for additional data. This may
   * come at a compression performance cost for very small chunks.
   *
   * @param level Compression level (0-11), or -1 for the encoder default (11)
   */
  def brotli(level: Int = BrotliCompressor.DefaultQuality): Flow[ByteString, ByteString, NotUsed] =
    CompressionUtils.compressorFlow(() => new BrotliCompressor(level))

  /**
   * Creates a flow that brotli-compresses a stream of ByteStrings with the given encoder parameters, which set
   * the quality, the window and the mode. The parameters are only read, so they can be shared.
   */
  def brotli(parameters: Encoder.Parameters): Flow[ByteString, ByteString, NotUsed] =
    CompressionUtils.compressorFlow(() => new BrotliCompressor(parameters))

  /**
   * Creates a flow that brotli-compresses a stream of ByteStrings, and flushes the encoder only at the end of the stream
   * and when the oldest data that it holds has waited for `maxFlushDelay`. A flush ends a block, which costs compressed
   * size and CPU when the elements are small (see `brotli(parameters)`, which flushes after every element); elements
   * that arrive within the delay share a block, and the output is the same as the output of a single call.
   *
   * @param maxFlushDelay the longest time that data waits in the encoder; zero flushes after every element
   */
  def brotli(parameters: Encoder.Parameters, maxFlushDelay: FiniteDuration): Flow[ByteString, ByteString, NotUsed] =
    if (maxFlushDelay.length <= 0) brotli(parameters)
    else Flow[ByteString].via(new BrotliFlushingCompressor(parameters, maxFlushDelay)).named("brotli")

  /**
   * Creates a Flow that decompresses a brotli-compressed stream of data. The decompressed data is emitted as it is
   * produced, in pieces of at most 64 KiB, and is not limited in size: see `unbrotli(maxOutputSize)`.
   */
  def unbrotli(): Flow[ByteString, ByteString, NotUsed] = unbrotli(0L)

  /**
   * Creates a Flow that decompresses a brotli-compressed stream of data, and fails when the decompressed data would
   * exceed `maxOutputSize` bytes. Use the limit for untrusted input: brotli can expand a few kilobytes into
   * gigabytes. No data beyond the limit is emitted.
   *
   * @param maxOutputSize the maximum number of decompressed bytes, or 0 for no limit
   */
  def unbrotli(maxOutputSize: Long): Flow[ByteString, ByteString, NotUsed] =
    Flow[ByteString].via(new BrotliDecompressor(maxOutputSize)).named("unbrotli")

}