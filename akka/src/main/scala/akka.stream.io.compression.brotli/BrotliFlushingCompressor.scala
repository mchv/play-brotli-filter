/*
 * Copyright 2026 Mariot Chauvin
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

package akka.stream.io.compression.brotli

import scala.concurrent.duration.FiniteDuration

import com.aayushatharva.brotli4j.encoder.Encoder

import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic }
import akka.util.ByteString

/**
 * Compresses a stream with brotli and flushes the encoder only when it has to: at the end of the stream, and when the
 * oldest data that the encoder holds has been there for `maxFlushDelay`, so that a stream that trickles is not delayed
 * by more than that.
 *
 * Flushing after every element, like the gzip flow does, ends a block each time. Elements of 8 KB cost 2% of the
 * compressed size, elements of 1 KB 16%, elements of 256 bytes 56%, and the CPU of ending that many small blocks. Elements
 * that arrive within the delay, as the pieces of a file or of a response that is produced faster than it is sent do, end up
 * in the same block, and the output is the same as the output of a single call.
 *
 * @param maxFlushDelay the longest time that data waits in the encoder before it is flushed
 */
final class BrotliFlushingCompressor(parameters: Encoder.Parameters, maxFlushDelay: FiniteDuration)
    extends GraphStage[FlowShape[ByteString, ByteString]] {
  require(maxFlushDelay.length > 0, "The flush delay must be positive: use the flow that flushes every element for zero")

  private val in = Inlet[ByteString]("BrotliFlushingCompressor.in")
  private val out = Outlet[ByteString]("BrotliFlushingCompressor.out")

  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new TimerGraphStageLogic(shape) with InHandler with OutHandler {
      private val FlushTimer = "flush"

      private val compressor = new BrotliCompressor(parameters)

      /** Whether the encoder holds data that has not been flushed, and the timer for it is running. */
      private var held = false

      /** Compressed data that nobody has asked for yet. */
      private var ready = ByteString.empty

      setHandlers(in, out, this)

      override def onPull(): Unit =
        if (ready.nonEmpty) {
          val data = ready
          ready = ByteString.empty
          push(out, data)
        } else if (!hasBeenPulled(in)) pull(in)

      override def onPush(): Unit = {
        val data = grab(in)
        val compressed = compressor.compress(data)
        if (data.nonEmpty && !held) {
          held = true
          scheduleOnce(FlushTimer, maxFlushDelay)
        }
        deliver(compressed)
      }

      override protected def onTimer(timerKey: Any): Unit = {
        held = false
        deliver(compressor.flush())
      }

      /** Pushes what is ready if the downstream is asking, and asks the upstream for more if there is nothing to push. */
      private def deliver(compressed: ByteString): Unit = {
        val data = ready ++ compressed
        if (data.isEmpty) {
          if (isAvailable(out) && !hasBeenPulled(in)) pull(in)
        } else if (isAvailable(out)) {
          ready = ByteString.empty
          push(out, data)
        } else ready = data
      }

      override def onUpstreamFinish(): Unit = {
        // finish() releases the encoder, and the last output may wait for demand: a timer must not flush it meanwhile
        cancelTimer(FlushTimer)
        held = false
        val last = ready ++ compressor.finish()
        ready = ByteString.empty
        if (last.nonEmpty) emit(out, last, () => completeStage()) else completeStage()
      }

      override def postStop(): Unit = compressor.close()
    }
}
