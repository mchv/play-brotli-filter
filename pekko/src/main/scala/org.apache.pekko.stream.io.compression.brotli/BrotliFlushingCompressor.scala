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

package org.apache.pekko.stream.io.compression.brotli

import scala.concurrent.duration.FiniteDuration

import com.aayushatharva.brotli4j.encoder.Encoder

import org.apache.pekko.stream.{ Attributes, FlowShape, Inlet, Outlet }
import org.apache.pekko.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler, TimerGraphStageLogic }
import org.apache.pekko.util.ByteString

/**
 * Compresses a stream with brotli and flushes the encoder only when it has to: at the end of the stream, and when the
 * oldest data that the encoder holds has been there for `maxFlushDelay`, so that a stream that trickles is not delayed
 * by more than that. The logic is [[BrotliFlushingLogic]], which a stage over other elements than [[ByteString]]s can
 * reuse.
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
    new BrotliFlushingLogic[ByteString, ByteString](shape, parameters, maxFlushDelay) {
      override protected def element(compressed: ByteString): ByteString = compressed
      override def onPush(): Unit = compress(grab(in))
    }
}

/**
 * The logic of a stage that compresses with brotli and flushes the encoder only when it has to: at the end of the
 * stream, and when the oldest data that the encoder holds has waited for `maxFlushDelay` (see
 * [[BrotliFlushingCompressor]]). The element types are left to the subclass, so that a stage over elements that carry
 * more than data, such as the chunks of an HTTP response, compresses them in one stage instead of taking them apart,
 * compressing, and putting them together in a stage each.
 *
 * The subclass handles `onPush`: it gives the data of the element to `compress`, or ends the stream with `finish`.
 * `element` wraps compressed data into an element for the downstream.
 *
 * The delay is scheduled with the scheduler of the materializer, which rounds it up to its tick
 * (`pekko.scheduler.tick-duration`, 10 ms by default): a delay shorter than the tick is as long as the tick. A delay of
 * zero or less flushes after every element, as the gzip flow does.
 */
abstract class BrotliFlushingLogic[In, Out](
    shape: FlowShape[In, Out],
    parameters: Encoder.Parameters,
    maxFlushDelay: FiniteDuration
) extends TimerGraphStageLogic(shape) with InHandler with OutHandler {
  private val in: Inlet[In] = shape.in
  private val out: Outlet[Out] = shape.out
  private val FlushTimer = "flush"
  private val flushEveryElement = maxFlushDelay.length <= 0

  private val compressor = new BrotliCompressor(parameters)

  /** Whether the encoder holds data that has not been flushed, and the timer for it is running. */
  private var held = false

  /** Whether `finish` has run: the encoder is released, and only the data it produced last may still be sent. */
  private var finished = false

  /** Compressed data that nobody has asked for yet. */
  private var ready = ByteString.empty

  setHandlers(in, out, this)

  /** Wraps compressed data into an element for the downstream. */
  protected def element(compressed: ByteString): Out

  /** Gives data to the encoder and sends what it produces, or asks for more. The subclass calls it from `onPush`. */
  protected final def compress(data: ByteString): Unit =
    if (flushEveryElement) deliver(compressor.compressAndFlush(data))
    else {
      val compressed = compressor.compress(data)
      if (data.nonEmpty && !held) {
        held = true
        scheduleOnce(FlushTimer, maxFlushDelay)
      }
      deliver(compressed)
    }

  /**
   * Ends the stream: the encoder is finished and released, its last output is sent, then `trailer` if there is one, and
   * the stage completes. The subclass calls it when an element ends the stream; the end of the upstream calls it too.
   */
  protected final def finish(trailer: Option[Out]): Unit = {
    // finish() releases the encoder, and the last output may wait for demand: a timer must not flush it meanwhile
    cancelTimer(FlushTimer)
    held = false
    finished = true
    val last = ready ++ compressor.finish()
    ready = ByteString.empty
    val elements = (if (last.nonEmpty) element(last) :: Nil else Nil) ++ trailer.toList
    if (elements.isEmpty) completeStage() else emitMultiple(out, elements.iterator, () => completeStage())
  }

  override def onPull(): Unit =
    if (ready.nonEmpty) {
      val data = ready
      ready = ByteString.empty
      push(out, element(data))
    } else if (!hasBeenPulled(in)) pull(in)

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
      push(out, element(data))
    } else ready = data
  }

  override def onUpstreamFinish(): Unit = if (!finished) finish(None)

  override def postStop(): Unit = compressor.close()
}
