/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com>
 *   - Mariot Chauvin
 */

package akka.stream.io.compression.brotli

import java.io.IOException
import java.util.zip.ZipException

import scala.annotation.tailrec

import com.aayushatharva.brotli4j.decoder.DecoderJNI

import akka.stream.{ Attributes, FlowShape, Inlet, Outlet }
import akka.stream.stage.{ GraphStage, GraphStageLogic, InHandler, OutHandler }
import akka.util.ByteString

object BrotliDecompressor {

  /** The size of the input buffer of the native decoder, which is also the size of the pieces that it is fed with. */
  private val InputBufferSize = 16 * 1024

  /** The size above which the native decoder does not return decoded data in one piece. */
  private val MaxOutputChunkSize = 64 * 1024
}

/**
 * Decompresses a brotli stream as it arrives: the decoded data is emitted as soon as the decoder has produced it, in
 * pieces of at most 64 KiB, and the memory used is bounded by the size of those pieces, whatever the size of the
 * stream and the size of the pieces it arrives in.
 *
 * The stream fails when the input is truncated, corrupt, or followed by other data, and when the decoded data would
 * exceed `maxOutputSize` bytes. Use the limit for untrusted input: brotli can expand a few kilobytes into
 * gigabytes. No data beyond the limit is emitted.
 *
 * @param maxOutputSize the maximum number of decoded bytes, or 0 for no limit
 */
class BrotliDecompressor(maxOutputSize: Long = 0L) extends GraphStage[FlowShape[ByteString, ByteString]] {
  import BrotliDecompressor._

  private val in = Inlet[ByteString]("BrotliDecompressor.in")
  private val out = Outlet[ByteString]("BrotliDecompressor.out")

  override val shape: FlowShape[ByteString, ByteString] = FlowShape(in, out)

  override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
    new GraphStageLogic(shape) with InHandler with OutHandler {

      private var decoder: DecoderJNI.Wrapper = _

      /** Input that has not been given to the decoder yet. */
      private var pending = ByteString.empty

      /** Whether some input has arrived: a stream without any input is an empty stream, not a truncated one. */
      private var started = false

      /** Whether the decoder has reached the end of the brotli stream. */
      private var done = false

      /** Whether the decoder has been given a chance to finish since the last input it was given. */
      private var nudged = false

      private var produced = 0L

      setHandlers(in, out, this)

      override def preStart(): Unit =
        decoder = new DecoderJNI.Wrapper(InputBufferSize, MaxOutputChunkSize)

      override def postStop(): Unit =
        if (decoder != null) {
          decoder.destroy()
          decoder = null
        }

      override def onPull(): Unit = advance()

      override def onPush(): Unit = {
        val bytes = grab(in)
        if (done) {
          if (bytes.nonEmpty) failStage(unexpectedData())
          else pull(in)
        } else if (bytes.isEmpty) pull(in)
        else {
          started = true
          pending = bytes
          advance()
        }
      }

      override def onUpstreamFinish(): Unit =
        // Some input may not have been decoded yet: the decoder is run when the downstream asks for data
        if (isAvailable(out)) advance()

      /** Emits decoded data, or asks for more input, or completes. Only called when the downstream is waiting. */
      private def advance(): Unit = {
        val chunk = decode()
        if (chunk.nonEmpty) push(out, chunk)
        else if (done) {
          if (pending.nonEmpty) failStage(unexpectedData())
          else if (isClosed(in)) completeStage()
          else if (!hasBeenPulled(in)) pull(in) // there should be nothing else: fail if there is
        } else if (isClosed(in)) {
          if (started) failStage(new ZipException("Truncated Brotli stream"))
          else completeStage()
        } else if (!hasBeenPulled(in)) pull(in)
      }

      /** Runs the decoder until it has data for the downstream, or needs input that is not there, or is done. */
      @tailrec private def decode(): ByteString =
        decoder.getStatus match {
          case DecoderJNI.Status.DONE =>
            done = true
            ByteString.empty
          case DecoderJNI.Status.OK =>
            decoder.push(0)
            decode()
          case DecoderJNI.Status.NEEDS_MORE_OUTPUT =>
            val chunk = pullOutput()
            if (chunk.nonEmpty) chunk else decode()
          case DecoderJNI.Status.NEEDS_MORE_INPUT =>
            if (decoder.hasOutput) {
              // Decoded data is ready before the decoder asks for more input: do not hold it back
              val chunk = pullOutput()
              if (chunk.nonEmpty) chunk else decode()
            } else if (pending.nonEmpty) {
              pushInput()
              decode()
            } else if (!nudged) {
              // All the input is given, but the decoder may still have to process what it has buffered: in particular
              // it only reports the end of a stream whose size is a multiple of its window after this.
              nudged = true
              decoder.push(0)
              decode()
            } else ByteString.empty
          case status =>
            throw new ZipException("Brotli decompression failed - status: " + status)
        }

      private def pushInput(): Unit = {
        val buffer = decoder.getInputBuffer
        buffer.clear()
        val copied = pending.copyToBuffer(buffer)
        pending = pending.drop(copied)
        nudged = false
        decoder.push(copied)
      }

      private def pullOutput(): ByteString = {
        val buffer = decoder.pull()
        val size = buffer.remaining()
        produced += size
        if (maxOutputSize > 0 && produced > maxOutputSize)
          throw new IOException(s"Decompressed data exceeds the limit of $maxOutputSize bytes")
        val bytes = new Array[Byte](size)
        buffer.get(bytes)
        ByteString.fromArrayUnsafe(bytes)
      }

      private def unexpectedData() = new ZipException("Unexpected data after the end of the Brotli stream")
    }
}
