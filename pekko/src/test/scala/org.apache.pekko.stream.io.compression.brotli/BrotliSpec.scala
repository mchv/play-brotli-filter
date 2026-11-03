/*
 * Copyright (C) 2009-2023 Lightbend Inc. <https://www.lightbend.com> and Mariot Chauvin <mariot.chauvin@gmail.com>
 */

package org.apache.pekko.stream.io.compression.brotli

import java.io.{ InputStream, OutputStream }

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.BrotliOutputStream;
import com.aayushatharva.brotli4j.decoder.BrotliInputStream;
import com.aayushatharva.brotli4j.encoder.Encoder
import java.util.zip.ZipException

import java.util.concurrent.{ CountDownLatch, TimeUnit }
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._

import org.apache.pekko.stream.io.compression.brotli.CoderSpec
import org.apache.pekko.stream.impl.io.compression.{ CompressionUtils, Compressor, GzipCompressor }
import org.apache.pekko.stream.scaladsl.{BrotliCompression, Flow, Keep, Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.util.ByteString

class BrotliSpec extends CoderSpec("brotli") {
  import CompressionTestingTools._


  {
    Brotli4jLoader.ensureAvailability();
  }

  /**
   * Compares the bytes of two ByteStrings. `ByteString.equals` looks every element up in the pieces of a rope, which takes
   * minutes for a few megabytes on Scala 2.12: use this for anything larger than a piece of the decoder (64 KiB).
   */
  private def sameBytes(actual: ByteString, expected: ByteString) = actual.toArray should equal(expected.toArray)

  private def wordsText: String = words(8000)

  private def words(count: Int): String = {
    val random = new scala.util.Random(42)
    val vocabulary = Vector("alpha", "beta", "gamma", "delta", "akka", "brotli", "stream", "flow")
    Iterator.continually(vocabulary(random.nextInt(vocabulary.size))).take(count).mkString(" ")
  }

  // High entropy text: it can only be compressed by repeating it, which needs a large enough window
  private def highEntropyText: String = {
    val random = new scala.util.Random(7)
    val alphabet = (('a' to 'z') ++ ('0' to '9')).toVector
    Iterator.continually(alphabet(random.nextInt(alphabet.size))).take(3000).mkString
  }

  override protected def newCompressor(): Compressor = new BrotliCompressor
  override protected val encoderFlow: Flow[ByteString, ByteString, Any] = BrotliCompression.brotli
  override protected def decoderFlow(maxBytesPerChunk: Int): Flow[ByteString, ByteString, Any] = BrotliCompression.unbrotli()

  protected def newDecodedInputStream(underlying: InputStream): InputStream =
    new BrotliInputStream(underlying)

  override protected def newEncodedOutputStream(underlying: OutputStream): OutputStream =
    new BrotliOutputStream(underlying)

  /* There is no CRC in Brotli */
  override def corruptInputCheck = false

  override def extraTests(): Unit = {
    "decode concatenated compressions" in {
      pending //TODO is that something we could support?
      ourDecode(Seq(encode("Hello, "), encode("dear "), encode("User!")).join) should readAs("Hello, dear User!")
    }
    "provide a similar compression ratio than the standard Brotli/Unbortli streams" in {
      ourEncode(largeTextBytes).length should be equals streamEncode(largeTextBytes).length
    }
    "throw an error on truncated input" in {
      val ex = the[RuntimeException] thrownBy ourDecode(streamEncode(smallTextBytes).dropRight(5))
      ex.ultimateCause.getMessage should equal("Truncated Brotli stream")
    }

    "honor the quality of the encoder parameters" in {
      val data = ByteString(wordsText, "UTF8")
      val fast = new BrotliCompressor(new Encoder.Parameters().setQuality(0)).compressAndFinish(data)
      val best = new BrotliCompressor(new Encoder.Parameters().setQuality(11)).compressAndFinish(data)
      best.length should be < fast.length
      ourDecode(fast) should equal(data)
      ourDecode(best) should equal(data)
    }

    "use the encoder default for a quality of -1" in {
      val data = ByteString(wordsText, "UTF8")
      new BrotliCompressor(-1).compressAndFinish(data) should equal(new BrotliCompressor(11).compressAndFinish(data))
    }

    "honor the window of the encoder parameters" in {
      val block = highEntropyText
      val data = ByteString(block + block, "UTF8")
      val small = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setWindow(10)).compressAndFinish(data)
      val large = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setWindow(16)).compressAndFinish(data)
      small.length should be > (large.length * 3 / 2)
      ourDecode(small) should equal(data)
      ourDecode(large) should equal(data)
    }

    "compress with every encoder mode" in {
      Encoder.Mode.values().foreach { mode =>
        val compressed = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setMode(mode)).compressAndFinish(largeTextBytes)
        ourDecode(compressed) should equal(largeTextBytes)
      }
    }

    "create the compression flow from encoder parameters" in {
      val parameters = new Encoder.Parameters().setQuality(3).setWindow(12).setMode(Encoder.Mode.TEXT)
      val compressed = Source.single(largeTextBytes).via(BrotliCompression.brotli(parameters)).join.awaitResult(3.seconds)
      ourDecode(compressed) should equal(largeTextBytes)
    }

    "decode a stream that arrives one byte at a time without redecoding what it has already decoded" in {
      val data = ByteString(words(400000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val pieces = compressed.grouped(1).toVector
      pieces.length should be > 10000
      val decoded = Source(pieces).via(BrotliCompression.unbrotli()).join.awaitResult(10.seconds)
      sameBytes(decoded, data)
    }

    "emit the decoded data before the whole stream has arrived" in {
      val data = ByteString(words(200000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(compressed.take(compressed.length / 2))
      val first = subscriber.expectNext(5.seconds)
      first should not be empty
      data.startsWith(first) should be(true)
      publisher.sendNext(compressed.drop(compressed.length / 2))
      publisher.sendComplete()
      subscriber.request(Long.MaxValue)
      val rest = subscriber.receiveWithin(5.seconds)
      sameBytes(first ++ rest.join, data)
    }

    "emit the decoded data in chunks of a bounded size" in {
      val data = ByteString(new Array[Byte](3 * 1024 * 1024))
      val compressed = new BrotliCompressor(1).compressAndFinish(data)
      val chunks = Source.single(compressed).via(BrotliCompression.unbrotli()).runWith(Sink.seq).awaitResult(10.seconds)
      chunks.map(_.length).sum should be(data.length)
      chunks.map(_.length).max should be <= 65536
    }

    "fail without emitting more than the limit when the decompressed output exceeds it" in {
      val limit = 1024 * 1024L
      val data = ByteString(new Array[Byte](20 * 1024 * 1024))
      val compressed = BrotliCompressor.compress(data, new Encoder.Parameters().setQuality(1))
      compressed.length should be < 10000
      val emitted = new AtomicLong
      val result = Source.single(compressed)
        .via(BrotliCompression.unbrotli(limit))
        .map { chunk => emitted.addAndGet(chunk.length.toLong); chunk }
        .runWith(Sink.ignore)
      val failure = the[RuntimeException] thrownBy result.awaitResult(10.seconds)
      failure.ultimateCause.getMessage should (include("exceeds") and include(limit.toString))
      emitted.get should be <= limit
    }

    "decode output of exactly the limit" in {
      val data = ByteString(new Array[Byte](1024 * 1024))
      val compressed = new BrotliCompressor(1).compressAndFinish(data)
      val decoded = Source.single(compressed).via(BrotliCompression.unbrotli(data.length.toLong)).join.awaitResult(10.seconds)
      sameBytes(decoded, data)
    }

    "not limit the output by default, and detect the end of an output that is a multiple of the window" in {
      // 8 MiB is twice the 4 MiB window of the default encoder: the decoder reports the end of the stream only after
      // it has been given a chance to finish
      val data = ByteString(new Array[Byte](8 * 1024 * 1024))
      val compressed = new BrotliCompressor(1).compressAndFinish(data)
      Source.single(compressed).via(BrotliCompression.unbrotli()).join.awaitResult(10.seconds).length should be(data.length)
    }

    "decode an empty stream and an empty input" in {
      val empty = new BrotliCompressor(5).compressAndFinish(ByteString.empty)
      ourDecode(empty) should equal(ByteString.empty)
      Source.empty[ByteString].via(BrotliCompression.unbrotli()).join.awaitResult(5.seconds) should equal(ByteString.empty)
    }

    "fail on data after the end of the stream, in the same piece or in a later one" in {
      val data = ByteString(words(1000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val trailing = ByteString("trailing bytes")
      a[RuntimeException] should be thrownBy ourDecode(compressed ++ trailing)
      a[RuntimeException] should be thrownBy
        Source(Vector(compressed, trailing)).via(BrotliCompression.unbrotli()).join.awaitResult(5.seconds)
    }

    "emit compressed data before the upstream completes" in {
      val chunk = ByteString(words(20000))
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.brotli(5)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(chunk)
      val first = subscriber.expectNext(5.seconds)
      first should not be empty
      // what was emitted for the chunk decodes to the chunk, without waiting for the end of the stream
      val (decoderIn, decoderOut) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      decoderOut.request(Long.MaxValue)
      decoderIn.sendNext(first)
      var decoded = ByteString.empty
      while (decoded.length < chunk.length) decoded ++= decoderOut.expectNext(5.seconds)
      sameBytes(decoded, chunk)
      decoderIn.sendError(new RuntimeException("done"))
      decoderOut.expectError()
      publisher.sendComplete()
      subscriber.request(Long.MaxValue)
      subscriber.receiveWithin(5.seconds)
    }

    "emit the data of every element so that the output decodes as it goes" in {
      val pieces = Vector("first piece ", "second piece ", "third piece").map(ByteString(_))
      val compressor = new BrotliCompressor(5)
      val outputs = pieces.map(compressor.compressAndFlush)
      outputs.foreach(_ should not be empty)
      val all = outputs.join ++ compressor.finish()
      ourDecode(all) should equal(pieces.join)
    }

    "compress input larger than the input buffer of the encoder" in {
      val data = ByteString(words(300000))
      data.length should be > 1024 * 1024
      val compressor = new BrotliCompressor(5)
      val compressed = compressor.compress(data) ++ compressor.finish()
      sameBytes(ourDecode(compressed), data)
    }

    "refuse input after it is closed or finished, instead of writing to the memory of a released encoder" in {
      val closed = new BrotliCompressor(5)
      closed.close()
      an[IllegalStateException] should be thrownBy closed.compress(ByteString("data"))
      an[IllegalStateException] should be thrownBy closed.compressAndFlush(ByteString("data"))
      val finished = new BrotliCompressor(5)
      finished.compressAndFinish(ByteString("data"))
      an[IllegalStateException] should be thrownBy finished.compress(ByteString("data"))
    }

    "be closable twice, and after finish" in {
      val unfinished = new BrotliCompressor(5)
      unfinished.compress(ByteString("data"))
      unfinished.close()
      unfinished.close()
      val finished = new BrotliCompressor(5)
      finished.compressAndFinish(ByteString("data"))
      finished.close()
    }

    "compress a strict body in one call" in {
      val parameters = new Encoder.Parameters().setQuality(5)
      Seq(0, 1, 100, 100000).foreach { size =>
        val data = ByteString(words(size)).take(size)
        sameBytes(ourDecode(BrotliCompressor.compress(data, parameters)), data)
      }
    }

    "compress a strict body made of several pieces like a single piece" in {
      val parameters = new Encoder.Parameters().setQuality(5)
      val pieces = Vector("one ", "two ", "three").map(ByteString(_))
      val rope = pieces.join
      BrotliCompressor.compress(rope, parameters) should equal(BrotliCompressor.compress(ByteString.fromArray(rope.toArray), parameters))
      val fitted = new Encoder.Parameters().setQuality(5).setWindow(BrotliCompressor.windowFor(8))
      BrotliCompressor.compress(rope.drop(2).take(8), parameters) should equal(
        ByteString(Encoder.compress("e two th".getBytes("UTF-8"), fitted)))
    }

    "release the native encoder when the stream completes, fails or is cancelled" in {
      // Observes close() from the thread of the stream without touching the encoder from the test thread
      class ObservedCompressor(val underlying: BrotliCompressor) extends Compressor {
        val closed = new CountDownLatch(1)
        override def compress(input: ByteString): ByteString = underlying.compress(input)
        override def flush(): ByteString = underlying.flush()
        override def finish(): ByteString = underlying.finish()
        override def compressAndFlush(input: ByteString): ByteString = underlying.compressAndFlush(input)
        override def compressAndFinish(input: ByteString): ByteString = underlying.compressAndFinish(input)
        override def close(): Unit = try underlying.close() finally closed.countDown()
      }
      def awaitReleased(compressor: ObservedCompressor): Unit = {
        compressor.closed.await(5, TimeUnit.SECONDS) should be(true)
        // close() has returned: the stream is done with the encoder
        an[IllegalStateException] should be thrownBy compressor.underlying.flush()
      }
      def observed() = new ObservedCompressor(new BrotliCompressor(5))

      val completed = observed()
      Source.single(ByteString("data")).via(CompressionUtils.compressorFlow(() => completed)).join.awaitResult(5.seconds)
      awaitReleased(completed)

      val failed = observed()
      Source(Vector(ByteString("data"))).concat(Source.failed(new RuntimeException("upstream failed")))
        .via(CompressionUtils.compressorFlow(() => failed)).runWith(Sink.ignore).failed.awaitResult(5.seconds)
      awaitReleased(failed)

      val cancelled = observed()
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(CompressionUtils.compressorFlow(() => cancelled)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(ByteString("data"))
      subscriber.expectNext(5.seconds)
      subscriber.cancel()
      awaitReleased(cancelled)
    }

    "reject a negative output limit instead of silently disabling it" in {
      an[IllegalArgumentException] should be thrownBy new BrotliDecompressor(-1L)
      an[IllegalArgumentException] should be thrownBy BrotliCompression.unbrotli(-1L)
    }

    "complete an empty stream without waiting for demand" in {
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      publisher.sendComplete()
      subscriber.expectSubscriptionAndComplete()
    }

    "complete right after the last output, without waiting for more demand" in {
      val data = ByteString(words(2000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(compressed)
      publisher.sendComplete()
      subscriber.expectNext(5.seconds) should equal(data)
      subscriber.expectComplete()
    }

    "complete right after the last output when the upstream completes later" in {
      val data = ByteString(words(2000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(compressed)
      subscriber.expectNext(5.seconds) should equal(data)
      publisher.sendComplete()
      subscriber.expectComplete()
    }

    "not flush between elements that arrive within the flush delay" in {
      val pieces = Vector.tabulate(40)(i => ByteString(words(500)).drop(i * 7).take(2048))
      val parameters = new Encoder.Parameters().setQuality(5)
      val oneCall = BrotliCompressor.compress(pieces.join, parameters).length
      val unflushed = Source(pieces).via(BrotliCompression.brotli(parameters, 10.seconds)).join.awaitResult(5.seconds)
      // Without a flush the output is the same as the output of a single call
      unflushed.length should be(oneCall)
      ourDecode(unflushed).toArray should equal(pieces.join.toArray)
    }

    "flush after every element when the flush delay is zero" in {
      val pieces = Vector.tabulate(40)(i => ByteString(words(500)).drop(i * 7).take(2048))
      val parameters = new Encoder.Parameters().setQuality(5)
      val flushed = Source(pieces).via(BrotliCompression.brotli(parameters, Duration.Zero)).join.awaitResult(5.seconds)
      flushed.length should be > BrotliCompressor.compress(pieces.join, parameters).length
      ourDecode(flushed).toArray should equal(pieces.join.toArray)
      // every element can be decoded as it arrives
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.brotli(parameters, Duration.Zero)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(pieces.head)
      subscriber.expectNext(2.seconds) should not be empty
    }

    "emit what it holds once the flush delay has passed, while the upstream is idle" in {
      val piece = ByteString(words(2000))
      val parameters = new Encoder.Parameters().setQuality(5)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.brotli(parameters, 100.millis)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      val sent = System.nanoTime()
      publisher.sendNext(piece)
      val first = subscriber.expectNext(2.seconds)
      val waited = (System.nanoTime() - sent) / 1000000
      // after the delay, and not much later
      waited should be >= 90L
      waited should be < 1000L
      // what was emitted decodes to the piece
      val (decoderIn, decoderOut) =
        TestSource.probe[ByteString].via(BrotliCompression.unbrotli()).toMat(TestSink.probe[ByteString])(Keep.both).run()
      decoderOut.request(Long.MaxValue)
      decoderIn.sendNext(first)
      var decoded = ByteString.empty
      while (decoded.length < piece.length) decoded ++= decoderOut.expectNext(2.seconds)
      decoded.toArray should equal(piece.toArray)
      decoderIn.sendError(new RuntimeException("done"))
      decoderOut.expectError()
      publisher.sendComplete()
      subscriber.request(Long.MaxValue)
      subscriber.receiveWithin(2.seconds)
    }

    "keep what a flush emits while the downstream is not asking, and emit it in order" in {
      val first = ByteString(words(1500))
      val second = ByteString(words(1500)).reverse
      val parameters = new Encoder.Parameters().setQuality(5)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.brotli(parameters, 50.millis)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      publisher.sendNext(first)
      Thread.sleep(300) // the flush happens without any demand
      subscriber.request(1)
      val emitted = subscriber.expectNext(2.seconds)
      publisher.sendNext(second)
      publisher.sendComplete()
      subscriber.request(Long.MaxValue)
      val rest = subscriber.receiveWithin(2.seconds)
      val all = rest.foldLeft(emitted)(_ ++ _)
      ourDecode(all).toArray should equal((first ++ second).toArray)
    }

    "stop the delayed flush flow when it is cancelled, and fail it when its upstream fails" in {
      val parameters = new Encoder.Parameters().setQuality(5)
      val (publisher, subscriber) =
        TestSource.probe[ByteString].via(BrotliCompression.brotli(parameters, 20.millis)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      subscriber.request(1)
      publisher.sendNext(ByteString("data"))
      subscriber.expectNext(2.seconds)
      subscriber.cancel()
      publisher.expectCancellation()
      val failing = TestSource.probe[ByteString].via(BrotliCompression.brotli(parameters, 20.millis)).toMat(TestSink.probe[ByteString])(Keep.both).run()
      failing._1.sendError(new RuntimeException("upstream failed"))
      failing._2.request(1)
      failing._2.expectError()
    }

    "fit the window to the body when compressing in one call with the default window" in {
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
      val parameters = new Encoder.Parameters().setQuality(5)
      def windowFor(size: Int) =
        windowBits(BrotliCompressor.compress(ByteString(words(size)).take(size), parameters))
      // 2^window - 16 is the largest distance of a match: it covers a body of the size, and no more than that
      windowFor(100) should be(10)
      windowFor(1008) should be(10)
      windowFor(1009) should be(11)
      windowFor(4080) should be(12)
      windowFor(4081) should be(13)
      windowFor(100000) should be(17)
      BrotliCompressor.windowFor(0) should be(10)
      BrotliCompressor.windowFor(16 * 1024 * 1024 - 16) should be(24)
      BrotliCompressor.windowFor(Int.MaxValue) should be(24)
    }

    "keep the window that was asked for, and the size of the output, when compressing in one call" in {
      val data = ByteString(words(5000)).take(30000)
      val asked = new Encoder.Parameters().setQuality(5).setWindow(20)
      val askedOutput = BrotliCompressor.compress(data, asked)
      askedOutput(0) should be(ByteString(Encoder.compress(data.toArray, asked))(0))
      // fitting the window changes the compressed size by a few bytes at most
      val fitted = BrotliCompressor.compress(data, new Encoder.Parameters().setQuality(5)).length
      math.abs(fitted - askedOutput.length) should be <= 8
      ourDecode(BrotliCompressor.compress(data, new Encoder.Parameters().setQuality(5))).toArray should equal(data.toArray)
    }
  }
}