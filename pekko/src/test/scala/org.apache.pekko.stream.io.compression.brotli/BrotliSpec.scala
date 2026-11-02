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

import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._

import org.apache.pekko.stream.io.compression.brotli.CoderSpec
import org.apache.pekko.stream.impl.io.compression.{ Compressor, GzipCompressor }
import org.apache.pekko.stream.scaladsl.{BrotliCompression, Flow, Keep, Sink, Source}
import org.apache.pekko.stream.testkit.scaladsl.{TestSink, TestSource}
import org.apache.pekko.util.ByteString

class BrotliSpec extends CoderSpec("brotli") {
  import CompressionTestingTools._


  {
    Brotli4jLoader.ensureAvailability();
  }

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
      ourDecode(fast).toArray should equal(data.toArray)
      ourDecode(best).toArray should equal(data.toArray)
    }

    "use the encoder default for a quality of -1" in {
      val data = ByteString(wordsText, "UTF8")
      new BrotliCompressor(-1).compressAndFinish(data).toArray should equal(new BrotliCompressor(11).compressAndFinish(data).toArray)
    }

    "honor the window of the encoder parameters" in {
      val block = highEntropyText
      val data = ByteString(block + block, "UTF8")
      val small = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setWindow(10)).compressAndFinish(data)
      val large = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setWindow(16)).compressAndFinish(data)
      small.length should be > (large.length * 3 / 2)
      ourDecode(small).toArray should equal(data.toArray)
      ourDecode(large).toArray should equal(data.toArray)
    }

    "compress with every encoder mode" in {
      Encoder.Mode.values().foreach { mode =>
        val compressed = new BrotliCompressor(new Encoder.Parameters().setQuality(5).setMode(mode)).compressAndFinish(largeTextBytes)
        ourDecode(compressed).toArray should equal(largeTextBytes.toArray)
      }
    }

    "create the compression flow from encoder parameters" in {
      val parameters = new Encoder.Parameters().setQuality(3).setWindow(12).setMode(Encoder.Mode.TEXT)
      val compressed = Source.single(largeTextBytes).via(BrotliCompression.brotli(parameters)).join.awaitResult(3.seconds)
      ourDecode(compressed).toArray should equal(largeTextBytes.toArray)
    }

    "decode a stream that arrives one byte at a time without redecoding what it has already decoded" in {
      val data = ByteString(words(400000))
      val compressed = new BrotliCompressor(5).compressAndFinish(data)
      val pieces = compressed.grouped(1).toVector
      pieces.length should be > 10000
      val decoded = Source(pieces).via(BrotliCompression.unbrotli()).join.awaitResult(10.seconds)
      decoded.toArray should equal(data.toArray)
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
      (first ++ rest.join).toArray should equal(data.toArray)
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
      decoded.toArray should equal(data.toArray)
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
      decoded.toArray should equal(chunk.toArray)
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
      ourDecode(all).toArray should equal(pieces.join.toArray)
    }

    "compress input larger than the input buffer of the encoder" in {
      val data = ByteString(words(300000))
      data.length should be > 1024 * 1024
      val compressor = new BrotliCompressor(5)
      val compressed = compressor.compress(data) ++ compressor.finish()
      ourDecode(compressed).toArray should equal(data.toArray)
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
        ourDecode(BrotliCompressor.compress(data, parameters)).toArray should equal(data.toArray)
      }
    }

    "compress a strict body made of several pieces like a single piece" in {
      val parameters = new Encoder.Parameters().setQuality(5)
      val pieces = Vector("one ", "two ", "three").map(ByteString(_))
      val rope = pieces.join
      BrotliCompressor.compress(rope, parameters) should equal(BrotliCompressor.compress(ByteString.fromArray(rope.toArray), parameters))
      BrotliCompressor.compress(rope.drop(2).take(8), parameters) should equal(
        ByteString(Encoder.compress("e two th".getBytes("UTF-8"), parameters)))
    }
  }
}