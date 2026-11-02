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

import scala.concurrent.duration._

import org.apache.pekko.stream.io.compression.brotli.CoderSpec
import org.apache.pekko.stream.impl.io.compression.{ Compressor, GzipCompressor }
import org.apache.pekko.stream.scaladsl.{Flow, BrotliCompression, Source}
import org.apache.pekko.util.ByteString

class BrotliSpec extends CoderSpec("brotli") {
  import CompressionTestingTools._


  {
    Brotli4jLoader.ensureAvailability();
  }

  private def wordsText: String = {
    val random = new scala.util.Random(42)
    val vocabulary = Vector("alpha", "beta", "gamma", "delta", "akka", "brotli", "stream", "flow")
    Iterator.continually(vocabulary(random.nextInt(vocabulary.size))).take(8000).mkString(" ")
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
      compressed should equal(new BrotliCompressor(parameters).compressAndFinish(largeTextBytes))
    }
  }
}