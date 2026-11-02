package bench

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.Encoder
import org.apache.pekko.stream.io.compression.brotli.BrotliCompressor
import org.apache.pekko.util.ByteString

/** The compressed size of one body, depending on how it reaches the encoder: all at once, or in pieces that are flushed or not. */
object Ratio {
  def main(args: Array[String]): Unit = {
    Brotli4jLoader.ensureAvailability()
    val input = Data.text(Int.MaxValue)
    val parameters = new Encoder.Parameters().setQuality(5)
    println(s"input: ${input.length} bytes, quality 5")
    println(f"${"one call"}%-34s ${Encoder.compress(input, parameters).length}%8d")
    for (pieceSize <- Seq(16384, 8192, 1024, 256); flush <- Seq(false, true)) {
      val compressor = new BrotliCompressor(parameters)
      val pieces = Data.chunks(input, pieceSize)
      val streamed = pieces.map(piece => if (flush) compressor.compressAndFlush(piece) else compressor.compress(piece))
      val size = streamed.map(_.length).sum + compressor.finish().length
      println(f"${s"pieces of $pieceSize, ${if (flush) "flushed" else "not flushed"}"}%-34s $size%8d")
    }
  }
}
