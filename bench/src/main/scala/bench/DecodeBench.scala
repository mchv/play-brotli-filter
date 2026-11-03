package bench

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.decoder.DirectDecompress
import com.aayushatharva.brotli4j.encoder.Encoder
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{ BrotliCompression, Sink, Source }
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class DecodeBench {
  @Param(Array("4096", "32768", "140421"))
  var size: Int = _

  /** How the compressed bytes arrive: in one piece, or in pieces of this many bytes. */
  @Param(Array("0", "1024", "256"))
  var chunkSize: Int = _

  var compressed: Array[Byte] = _
  var pieces: Vector[ByteString] = _
  var system: ActorSystem = _

  @Setup
  def setup(): Unit = {
    Brotli4jLoader.ensureAvailability()
    compressed = Encoder.compress(Data.text(size), new Encoder.Parameters().setQuality(5))
    pieces = if (chunkSize == 0) Vector(ByteString.fromArrayUnsafe(compressed)) else Data.chunks(compressed, chunkSize)
    system = ActorSystem("bench")
  }

  @TearDown
  def tearDown(): Unit = Await.result(system.terminate(), 10.seconds)

  /** The floor: one native call through brotli4j. */
  @Benchmark
  def a_oneShotDecoder(): Array[Byte] = DirectDecompress.decompress(compressed).getDecompressedData

  @Benchmark
  def b_decoderFlow(): ByteString = {
    implicit val sys: ActorSystem = system
    Await.result(Source(pieces).via(BrotliCompression.unbrotli()).runWith(Sink.fold(ByteString.empty)(_ ++ _)), 60.seconds)
  }
}
