package bench

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.Encoder
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.io.compression.brotli.BrotliCompressor
import org.apache.pekko.stream.scaladsl.{ BrotliCompression, Sink, Source }
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations._

@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class EncodeBench {
  @Param(Array("4096", "32768", "140421"))
  var size: Int = _

  var input: Array[Byte] = _
  var inputBs: ByteString = _
  var chunks: Vector[ByteString] = _
  var system: ActorSystem = _

  @Setup
  def setup(): Unit = {
    Brotli4jLoader.ensureAvailability()
    input = Data.text(size)
    inputBs = ByteString.fromArrayUnsafe(input)
    chunks = Data.chunks(input, 8192)
    system = ActorSystem("bench")
  }

  @TearDown
  def tearDown(): Unit = Await.result(system.terminate(), 10.seconds)

  private def params = new Encoder.Parameters().setQuality(5)

  /** The floor: one native call through brotli4j. */
  @Benchmark
  def a_oneShotEncoder(): Array[Byte] = Encoder.compress(input, params)

  /** What BrotliFilter does for a strict body. */
  @Benchmark
  def b_strict(): ByteString = BrotliCompressor.compress(inputBs, params)

  /** The compressor of the streaming flow, without the stream. */
  @Benchmark
  def c_compressorChunked(): ByteString = {
    val compressor = new BrotliCompressor(5)
    chunks.foreach(compressor.compress)
    compressor.finish()
  }

  /** What BrotliFilter does for a response above the chunked threshold. */
  @Benchmark
  def d_streamingFlow(): ByteString = {
    implicit val sys: ActorSystem = system
    Await.result(Source(chunks).via(BrotliCompression.brotli(5)).runWith(Sink.fold(ByteString.empty)(_ ++ _)), 20.seconds)
  }
}
