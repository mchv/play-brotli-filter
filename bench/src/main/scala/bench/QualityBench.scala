package bench

import java.util.concurrent.TimeUnit

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.Encoder
import org.openjdk.jmh.annotations._

/** Time per quality, one-shot. The compressed size per quality is printed by the setup. */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
class QualityBench {
  @Param(Array("1", "3", "4", "5", "6", "8", "9", "11"))
  var quality: Int = _

  @Param(Array("32768", "140421"))
  var size: Int = _

  var input: Array[Byte] = _

  @Setup
  def setup(): Unit = {
    Brotli4jLoader.ensureAvailability()
    input = Data.text(size)
    val out = Encoder.compress(input, new Encoder.Parameters().setQuality(quality)).length
    println(f"SIZE quality=$quality size=$size compressed=$out ratio=${input.length.toDouble / out}%.2f")
  }

  @Benchmark
  def oneShot(): Array[Byte] = Encoder.compress(input, new Encoder.Parameters().setQuality(quality))
}
