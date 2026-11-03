package bench

import java.util.concurrent.TimeUnit
import java.util.zip.Deflater

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.encoder.Encoder
import org.openjdk.jmh.annotations._

/**
 * Small bodies: the time of one native brotli call per quality and window, against zlib at level 6, which is what the gzip
 * filter uses by default. The compressed sizes are printed by the setup.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 2, time = 1)
@Measurement(iterations = 3, time = 1)
@Fork(1)
class SmallBodyBench {
  @Param(Array("1024", "4096", "16384"))
  var size: Int = _

  @Param(Array("1", "2", "3", "4", "5"))
  var quality: Int = _

  @Param(Array("-1", "13"))
  var window: Int = _

  var input: Array[Byte] = _

  @Setup
  def setup(): Unit = {
    Brotli4jLoader.ensureAvailability()
    input = Data.text(size)
    val brotli = Encoder.compress(input, parameters).length
    println(s"SIZE brotli size=$size quality=$quality window=$window compressed=$brotli zlib6=${zlib().length}")
  }

  private def parameters = new Encoder.Parameters().setQuality(quality).setWindow(window)

  private def zlib(): Array[Byte] = {
    val deflater = new Deflater(6, true)
    deflater.setInput(input)
    deflater.finish()
    val out = new Array[Byte](input.length + 64)
    val n = deflater.deflate(out)
    deflater.end()
    java.util.Arrays.copyOf(out, n)
  }

  @Benchmark
  def brotli(): Array[Byte] = Encoder.compress(input, parameters)

  /** Independent of quality and window: read it once per size. */
  @Benchmark
  def zlib6(): Array[Byte] = zlib()
}
