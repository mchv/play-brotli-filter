package bench

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class LatencyHistogramSpec extends AnyWordSpec with Matchers {

  private def micros(values: Seq[Long]): LatencyHistogram = {
    val histogram = new LatencyHistogram
    values.foreach(v => histogram.record(v * 1000))
    histogram
  }

  "A LatencyHistogram" should {
    "be exact for latencies under 64 microseconds" in {
      val histogram = micros(Seq(5, 7, 40))
      histogram.percentile(0.33) should be(5.0)
      histogram.percentile(0.66) should be(7.0)
      histogram.percentile(1.0) should be(40.0)
    }

    "give the percentiles of a wide range within 1.6%" in {
      val random = new scala.util.Random(1)
      val values = Array.fill(200000)(math.exp(random.nextGaussian() * 1.2 + 8.0).toLong)
      val histogram = micros(values.toSeq)
      val sorted = values.sorted
      Seq(0.5, 0.95, 0.99, 0.999).foreach { p =>
        val exact = sorted(math.min(sorted.length - 1, math.ceil(sorted.length * p).toInt - 1)).toDouble
        math.abs(histogram.percentile(p) - exact) / exact should be < 0.016
      }
    }

    "keep counting after the room that the old latency arrays had" in {
      val histogram = new LatencyHistogram
      (1 to 1000000).foreach(_ => histogram.record(3000000L))
      histogram.count should be(1000000L)
      histogram.percentile(0.5) should (be >= 3000.0 and be <= 3050.0)
    }

    "add the histograms of several threads" in {
      val total = micros(Seq(1, 2, 3))
      total.add(micros(Seq(4, 5, 6)))
      total.count should be(6L)
      total.percentile(1.0) should be(6.0)
      total.percentile(0.5) should be(3.0)
    }

    "report zero when it is empty" in {
      new LatencyHistogram().percentile(0.99) should be(0.0)
    }
  }
}
