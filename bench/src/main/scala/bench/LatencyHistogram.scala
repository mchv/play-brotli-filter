package bench

/**
 * The latencies of a run in bounded memory: a histogram with a bucket for every microsecond up to 64, then 64 buckets per
 * power of two (a relative error under 1.6%), which covers any latency. It is written by one thread; add the histograms of
 * the threads to read the percentiles of all of them.
 */
final class LatencyHistogram {
  private val SubBits = 6
  private val SubBuckets = 1 << SubBits
  private val counts = new Array[Long](SubBuckets + (63 - SubBits) * SubBuckets)

  def record(nanos: Long): Unit = counts(index(math.max(0L, nanos / 1000))) += 1

  def add(other: LatencyHistogram): Unit = {
    var i = 0
    while (i < counts.length) { counts(i) += other.counts(i); i += 1 }
  }

  def count: Long = counts.sum

  /** The latency in microseconds that `p` (from 0 to 1) of the recorded latencies do not exceed. */
  def percentile(p: Double): Double = {
    val total = count
    if (total == 0) 0.0
    else {
      val rank = math.min(total, math.max(1L, math.ceil(total * p).toLong))
      var seen = 0L
      var i = 0
      while (seen + counts(i) < rank) { seen += counts(i); i += 1 }
      upperBound(i)
    }
  }

  private def index(micros: Long): Int =
    if (micros < SubBuckets) micros.toInt
    else {
      val msb = 63 - java.lang.Long.numberOfLeadingZeros(micros)
      SubBuckets + (msb - SubBits) * SubBuckets + ((micros >>> (msb - SubBits)) & (SubBuckets - 1)).toInt
    }

  private def upperBound(index: Int): Double =
    if (index < SubBuckets) index.toDouble
    else {
      val k = index - SubBuckets
      val msb = k / SubBuckets + SubBits
      val sub = k % SubBuckets
      ((1L << msb) | (sub.toLong << (msb - SubBits))) + (1L << (msb - SubBits)) - 1.0
    }
}
