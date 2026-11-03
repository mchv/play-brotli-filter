package bench

import java.io.ByteArrayInputStream
import java.lang.management.ManagementFactory
import java.net.URI
import java.net.http.{ HttpClient, HttpRequest, HttpResponse }
import java.util.zip.GZIPInputStream
import java.util.concurrent.atomic.AtomicLong

import scala.concurrent.duration._

import com.aayushatharva.brotli4j.Brotli4jLoader
import com.aayushatharva.brotli4j.decoder.BrotliInputStream
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import play.api.http.{ HttpChunk, HttpEntity }
import play.api.mvc.{ EssentialFilter, ResponseHeader, Result }
import play.api.routing.Router
import play.api.routing.sird._
import play.api.BuiltInComponents
import play.core.server.{ NettyServerComponents, ServerConfig }
import play.filters.brotli.{ BrotliFilter, BrotliFilterConfig }
import play.filters.gzip.{ GzipFilter, GzipFilterConfig }

/**
 * A Play application on the Netty server with one compression filter, and clients that request it concurrently over
 * real connections. It reports the throughput, the latency, and the CPU, the garbage collection and the allocation per
 * request, and it checks that the compressed responses decode to the right body.
 *
 * Usage: `LoadTest <gzip|br> <small|large|streamed|chunked|tiny> <connections> <warm-up seconds> <seconds> <verify every n>`
 *
 * The server and the clients share the JVM, hence the CPU: compare filters with the same arguments, not with other
 * machines. The CPU per request includes the clients, which do the same work for each filter apart from decoding the
 * responses that they verify.
 */
object LoadTest {

  private val css: Array[Byte] = Data.text(Int.MaxValue)

  private def chunks(size: Int): Vector[ByteString] = Data.chunks(css, size)

  private def lastChunk = Source.single(HttpChunk.LastChunk(play.api.mvc.Headers()))

  private def chunked(size: Int): Result =
    Result(
      ResponseHeader(200),
      HttpEntity.Chunked(Source(chunks(size)).map(HttpChunk.Chunk(_)).concat(lastChunk), Some("text/css")))

  def main(args: Array[String]): Unit = {
    val Array(filterName, endpoint, connectionsArg, warmUpArg, secondsArg, verifyArg) = args
    val connections = connectionsArg.toInt
    val warmUp = warmUpArg.toInt.seconds
    val duration = secondsArg.toInt.seconds
    val verifyEvery = verifyArg.toInt
    Brotli4jLoader.ensureAvailability()

    val components = new NettyServerComponents with BuiltInComponents {
      override lazy val serverConfig: ServerConfig = ServerConfig(port = Some(0), address = "127.0.0.1")

      override lazy val httpFilters: Seq[EssentialFilter] = Seq(filterName match {
        case "gzip" => new GzipFilter(GzipFilterConfig.fromConfiguration(configuration))(materializer)
        case "br"   => new BrotliFilter(BrotliFilterConfig.fromConfiguration(configuration))(materializer)
      })

      override lazy val router: Router = Router.from {
        case GET(p"/small") =>
          defaultActionBuilder(
            Result(ResponseHeader(200), HttpEntity.Strict(ByteString.fromArray(css.take(4096)), Some("text/css"))))
        case GET(p"/large") =>
          defaultActionBuilder(
            Result(ResponseHeader(200), HttpEntity.Strict(ByteString.fromArrayUnsafe(css), Some("text/css"))))
        case GET(p"/streamed") =>
          defaultActionBuilder(
            Result(
              ResponseHeader(200),
              HttpEntity.Streamed(Source(chunks(8192)), Some(css.length.toLong), Some("text/css"))))
        case GET(p"/chunked") => defaultActionBuilder(chunked(8192))
        case GET(p"/tiny")    => defaultActionBuilder(chunked(256))
      }
    }
    val server = components.server
    val port = server.httpPort.get

    val expected = if (endpoint == "small") css.take(4096) else css
    val encoding = if (filterName == "gzip") "gzip" else "br"
    val client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()
    val request = HttpRequest.newBuilder(new URI(s"http://127.0.0.1:$port/$endpoint")).header("Accept-Encoding", encoding).build()

    def decode(body: Array[Byte]): Array[Byte] = {
      val in = if (filterName == "gzip") new GZIPInputStream(new ByteArrayInputStream(body)) else new BrotliInputStream(new ByteArrayInputStream(body))
      try in.readAllBytes()
      finally in.close()
    }

    val start = System.nanoTime()
    val measureFrom = start + warmUp.toNanos
    val end = measureFrom + duration.toNanos
    val failures = new AtomicLong
    val bytes = new AtomicLong
    val latencies = Array.fill(connections)(new Array[Long](2000000))
    val counts = new Array[Int](connections)

    val os = ManagementFactory.getOperatingSystemMXBean.asInstanceOf[com.sun.management.OperatingSystemMXBean]
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val gcs = ManagementFactory.getGarbageCollectorMXBeans
    def allocated(): Long = threads.getThreadAllocatedBytes(threads.getAllThreadIds).filter(_ > 0).sum
    def gcMillis(): Long = { var total = 0L; gcs.forEach(gc => total += gc.getCollectionTime); total }
    var cpu0, alloc0, gc0 = 0L

    val workers = (0 until connections).map { i =>
      val thread = new Thread(() => {
        var n = 0
        var verified = 0
        while (System.nanoTime() < end) {
          val sent = System.nanoTime()
          val response = client.send(request, HttpResponse.BodyHandlers.ofByteArray())
          val took = System.nanoTime() - sent
          val ok = response.statusCode == 200 && response.headers.firstValue("Content-Encoding").orElse("") == encoding
          if (!ok) failures.incrementAndGet()
          n += 1
          if (n % verifyEvery == 0 && !java.util.Arrays.equals(decode(response.body), expected)) failures.incrementAndGet()
          if (sent >= measureFrom) {
            if (counts(i) < latencies(i).length) { latencies(i)(counts(i)) = took; counts(i) += 1 }
            bytes.addAndGet(response.body.length.toLong)
          }
        }
      })
      thread.start()
      thread
    }

    Thread.sleep(warmUp.toMillis)
    cpu0 = os.getProcessCpuTime; alloc0 = allocated(); gc0 = gcMillis()
    Thread.sleep(duration.toMillis)
    val cpu1 = os.getProcessCpuTime; val alloc1 = allocated(); val gc1 = gcMillis()
    workers.foreach(_.join())
    server.stop()

    val all = (0 until connections).flatMap(i => latencies(i).take(counts(i))).sorted
    val requests = all.size.toLong
    def percentile(p: Double): Double = all((all.size * p).toInt.min(all.size - 1)) / 1000.0
    println(
      f"RESULT filter=$filterName%-4s endpoint=$endpoint%-8s connections=$connections%-3d requests=$requests%-7d " +
        f"rps=${requests / duration.toSeconds.toDouble}%8.0f p50=${percentile(0.5)}%8.0fus p95=${percentile(0.95)}%8.0fus p99=${percentile(0.99)}%8.0fus " +
        f"bytes/resp=${bytes.get / requests}%7d cpu/req=${(cpu1 - cpu0) / 1000.0 / requests}%7.0fus " +
        f"alloc/req=${(alloc1 - alloc0) / 1024.0 / requests}%7.0fKB gc=${(gc1 - gc0) / duration.toSeconds.toDouble}%5.0fms/s failures=${failures.get}")
    // The exit status stays 0: a failing command would stop the commands that follow it in the same sbt run
    sys.exit(0)
  }
}
