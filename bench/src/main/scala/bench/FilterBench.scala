package bench

import java.util.concurrent.TimeUnit

import scala.concurrent.Await
import scala.concurrent.duration._

import com.aayushatharva.brotli4j.Brotli4jLoader
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.Materializer
import org.apache.pekko.stream.scaladsl.Source
import org.apache.pekko.util.ByteString
import org.openjdk.jmh.annotations._
import play.api.http.{ HttpChunk, HttpEntity }
import play.api.libs.streams.Accumulator
import play.api.mvc.{ EssentialAction, EssentialFilter, RequestHeader, ResponseHeader, Result }
import play.api.test.FakeRequest
import play.filters.brotli.{ BrotliFilter, BrotliFilterConfig }
import play.filters.gzip.{ GzipFilter, GzipFilterConfig }

/**
 * The brotli filter against the gzip filter of Play, each run on the same response from the request to the last byte
 * of the compressed body, without a network. The compressed size of each combination is printed by the setup.
 *
 * `codec`: `none` is the brotli filter for a request that does not accept brotli, so the response is not compressed:
 * the cost of the filter and of the benchmark, which the other codecs include. `gzip` is Play's default level (-1, which is 6), `gzip9` its best level, `br4` and `br5` are brotli
 * qualities 4 and 5 (5 is the default of the brotli filter).
 *
 * `body`: `strict` is a whole body in memory; `streamed` is a body of known length that is above the chunked
 * threshold, so it is compressed as a stream; `chunked` is the same body as a chunked response; `chunkedTiny` is
 * that body in chunks of 256 bytes, which the filters flush one by one.
 */
@State(Scope.Benchmark)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(1)
class FilterBench {
  @Param(Array("none", "gzip", "gzip9", "br4", "br5"))
  var codec: String = _

  @Param(Array("strict4k", "strict140k", "streamed140k", "chunked140k", "chunkedTiny140k"))
  var body: String = _

  var system: ActorSystem = _
  var materializer: Materializer = _
  var filter: EssentialFilter = _
  var action: EssentialAction = _
  var request: RequestHeader = _

  @Setup
  def setup(): Unit = {
    Brotli4jLoader.ensureAvailability()
    system = ActorSystem("bench")
    materializer = Materializer.matFromSystem(system)
    implicit val implicitMaterializer: Materializer = materializer

    filter = codec match {
      case "gzip"  => new GzipFilter(GzipFilterConfig())
      case "gzip9" => new GzipFilter(GzipFilterConfig(compressionLevel = 9))
      case "br4"   => new BrotliFilter(BrotliFilterConfig(quality = 4))
      case "br5"   => new BrotliFilter(BrotliFilterConfig(quality = 5))
      case "none"  => new BrotliFilter(BrotliFilterConfig())
    }
    request = codec match {
      case "none"                      => FakeRequest()
      case gzip if gzip.startsWith("gzip") => FakeRequest().withHeaders("Accept-Encoding" -> "gzip")
      case _                           => FakeRequest().withHeaders("Accept-Encoding" -> "br")
    }
    val result = response()
    action = filter(EssentialAction(_ => Accumulator.done(result)))

    val size = run().length
    println(s"SIZE codec=$codec body=$body compressed=$size")
  }

  @TearDown
  def tearDown(): Unit = Await.result(system.terminate(), 10.seconds)

  private def response(): Result = {
    val text = Data.text(if (body == "strict4k") 4096 else Int.MaxValue)
    val contentType = Some("text/css")
    val entity = body match {
      case "strict4k" | "strict140k" => HttpEntity.Strict(ByteString.fromArrayUnsafe(text), contentType)
      case "streamed140k" =>
        HttpEntity.Streamed(Source(Data.chunks(text, 8192)), Some(text.length.toLong), contentType)
      case "chunked140k" =>
        HttpEntity.Chunked(Source(Data.chunks(text, 8192)).map(HttpChunk.Chunk(_)).concat(Source.single(HttpChunk.LastChunk(play.api.mvc.Headers()))), contentType)
      case "chunkedTiny140k" =>
        HttpEntity.Chunked(Source(Data.chunks(text, 256)).map(HttpChunk.Chunk(_)).concat(Source.single(HttpChunk.LastChunk(play.api.mvc.Headers()))), contentType)
    }
    Result(ResponseHeader(200), entity)
  }

  private def run(): ByteString = {
    implicit val implicitMaterializer: Materializer = materializer
    val compressed = action(request).run().flatMap(_.body.consumeData)(system.dispatcher)
    Await.result(compressed, 30.seconds)
  }

  @Benchmark
  def respond(): ByteString = run()
}
