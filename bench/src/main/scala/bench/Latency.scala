package bench

import scala.concurrent.Await
import scala.concurrent.duration._

import com.aayushatharva.brotli4j.Brotli4jLoader
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.stream.scaladsl.{ BrotliCompression, Sink, Source }
import org.apache.pekko.util.ByteString

/** When does the compression flow emit its first byte, and how much does it hold in memory? */
object Latency {
  def main(args: Array[String]): Unit = {
    Brotli4jLoader.ensureAvailability()
    implicit val system: ActorSystem = ActorSystem("latency")
    val chunk = ByteString(Data.text(16384))
    val start = System.nanoTime()
    def ms = (System.nanoTime() - start) / 1000000
    val log = new java.util.concurrent.ConcurrentLinkedQueue[String]()
    val done = Source(1 to 10)
      .throttle(1, 100.millis)
      .map { i => log.add(s"$ms ms: upstream chunk $i of ${chunk.length} bytes"); chunk }
      .via(BrotliCompression.brotli(5))
      .runWith(Sink.foreach(out => log.add(s"$ms ms: downstream chunk of ${out.length} bytes")))
    Await.result(done, 30.seconds)
    log.forEach(l => println(l))
    Await.result(system.terminate(), 10.seconds)
  }
}
