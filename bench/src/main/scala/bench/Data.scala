package bench

import org.apache.pekko.util.ByteString

object Data {
  /** Real world text: the minified bootstrap css of the tests, cut at the requested size. */
  def text(size: Int): Array[Byte] = {
    val in = getClass.getResourceAsStream("/bootstrap.min.css")
    val all = try in.readAllBytes() finally in.close()
    java.util.Arrays.copyOf(all, math.min(size, all.length))
  }

  def chunks(bytes: Array[Byte], chunkSize: Int): Vector[ByteString] =
    bytes.grouped(chunkSize).map(ByteString.fromArrayUnsafe).toVector
}
