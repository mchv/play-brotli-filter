/*
 * Copyright 2026 Mariot Chauvin
 *
 * Mariot Chauvin licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package play.filters.brotli

import com.aayushatharva.brotli4j.encoder.Encoder
import scala.concurrent.duration._

import org.specs2.mutable.Specification
import play.api.Configuration
import play.api.http.HttpEntity
import play.api.mvc.{ Result, Results }
import play.api.test.FakeRequest

class BrotliFilterConfigSpec extends Specification {

  private val prefix = "play.filters.brotli"

  private def configured(settings: (String, Any)*): BrotliFilterConfig =
    BrotliFilterConfig.fromConfiguration(Configuration.from(settings.toMap).withFallback(Configuration.reference))

  private def invalidMessage(config: => BrotliFilterConfig): String =
    try {
      config
      "no exception"
    } catch {
      case e: IllegalArgumentException => e.getMessage
    }

  private def resultWith(contentType: Option[String]): Result = contentType match {
    case Some(value) => Results.Ok("body").as(value)
    case None        => Results.Ok.sendEntity(HttpEntity.NoEntity)
  }

  private def isBrotlied(config: BrotliFilterConfig, contentType: Option[String]): Boolean =
    config.shouldBrotli(FakeRequest(), resultWith(contentType))

  "The default configuration" should {
    "match the documented defaults" in {
      val config = configured()
      config.quality must_== 5
      config.streamQuality must_== 5
      config.window must_== -1
      config.mode must_== Encoder.Mode.GENERIC
      config.bufferSize must_== 0
      config.chunkedThreshold must_== 102400
      config.threshold must_== 0
      config.flushInterval must_== 5.millis
      isBrotlied(config, Some("image/png")) must beTrue
      isBrotlied(config, None) must beTrue
    }

    "have the same defaults when built in code" in {
      val config = BrotliFilterConfig()
      val fromReference = configured()
      config.quality must_== fromReference.quality
      config.streamQuality must_== fromReference.streamQuality
      config.window must_== fromReference.window
      config.mode must_== fromReference.mode
      config.bufferSize must_== fromReference.bufferSize
      config.chunkedThreshold must_== fromReference.chunkedThreshold
      config.threshold must_== fromReference.threshold
      config.flushInterval must_== fromReference.flushInterval
    }
  }

  "The stream quality" should {
    "be read, and be the quality of the stream encoder parameters only" in {
      val config = configured(s"$prefix.quality" -> 7, s"$prefix.streamQuality" -> 3, s"$prefix.window" -> 18, s"$prefix.mode" -> "text")
      config.streamQuality must_== 3
      config.streamEncoderParameters.quality() must_== 3
      config.streamEncoderParameters.lgwin() must_== 18
      config.streamEncoderParameters.mode() must_== Encoder.Mode.TEXT
      config.encoderParameters.quality() must_== 7
    }

    "accept every value from 0 to 11 and -1 for the encoder default" in {
      ((-1 to 11).map(q => configured(s"$prefix.streamQuality" -> q).streamEncoderParameters.quality() must_== q)).reduce(_ and _)
    }

    "be rejected outside -1 to 11, naming the setting" in {
      (invalidMessage(configured(s"$prefix.streamQuality" -> 12)) must contain("streamQuality")) and
        (invalidMessage(BrotliFilterConfig(streamQuality = -2)) must contain("streamQuality")) and
        (invalidMessage(BrotliFilterConfig().withStreamQuality(12)) must contain("streamQuality"))
    }

    "give a new instance of the stream encoder parameters each time" in {
      val config = BrotliFilterConfig()
      config.streamEncoderParameters must not(beTheSameAs(config.streamEncoderParameters))
    }
  }

  "The quality" should {
    "accept every value from 0 to 11 and -1 for the encoder default" in {
      ((-1 to 11).map(q => configured(s"$prefix.quality" -> q).encoderParameters.quality() must_== q)).reduce(_ and _)
    }

    "be rejected above 11" in {
      val message = invalidMessage(configured(s"$prefix.quality" -> 12))
      (message must contain("quality")) and (message must contain("12"))
    }

    "be rejected below -1" in {
      invalidMessage(configured(s"$prefix.quality" -> -2)) must contain("quality")
    }

    "be rejected when built in code" in {
      invalidMessage(BrotliFilterConfig(quality = 12)) must contain("quality")
      invalidMessage(BrotliFilterConfig().withQuality(-2)) must contain("quality")
    }
  }

  "The window" should {
    "accept -1 for the encoder default and 10 to 24" in {
      ((Seq(-1) ++ (10 to 24)).map(w => configured(s"$prefix.window" -> w).encoderParameters.lgwin() must_== w)).reduce(_ and _)
    }

    "be rejected below 10 and above 24" in {
      invalidMessage(configured(s"$prefix.window" -> 9)) must contain("window")
      invalidMessage(configured(s"$prefix.window" -> 25)) must contain("window")
      invalidMessage(BrotliFilterConfig().withWindow(0)) must contain("window")
    }
  }

  "The mode" should {
    "accept generic, text and font in any case" in {
      configured(s"$prefix.mode" -> "generic").encoderParameters.mode() must_== Encoder.Mode.GENERIC
      configured(s"$prefix.mode" -> "text").encoderParameters.mode() must_== Encoder.Mode.TEXT
      configured(s"$prefix.mode" -> "FONT").encoderParameters.mode() must_== Encoder.Mode.FONT
      configured(s"$prefix.mode" -> "Text").encoderParameters.mode() must_== Encoder.Mode.TEXT
    }

    "be rejected with the allowed values when unknown" in {
      val message = invalidMessage(configured(s"$prefix.mode" -> "bogus"))
      (message must contain("bogus")) and (message must contain("generic")) and (message must contain("text")) and
        (message must contain("font"))
    }
  }

  "The encoder parameters" should {
    "combine quality, window and mode" in {
      val parameters = configured(s"$prefix.quality" -> 7, s"$prefix.window" -> 18, s"$prefix.mode" -> "font").encoderParameters
      parameters.quality() must_== 7
      parameters.lgwin() must_== 18
      parameters.mode() must_== Encoder.Mode.FONT
    }

    "be a new instance each time, as brotli4j parameters are mutable" in {
      val config = BrotliFilterConfig()
      (config.encoderParameters must not(beTheSameAs(config.encoderParameters)))
    }
  }

  "The sizes" should {
    "be read with memory size units" in {
      val config = configured(
        s"$prefix.bufferSize" -> "16k",
        s"$prefix.chunkedThreshold" -> "1m",
        s"$prefix.threshold" -> "1k"
      )
      config.bufferSize must_== 16384
      config.chunkedThreshold must_== 1048576
      config.threshold must_== 1024
    }

    "be read as plain byte counts" in {
      val config = configured(s"$prefix.bufferSize" -> 1000, s"$prefix.threshold" -> 500)
      config.bufferSize must_== 1000
      config.threshold must_== 500
    }

    "accept 0 and Int.MaxValue for the buffer size, which disable the buffer" in {
      configured(s"$prefix.bufferSize" -> "0").bufferSize must_== 0
      configured(s"$prefix.bufferSize" -> Int.MaxValue.toString).bufferSize must_== Int.MaxValue
    }

    "be rejected when they do not fit in an Int" in {
      invalidMessage(configured(s"$prefix.bufferSize" -> "3g")) must contain("bufferSize")
      invalidMessage(configured(s"$prefix.chunkedThreshold" -> "3g")) must contain("chunkedThreshold")
      invalidMessage(configured(s"$prefix.threshold" -> "3g")) must contain("threshold")
    }

    "reject a negative buffer size" in {
      invalidMessage(BrotliFilterConfig(bufferSize = -1)) must contain("bufferSize")
    }

    "reject a negative threshold" in {
      invalidMessage(BrotliFilterConfig(threshold = -1)) must contain("threshold")
    }
  }

  "The content type white list" should {
    val config = configured(s"$prefix.contentType.whiteList" -> Seq("text/*", "application/json"))

    "compress the content types it matches, ignoring parameters" in {
      isBrotlied(config, Some("text/html; charset=utf-8")) must beTrue
      isBrotlied(config, Some("text/css")) must beTrue
      isBrotlied(config, Some("application/json")) must beTrue
    }

    "not compress other content types" in {
      isBrotlied(config, Some("image/png")) must beFalse
      isBrotlied(config, Some("application/javascript")) must beFalse
    }

    "not compress a response without a content type" in {
      isBrotlied(config, None) must beFalse
    }

    "match case-insensitively" in {
      isBrotlied(configured(s"$prefix.contentType.whiteList" -> Seq("TEXT/HTML")), Some("text/html")) must beTrue
      isBrotlied(config, Some("TEXT/PLAIN")) must beTrue
    }

    "accept */* to match every content type" in {
      isBrotlied(configured(s"$prefix.contentType.whiteList" -> Seq("*/*")), Some("image/png")) must beTrue
    }
  }

  "The content type black list" should {
    val config = configured(s"$prefix.contentType.blackList" -> Seq("image/*", "application/zip"))

    "not compress the content types it matches" in {
      isBrotlied(config, Some("image/png")) must beFalse
      isBrotlied(config, Some("image/svg+xml")) must beFalse
      isBrotlied(config, Some("application/zip")) must beFalse
    }

    "compress other content types" in {
      isBrotlied(config, Some("text/html; charset=utf-8")) must beTrue
      isBrotlied(config, Some("application/json")) must beTrue
    }

    "compress a response without a content type" in {
      isBrotlied(config, None) must beTrue
    }
  }

  "The white list and black list together" should {
    "ignore the black list when the white list is not empty" in {
      val config = configured(
        s"$prefix.contentType.whiteList" -> Seq("text/html"),
        s"$prefix.contentType.blackList" -> Seq("text/html")
      )
      isBrotlied(config, Some("text/html")) must beTrue
      isBrotlied(config, Some("text/css")) must beFalse
    }
  }

  "A content type that is not a media type" should {
    "be rejected in the white list, naming the setting" in {
      val message = invalidMessage(configured(s"$prefix.contentType.whiteList" -> Seq("bogus")))
      (message must contain("bogus")) and (message must contain("contentType.whiteList"))
    }

    "be rejected in the black list, naming the setting" in {
      val message = invalidMessage(configured(s"$prefix.contentType.blackList" -> Seq("text/html", "*")))
      (message must contain("contentType.blackList"))
    }
  }

  "The flush interval" should {
    "default to 5 milliseconds, in the configuration and in code" in {
      configured().flushInterval must_== 5.millis
      BrotliFilterConfig().flushInterval must_== 5.millis
    }

    "be read as a duration, and 0 flushes after every element" in {
      configured(s"$prefix.flushInterval" -> "20 ms").flushInterval must_== 20.millis
      configured(s"$prefix.flushInterval" -> "1 second").flushInterval must_== 1.second
      configured(s"$prefix.flushInterval" -> "0").flushInterval must_== Duration.Zero
      BrotliFilterConfig().withFlushInterval(Duration.Zero).flushInterval must_== Duration.Zero
    }

    "be rejected when negative" in {
      invalidMessage(configured(s"$prefix.flushInterval" -> "-1 ms")) must contain("flushInterval")
      invalidMessage(BrotliFilterConfig(flushInterval = (-1).millis)) must contain("flushInterval")
    }
  }
}
