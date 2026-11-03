# Brotli filter for play

[![License](https://img.shields.io/:license-Apache%202.0-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0)

> **Active development happens in this repository.** It is a fork of [guardian/play-brotli-filter](https://github.com/guardian/play-brotli-filter), maintained independently by [@mchv](https://github.com/mchv); the Guardian repository is no longer where development takes place. Releases from here are published under the `io.github.mchv` group id. Releases up to `0.18.0` were published by the Guardian as `com.gu`, so update your dependency's group id when you move to a newer version. The artifact names are unchanged.

 * A [Brotli](https://opensource.googleblog.com/2015/09/introducing-brotli-new-compression.html) filter for the [Play framework](https://www.playframework.com/)
 * A [Brotli](https://opensource.googleblog.com/2015/09/introducing-brotli-new-compression.html) compression `Operator` and a decompression `Operator` for [Akka streams](https://doc.akka.io/docs/akka/current/stream/stream-flows-and-basics.html) 
 * A [Brotli](https://opensource.googleblog.com/2015/09/introducing-brotli-new-compression.html) compression `Operator` and a decompression `Operator` for [Apache Pekko](https://pekko.apache.org/) streams 


## Install

Add as a dependency:

* Play **3.0** use [![play-v30-brotli-filter](https://index.scala-lang.org/mchv/play-brotli-filter/play-v30-brotli-filter/latest-by-scala-version.svg)](https://index.scala-lang.org/mchv/play-brotli-filter/play-v30-brotli-filter/)
  ```scala
  libraryDependencies += "io.github.mchv" %% "play-v30-brotli-filter" % "[latest version number]"
  ```
* Play **2.9** use [![play-v29-brotli-filter](https://index.scala-lang.org/mchv/play-brotli-filter/play-v29-brotli-filter/latest-by-scala-version.svg)](https://index.scala-lang.org/mchv/play-brotli-filter/play-v29-brotli-filter/)
  ```scala
  libraryDependencies += "io.github.mchv" %% "play-v29-brotli-filter" % "[latest version number]"
  ```
* Play **2.8** use [![play-v28-brotli-filter](https://index.scala-lang.org/mchv/play-brotli-filter/play-v28-brotli-filter/latest-by-scala-version.svg)](https://index.scala-lang.org/mchv/play-brotli-filter/play-v28-brotli-filter/)
  ```scala
  libraryDependencies += "io.github.mchv" %% "play-v28-brotli-filter" % "[latest version number]"
  ```

* Akka use [![akka-stream-brotli](https://index.scala-lang.org/mchv/play-brotli-filter/akka-stream-brotli/latest-by-scala-version.svg)](https://index.scala-lang.org/mchv/play-brotli-filter/akka-stream-brotli/)
  ```scala
  libraryDependencies += "io.github.mchv" %% "akka-stream-brotli" % "[latest version number]"
  ```
  
* Pekko use [![pekko-stream-brotli](https://index.scala-lang.org/mchv/play-brotli-filter/pekko-stream-brotli/latest-by-scala-version.svg)](https://index.scala-lang.org/mchv/play-brotli-filter/pekko-stream-brotli/)
  ```scala
  libraryDependencies += "io.github.mchv" %% "pekko-stream-brotli" % "[latest version number]"
  ```

## Configure

The filter is configured under `play.filters.brotli`. All the settings are optional, these are the defaults:

```
play.filters {

  # Brotli filter configuration
  brotli {

    # The compression-speed vs compression-density tradeoffs. The higher the quality, the slower the compression.
    # Range is 0 (fastest, largest output) to 11 (slowest, smallest output). -1 uses the default of the brotli
    # encoder, which is 11.
    quality = 5

    # The log2 of the sliding window size, from 10 (1 KiB) to 24 (16 MiB). A larger window can find more repetitions in
    # large responses but needs more memory, also in the browser. -1 uses the default of the brotli encoder.
    window = -1

    # The kind of content the encoder is tuned for: "generic", "text" (UTF-8 text) or "font" (WOFF 2.0 fonts).
    mode = "generic"

    # The maximum amount of data to send to the brotli compressor in one go. Use `0` or `2147483647` to disable the buffering.
    bufferSize = 8k

    # The maximum amount of content to buffer for compressing with brotli in order to calculate the content length before falling back
    # to chunked encoding.
    chunkedThreshold = 100k

    contentType {

      # If non empty, then a response will only be compressed if its content type is in this list.
      whiteList = []

      # The black list is only used if the white list is empty.
      # Compress all responses except the ones whose content type is in this list.
      blackList = []
    }

    # The longest time that the compressed data of a streamed or chunked response waits in the encoder before it is sent.
    # The encoder is flushed only when its oldest data has waited that long, and at the end of the response. Flushing after
    # every chunk, as the gzip filter does, ends a block each time and costs compressed size and CPU when the chunks are
    # small: chunks of 8 KB cost 2% of the size, chunks of 1 KB 16%, chunks of 256 bytes 56%. 0 flushes after every chunk.
    flushInterval = 5 ms

    # The byte threshold for the response body size which controls if a response should be compressed (e.g. 1k).
    # Responses of this size or smaller are not compressed. If the body size cannot be determined, then it is assumed
    # to be over the threshold.
    threshold = 0

  }
}
```

For example, to only compress text and JSON responses, and only when they are larger than 1 KiB:

```
play.filters.brotli {
  contentType.whiteList = ["text/*", "application/json"]
  threshold = 1k
}
```

* **Quality:** the default is `5`, because the highest quality, `11`, is much slower and is rarely worth it when compressing responses on the fly. Set it to `11` or `-1` for the smallest output, for example when the responses are cached.
* **Content types:** an entry is a media type such as `text/html`, `text/*` or `*/*`. The matching ignores the case and the parameters (`; charset=utf-8`) of the response content type. With a white list, a response without a content type is not compressed. With only a black list, it is.
* **Invalid values:** the application fails to start with a message that names the setting, for example when the quality is above 11 or an entry of a content type list is not a media type.
* **Code:** `BrotliFilterConfig` has the same settings (`withQuality`, `withWindow`, `withMode`, `withBufferSize`, `withChunkedThreshold`, `withThreshold`, `withFlushInterval` and `withShouldBrotli`) for filters that are not created from the configuration.

## Streaming

The Play filter compresses a response as it is produced: a streamed or chunked response is not held in memory until it ends, and its compressed bytes are sent at the latest `flushInterval` (5 ms by default) after the chunk that they come from is available. Chunks that arrive within the interval share a block, so the size of the output is the size of the output of a single call, whatever the size of the chunks. Only a strict body, or a streamed body that is no larger than `chunkedThreshold`, is compressed in one call.

The Akka and Pekko modules, which the filter is built on, are incremental as well:

* `BrotliCompression.brotli(level)` compresses a stream. `BrotliCompression.brotli(parameters)` takes a brotli4j `Encoder.Parameters` to also set the window and the mode. Like the gzip flow, they flush after every element, so that every element that comes out can be decoded as it arrives. Flushing costs compression ratio when the elements are small. `BrotliCompression.brotli(parameters, maxFlushDelay)` flushes only at the end of the stream and when the oldest data that the encoder holds has waited for `maxFlushDelay`; this is the flow of the Play filter. For a 140 KB css file at quality 5, compared with compressing it in one call, elements of 16 KB cost 1%, elements of 8 KB cost 2%, elements of 1 KB cost 16% and elements of 256 bytes cost 56%.
* `BrotliCompression.unbrotli()` decompresses a stream and emits the decoded data as it is produced, in pieces of at most 64 KiB, whatever the size of the pieces it receives. For input that you do not trust, use `BrotliCompression.unbrotli(maxOutputSize)`: brotli can expand a few kilobytes into gigabytes. The stream fails as soon as the output would exceed the limit, and nothing beyond the limit is emitted. The stream also fails when the input is truncated, corrupt, or followed by other data.
* `BrotliCompressor.compress(data, parameters)` compresses a body that is already in memory in one call.

The [benchmarks](bench/README.md) measure these against a single native brotli4j call.
