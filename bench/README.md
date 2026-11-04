# Benchmarks

[JMH](https://github.com/openjdk/jmh) benchmarks of the brotli encoders and decoders. This module is not published and
the root project does not aggregate it, so `+test`, `+testFull` and `+publishSigned` do not touch it.

The input is the minified bootstrap css of the Play tests, cut at 4 KB, 32 KB and its full 140 KB size: real text, and
sizes from a small to a typical response.

| Benchmark | What it measures |
|---|---|
| `EncodeBench.a_oneShotEncoder` | The floor: one native brotli4j call |
| `EncodeBench.b_strict` | What the Play filter does for a strict body |
| `EncodeBench.c_compressorChunked` | `BrotliCompressor` fed with 8 KB pieces, without the stream |
| `EncodeBench.d_streamingFlow` | `BrotliCompression.brotli`, the flow of the Play filter for a response above the chunked threshold |
| `DecodeBench.a_oneShotDecoder` | The floor: one native brotli4j call |
| `DecodeBench.b_decoderFlow` | `BrotliCompression.unbrotli`, with the compressed bytes arriving in one piece or in pieces of 1 KB or 256 bytes |
| `FilterBench.respond` | The brotli filter against Play's gzip filter (levels 6 and 9), on strict, streamed, chunked and tiny-chunked responses, from the request to the last byte of the body, with a `none` row for the cost of the filter and of the benchmark. `br4` and `br5` compress every response at that quality, `br` is the default configuration (quality 5, stream quality 4). The setup prints the compressed sizes |
| `SmallBodyBench` | Small bodies: one native call per quality and window, against zlib at level 6 (the gzip filter default). The setup prints the compressed sizes |
| `LoadTest` | A Play application on the Netty server with the brotli filter or Play's gzip filter, and clients on real connections: throughput, latency, CPU, garbage collection and allocation per request, and a check that the compressed responses decode to the right body (a `main`, run by `load-test.sh`) |
| `QualityBench.oneShot` | Time per quality; the compressed size per quality is printed |
| `Latency` | When the compression flow emits its first byte (a `main`, not a JMH benchmark) |
| `Ratio` | The compressed size depending on how the body reaches the encoder: in one call, or in pieces that are flushed or not (a `main`) |

Run them from the root of the repository (JDK 17 or newer, as for the build):

```
sbt "bench/Jmh/run -prof gc bench.EncodeBench"
sbt "bench/Jmh/run -prof gc bench.DecodeBench"
sbt "bench/Jmh/run -prof gc bench.FilterBench"
sbt "bench/Jmh/run bench.QualityBench"
sbt "bench/runMain bench.Latency"
sbt "bench/runMain bench.Ratio"
```

`-prof gc` adds the allocation per operation (`gc.alloc.rate.norm`), which says as much as the time here: most of the
cost beyond the floor is copying. Compare the numbers of two commits on the same machine in the same session, and keep
in mind that a virtual machine with a few cores is noisy: look at the ratios and at the error column.

## Load test

`bench/load-test.sh [connections] [warm-up seconds] [seconds] [verify every n] [series] [filters] [endpoints]` starts a Play
application for each combination of filter (`gzip`, `br`; `brN` for a flush interval of N ms, `brqN` for a stream quality of N) and endpoint (`small` 4 KB, `large` 140 KB, `streamed`, `chunked` and `tiny`, the
last three in pieces of 8 KB, 8 KB and 256 bytes), in a JVM of its own, and repeats the whole series so that the noise of
the machine averages out. `bench/load-summary.sh <log>` prints the median of each figure. The server and the clients
share the JVM, hence the CPU: compare the filters with each other, not with other machines.


## Measuring two commits

The `bench` workflow (`.github/workflows/bench.yml`) checks out two commits on the same runner and runs `FilterBench`
and the load test for each, the load test series alternating between the two. It runs on a pull request that gets the
label `bench`, measuring the pull request against its base, and by hand from the Actions tab or with
`gh workflow run bench.yml -f base=main -f head=<branch>`, where the codecs, filters and series can be set. The summary
of the run has the figures, and the logs are an artifact. The runner is shared and noisy: compare the two commits with
each other, and each brotli row with the gzip row of its own run.
