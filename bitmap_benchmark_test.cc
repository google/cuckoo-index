// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// -----------------------------------------------------------------------------
// File: bitmap_benchmark_test.cc
// -----------------------------------------------------------------------------
//
// Benchmarks for Roaring and ZSTD-compressed bitmaps.
//
// bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off
//   :bitmap_benchmark_test
//
// Example run with bitmap index on DMV `city` column:
// - 2^13 rows per stripe (1,447 stripes)
// - 47,085,380 bits of which 2,021,715 are set (density of 4.3%)
//
// Example run with bitmap index on DMV `city` column:
// - 2^13 rows per stripe (1,447 stripes)
// - 47,085,380 bits of which 2,021,715 are set (density of 4.3%)
//
// Run on (8 X 2300 MHz CPUs); 2019-10-25T05:57:56
// CPU: Intel Haswell with HyperThreading (4 cores) dL1:32KB dL2:256KB dL3:45MB
// Benchmark                       Time(ns)        CPU(ns)     Iterations
// ----------------------------------------------------------------------
// BM_RLECompress                 123830177      123814940              5
// BM_RLEDecompress               162191030      162167372              4
// BM_RLEDecompressPartial           246357         246357           3100
// BM_RoaringCompressFromIndexes   20305821       20308686             34
// BM_RoaringCompressFromBitmap    41166051       41159732             17
// BM_RoaringDecompressToIndexes    5048275        5049205            100
// BM_RoaringDecompressToBitmap    17180132       17180604             40
// BM_ZstdCompressBitmapBytes      11173145       11174765             63
// BM_ZstdDecompressBitmapBytes     6565571        6566545            100

#include "common/bitmap.h"
#include "common/rle_bitmap.h"
#include "evaluation_utils.h"
#include "benchmark/benchmark.h"
#include "absl/flags/flag.h"
#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "external/com_google_benchmark/_virtual_includes/benchmark/benchmark/benchmark.h"

ABSL_FLAG(std::string, path, "", "Path to bitmap file.");

namespace ci {
namespace {

// **** Helper methods ****

std::vector<uint32_t> IndexesFromBitmap(const Bitmap64& bitmap) {
  std::vector<uint32_t> indexes;
  for (const uint32_t index : bitmap.TrueBitIndices()) {
    indexes.push_back(index);
  }
  return indexes;
}

std::vector<uint32_t> IndexesFromRoaring(const Roaring& roaring) {
  std::vector<uint32_t> indexes;
  indexes.resize(roaring.cardinality());
  roaring.toUint32Array(indexes.data());
  return indexes;
}

Roaring RoaringFromIndexes(const std::vector<uint32_t>& indexes) {
  return Roaring(indexes.size(), indexes.data());
}

std::string RoaringToBytes(const Roaring& roaring) {
  std::string result;
  result.resize(roaring.getSizeInBytes(/*portable=*/false));
  roaring.write(result.data());
  return result;
}

Roaring RoaringFromBytes(const std::string& bytes) {
  return Roaring::readSafe(bytes.data(), bytes.size());
}

// **** RLE benchmarks ****

void BM_RLECompress(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));

  while (state.KeepRunning()) {
    const RleBitmap rle_bitmap(bitmap);
  }
}
BENCHMARK(BM_RLECompress);

void BM_RLEDecompress(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const RleBitmap rle_bitmap(bitmap);

  while (state.KeepRunning()) {
    const Bitmap64 extracted =
        rle_bitmap.Extract(/*offset=*/0, /*size=*/bitmap.bits());
  }
}
BENCHMARK(BM_RLEDecompress);

// Extract a bitmap that corresponds to a single unique value from a
// back-to-back encoded bitmap (global bitmap). Each bit in this extracted
// bitmap would correspond to a stripe.
void BM_RLEDecompressPartial(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const RleBitmap rle_bitmap(bitmap);

  while (state.KeepRunning()) {
    const Bitmap64 extracted = rle_bitmap.Extract(/*offset=*/bitmap.bits() / 2,
                                                  /*size=*/128);
  }
}
BENCHMARK(BM_RLEDecompressPartial);

// **** Roaring benchmarks ****

void BM_RoaringCompressFromIndexes(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const std::vector<uint32_t> indexes = IndexesFromBitmap(bitmap);

  while (state.KeepRunning()) {
    const Roaring roaring = RoaringFromIndexes(indexes);
    benchmark::DoNotOptimize(RoaringToBytes(roaring));
  }
}
BENCHMARK(BM_RoaringCompressFromIndexes);

void BM_RoaringCompressFromBitmap(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));

  while (state.KeepRunning()) {
    Roaring roaring;
    for (const uint32_t index : bitmap.TrueBitIndices()) {
      roaring.add(index);
    }
    benchmark::DoNotOptimize(RoaringToBytes(roaring));
  }
}
BENCHMARK(BM_RoaringCompressFromBitmap);

void BM_RoaringDecompressToIndexes(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const std::vector<uint32_t> indexes = IndexesFromBitmap(bitmap);
  Roaring roaring = RoaringFromIndexes(indexes);
  const std::string bytes = RoaringToBytes(roaring);

  while (state.KeepRunning()) {
    const Roaring roaring = RoaringFromBytes(bytes);
    benchmark::DoNotOptimize(IndexesFromRoaring(roaring));
  }
}
BENCHMARK(BM_RoaringDecompressToIndexes);

void BM_RoaringDecompressToBitmap(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const std::vector<uint32_t> indexes = IndexesFromBitmap(bitmap);
  Roaring roaring = RoaringFromIndexes(indexes);
  const std::string bytes = RoaringToBytes(roaring);

  Bitmap64 decompressed(/*size=*/bitmap.bits());
  while (state.KeepRunning()) {
    const Roaring roaring = RoaringFromBytes(bytes);
    for (RoaringSetBitForwardIterator it = roaring.begin(); it != roaring.end();
         ++it) {
      decompressed.Set(*it, true);
    }
  }
}
BENCHMARK(BM_RoaringDecompressToBitmap);

// **** ZSTD benchmarks ****

void BM_ZstdCompressBitmapBytes(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const std::string bitmap_bytes = SerializeBitmap(bitmap);

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(Compress(bitmap_bytes));
  }
}
BENCHMARK(BM_ZstdCompressBitmapBytes);

void BM_ZstdDecompressBitmapBytes(benchmark::State& state) {
  const Bitmap64 bitmap = ReadBitmapFromFile(absl::GetFlag(FLAGS_path));
  const std::string bitmap_bytes = SerializeBitmap(bitmap);
  const std::string zstd_bytes = Compress(bitmap_bytes);

  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(Compress(zstd_bytes));
  }
}
BENCHMARK(BM_ZstdDecompressBitmapBytes);

}  // namespace
}  // namespace clt
