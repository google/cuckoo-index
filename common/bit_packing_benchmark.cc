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
// File: bit_packing_benchmark.cc
// -----------------------------------------------------------------------------
//
// Benchmarks for our bit-packing methods.
//
// To run the benchmarks call (turn off dynamic linking):
// bazel run -c opt --dynamic_mode=off common:bit_packing_benchmark
//
// Run on (12 X 4500 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x6)
//   L1 Instruction 32 KiB (x6)
//   L2 Unified 1024 KiB (x6)
//   L3 Unified 8448 KiB (x1)
// Load Average: 1.44, 2.07, 1.93
// --------------------------------------------------------------------
// Benchmark                          Time             CPU   Iterations
// --------------------------------------------------------------------
// BM_BitPack32_Zeros             0.000 ns        0.000 ns   1000000000
// BM_BitPack32_1Bit               1.05 ns         1.04 ns    655400000
// BM_BitPack32_7Bits              1.04 ns         1.04 ns    672100000
// BM_BitPack32_15Bits             1.04 ns         1.04 ns    673200000
// BM_BitPack32_31Bits             1.05 ns         1.05 ns    665700000
// BM_Read_Zeros                  0.722 ns        0.722 ns    987300000
// BM_Read_1Bit                   0.752 ns        0.752 ns    948200000
// BM_Read_7Bits                  0.748 ns        0.748 ns    945300000
// BM_Read_15Bits                 0.742 ns        0.742 ns    956000000
// BM_Read_31Bits                 0.764 ns        0.764 ns    953600000
// BM_Read_32Bits                 0.723 ns        0.723 ns    960900000
// BM_BatchRead_Zeros             0.250 ns        0.250 ns   1000000000
// BM_BatchRead_1Bit              0.357 ns        0.357 ns   1000000000
// BM_BatchRead_7Bits             0.472 ns        0.472 ns   1000000000
// BM_BatchRead_15Bits            0.513 ns        0.513 ns   1000000000
// BM_BatchRead_31Bits            0.685 ns        0.685 ns    775800000
// BM_BatchRead_32Bits            0.723 ns        0.723 ns   1000000000
// BM_BatchRead_6Bits_64Vals      0.378 ns        0.378 ns   1000000000
// BM_BatchRead_6Bits_31Vals      0.852 ns        0.852 ns    799455187

#include <limits>
#include <vector>

#include "benchmark/benchmark.h"
#include "common/bit_packing.h"

namespace ci {
namespace {

// Helper method used in benchmarks to check the time it takes to create
// a bit-packed array with the given number of entries of the given value.
void CheckStoreBitPacked32(benchmark::State& state, int size, uint32_t value) {
  const std::vector<uint32_t> vec(size, value);
  const int bw = BitWidth(value);
  ByteBuffer buffer;

  while (state.KeepRunningBatch(size)) {
    buffer.set_pos(0);
    StoreBitPacked<uint32_t>(vec, bw, &buffer);
  }
  BitPackedReader<uint32_t> reader(bw, buffer.data());
  assert(value == reader.Get(0));
}

constexpr int kArraySize = 100 * 1000;

static void BM_BitPack32_Zeros(benchmark::State& state) {
  CheckStoreBitPacked32(state, kArraySize, 0);
}
BENCHMARK(BM_BitPack32_Zeros);

static void BM_BitPack32_1Bit(benchmark::State& state) {
  CheckStoreBitPacked32(state, kArraySize, 1);
}
BENCHMARK(BM_BitPack32_1Bit);

static void BM_BitPack32_7Bits(benchmark::State& state) {
  CheckStoreBitPacked32(state, kArraySize, 127);
}
BENCHMARK(BM_BitPack32_7Bits);

static void BM_BitPack32_15Bits(benchmark::State& state) {
  CheckStoreBitPacked32(state, kArraySize, (1UL << 15) - 1);
}
BENCHMARK(BM_BitPack32_15Bits);

static void BM_BitPack32_31Bits(benchmark::State& state) {
  CheckStoreBitPacked32(state, kArraySize, (1UL << 31) - 1);
}
BENCHMARK(BM_BitPack32_31Bits);

// Helper method used in benchmarks to check the time it takes to read
// a bit-packed array with the given number of entries of the given value.
void ReadBitPacked32(benchmark::State& state, int size, uint32_t value) {
  const std::vector<uint32_t> vec(size, value);
  const int bw = BitWidth(value);
  ByteBuffer buffer;
  StoreBitPacked<uint32_t>(vec, bw, &buffer);

  while (state.KeepRunningBatch(size)) {
    BitPackedReader<uint32_t> reader(bw, buffer.data());
    for (int i = 0; i < size; ++i) benchmark::DoNotOptimize(reader.Get(i));
  }
}

static void BM_Read_Zeros(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, 0);
}
BENCHMARK(BM_Read_Zeros);

static void BM_Read_1Bit(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, 1);
}
BENCHMARK(BM_Read_1Bit);

static void BM_Read_7Bits(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, 127);
}
BENCHMARK(BM_Read_7Bits);

static void BM_Read_15Bits(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, (1UL << 15) - 1);
}
BENCHMARK(BM_Read_15Bits);

static void BM_Read_31Bits(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, (1UL << 31) - 1);
}
BENCHMARK(BM_Read_31Bits);

static void BM_Read_32Bits(benchmark::State& state) {
  ReadBitPacked32(state, kArraySize, std::numeric_limits<uint32_t>::max());
}
BENCHMARK(BM_Read_32Bits);

// Helper method used in benchmarks to check the time it takes to read
// a bit-packed array with the given number of entries of the given value.
void BatchReadBitPacked32(benchmark::State& state, int size, uint32_t value) {
  const std::vector<uint32_t> vec(size, value);
  const int bw = BitWidth(value);
  ByteBuffer buffer;
  StoreBitPacked<uint32_t>(vec, bw, &buffer);

  std::vector<uint32_t> batch(size);
  while (state.KeepRunningBatch(size)) {
    BitPackedReader<uint32_t> reader(bw, buffer.data());
    reader.GetBatch(size, [&](size_t i, uint32_t value) { batch[i] = value; });
  }
  assert(batch[0] == value);
}

static void BM_BatchRead_Zeros(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, 0);
}
BENCHMARK(BM_BatchRead_Zeros);

static void BM_BatchRead_1Bit(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, 1);
}
BENCHMARK(BM_BatchRead_1Bit);

static void BM_BatchRead_7Bits(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, 127);
}
BENCHMARK(BM_BatchRead_7Bits);

static void BM_BatchRead_15Bits(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, (1UL << 15) - 1);
}
BENCHMARK(BM_BatchRead_15Bits);

static void BM_BatchRead_31Bits(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, (1UL << 31) - 1);
}
BENCHMARK(BM_BatchRead_31Bits);

static void BM_BatchRead_32Bits(benchmark::State& state) {
  BatchReadBitPacked32(state, kArraySize, std::numeric_limits<uint32_t>::max());
}
BENCHMARK(BM_BatchRead_32Bits);

static void BM_BatchRead_6Bits_64Vals(benchmark::State& state) {
  BatchReadBitPacked32(state, 64, 63);
}
BENCHMARK(BM_BatchRead_6Bits_64Vals);

static void BM_BatchRead_6Bits_31Vals(benchmark::State& state) {
  BatchReadBitPacked32(state, 31, 63);
}
BENCHMARK(BM_BatchRead_6Bits_31Vals);

}  // namespace
}  // namespace ci
