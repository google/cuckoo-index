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
// File: bit_packing_test.cc
// -----------------------------------------------------------------------------
//
// Tests for our bit-packing methods.

#include "common/bit_packing.h"

#include <limits>
#include <vector>

#include "gtest/gtest.h"

namespace ci {
namespace {

TEST(BitPackingTest, BytesRequired) {
  EXPECT_EQ(0, BitPackingBytesRequired(0));
  EXPECT_EQ(1, BitPackingBytesRequired(1));
  EXPECT_EQ(1, BitPackingBytesRequired(8));
  EXPECT_EQ(2, BitPackingBytesRequired(9));
  EXPECT_EQ(33, BitPackingBytesRequired(257));
  EXPECT_EQ(2305843009213693951,
            BitPackingBytesRequired(std::numeric_limits<uint64_t>::max() - 7));
}

TEST(BitPackingTest, BitWidth) {
  // 32 bit version.
  EXPECT_EQ(0, BitWidth<uint32_t>(0));
  EXPECT_EQ(1, BitWidth<uint32_t>(1));
  EXPECT_EQ(2, BitWidth<uint32_t>(2));
  EXPECT_EQ(2, BitWidth<uint32_t>(3));
  EXPECT_EQ(3, BitWidth<uint32_t>(4));
  EXPECT_EQ(3, BitWidth<uint32_t>(7));
  EXPECT_EQ(8, BitWidth<uint32_t>(255));
  EXPECT_EQ(9, BitWidth<uint32_t>(256));
  EXPECT_EQ(32, BitWidth<uint32_t>(std::numeric_limits<uint32_t>::max()));

  // 64 bit version.
  EXPECT_EQ(0, BitWidth<uint64_t>(0));
  EXPECT_EQ(1, BitWidth<uint64_t>(1));
  EXPECT_EQ(2, BitWidth<uint64_t>(2));
  EXPECT_EQ(2, BitWidth<uint64_t>(3));
  EXPECT_EQ(3, BitWidth<uint64_t>(4));
  EXPECT_EQ(3, BitWidth<uint64_t>(7));
  EXPECT_EQ(8, BitWidth<uint64_t>(255));
  EXPECT_EQ(9, BitWidth<uint64_t>(256));
  EXPECT_EQ(32, BitWidth<uint64_t>(std::numeric_limits<uint32_t>::max()));
  EXPECT_EQ(33,
            BitWidth<uint64_t>(std::numeric_limits<uint32_t>::max() + 1ULL));
  EXPECT_EQ(64, BitWidth<uint64_t>(std::numeric_limits<uint64_t>::max()));
}

TEST(BitPackingTest, MaxBitWidthOnEmptyArray) {
  const std::vector<uint32_t> empty32;
  const std::vector<uint64_t> empty64;

  EXPECT_EQ(0, MaxBitWidth<uint32_t>(empty32));
  EXPECT_EQ(0, MaxBitWidth<uint64_t>(empty64));
}

TEST(BitPackingTest, MaxBitWidthOnZeros) {
  const std::vector<uint32_t> zeros32{0, 0, 0, 0, 0};
  const std::vector<uint64_t> zeros64{0ULL, 0ULL, 0ULL};

  EXPECT_EQ(0, MaxBitWidth<uint32_t>(zeros32));
  EXPECT_EQ(0, MaxBitWidth<uint64_t>(zeros64));
}

TEST(BitPackingTest, MaxBitWidthOnSmallValues) {
  const std::vector<uint32_t> vec32{0, 1, 3, 0, 7};
  const std::vector<uint64_t> vec64{0ULL, 127ULL, 0ULL};

  EXPECT_EQ(3, MaxBitWidth<uint32_t>(vec32));
  EXPECT_EQ(7, MaxBitWidth<uint64_t>(vec64));
}

TEST(BitPackingTest, MaxBitWidthOnMaxValues) {
  const std::vector<uint32_t> vec32{0, 1, 3, 0,
                                    std::numeric_limits<uint32_t>::max()};
  const std::vector<uint64_t> vec64{0ULL, 127ULL,
                                    std::numeric_limits<uint64_t>::max()};

  EXPECT_EQ(32, MaxBitWidth<uint32_t>(vec32));
  EXPECT_EQ(64, MaxBitWidth<uint64_t>(vec64));
}

// Bit-packs the given array and then compares the result with the given array.
// Also checks the size of the bit-packed array.
template <typename T>
void CheckBitPack(absl::Span<const T> array, int bit_packed_size) {
  const int bw = MaxBitWidth(array);
  const size_t num_bytes = BitPackingBytesRequired(bw * array.size());
  EXPECT_EQ(bit_packed_size, num_bytes);
  ByteBuffer buffer;
  StoreBitPacked<T>(array, bw, &buffer);
  EXPECT_EQ(bit_packed_size, buffer.pos());
  PutSlopBytes(&buffer);
  EXPECT_EQ(bit_packed_size + 8, buffer.pos());

  BitPackedReader<T> reader(bw, buffer.data());

  for (size_t i = 0; i < array.size(); ++i) EXPECT_EQ(array[i], reader.Get(i));
}

TEST(BitPackingTest, BitPack32EmptyArray) {
  const std::vector<uint32_t> empty;
  CheckBitPack<uint32_t>(empty, 0);
}

TEST(BitPackingTest, BitPack32Zeros) {
  const std::vector<uint32_t> zeros{0, 0, 0, 0, 0, 0, 0, 0};
  CheckBitPack<uint32_t>(zeros, 0);
}

TEST(BitPackingTest, BitPack32Bits) {
  const std::vector<uint32_t> bits{0, 1, 0, 1, 0, 0, 1, 1};
  CheckBitPack<uint32_t>(bits, 1);
}

TEST(BitPackingTest, BitPack32SmallValues) {
  const std::vector<uint32_t> values{7, 2, 0, 1, 0, 4, 3, 0};
  CheckBitPack<uint32_t>(values, 3);
}

TEST(BitPackingTest, BitPack32LargeValues) {
  const std::vector<uint32_t> values{0, 42,
                                     std::numeric_limits<uint32_t>::max() / 8,
                                     std::numeric_limits<uint32_t>::max() / 4};
  CheckBitPack<uint32_t>(values, 15);
}

TEST(BitPackingTest, BitPack32RangeOfValues) {
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i < (std::numeric_limits<uint32_t>::max() >> 4);
       i = (i + 1) * 2)
    values.push_back(i);
  CheckBitPack<uint32_t>(values, 98);
}

// Test bit-widths 0-32 with array-lengths 0-1024.
TEST(BitPackingTest, BitPack32GetRange) {
  constexpr int kMaxLength = 1024;

  for (int bit_width = 0; bit_width <= 32; ++bit_width) {
    // Fill an array with values that take up to bit_width bits.
    std::vector<uint32_t> src(kMaxLength);
    if (bit_width > 0) {
      for (int i = 0; i < kMaxLength; ++i) src[i] = 1 << (i % bit_width);
    }
    ASSERT_EQ(MaxBitWidth<uint32_t>(src), bit_width);
    ByteBuffer buffer;
    StoreBitPacked<uint32_t>(src, bit_width, &buffer);
    PutSlopBytes(&buffer);
    BitPackedReader<uint32_t> reader(bit_width, buffer.data());

    // Check all array lengths from 0 to kMaxLength.
    for (int length = 0; length < kMaxLength; ++length) {
      std::vector<uint32_t> result(length);
      reader.GetBatch(length,
                      [&](size_t i, uint32_t value) { result[i] = value; });
      ASSERT_EQ(result, absl::Span<uint32_t>(src.data(), length))
          << "bit_width: " << bit_width << ", length: " << length;
      // Also double-check with Get(..).
      for (int i = 0; i < length; i++) ASSERT_EQ(reader.Get(i), src[i]);
    }
  }
}

TEST(BitPackingTest, BitPack64EmptyArray) {
  const std::vector<uint64_t> empty;
  CheckBitPack<uint64_t>(empty, 0);
}

TEST(BitPackingTest, BitPack64Zeros) {
  const std::vector<uint64_t> zeros{0, 0, 0, 0, 0, 0, 0, 0};
  CheckBitPack<uint64_t>(zeros, 0);
}

TEST(BitPackingTest, BitPack64Bits) {
  const std::vector<uint64_t> bits{0, 1, 0, 1, 0, 0, 1, 1};
  CheckBitPack<uint64_t>(bits, 1);
}

TEST(BitPackingTest, BitPack64SmallValues) {
  const std::vector<uint64_t> values{7, 2, 0, 1, 0, 4, 3, 0};
  CheckBitPack<uint64_t>(values, 3);
}

TEST(BitPackingTest, BitPack64LargeValues) {
  const std::vector<uint64_t> values{0, 42,
                                     std::numeric_limits<uint64_t>::max() / 8,
                                     std::numeric_limits<uint64_t>::max() / 4};
  CheckBitPack<uint64_t>(values, 31);
}

TEST(BitPackingTest, BitPack64MaxValue) {
  const std::vector<uint64_t> values{0, std::numeric_limits<uint64_t>::max()};
  CheckBitPack<uint64_t>(values, 16);
}

TEST(BitPackingTest, BitPack64RangeOfValues) {
  std::vector<uint64_t> values;
  for (uint64_t i = 0; i < (std::numeric_limits<uint64_t>::max() >> 4);
       i = (i + 1) * 2)
    values.push_back(i);
  CheckBitPack<uint64_t>(values, 450);
}

TEST(BitPackingTest, BitPack64ForPowersOf2) {
  for (int shift = 0; shift < 64; ++shift) {
    std::vector<uint64_t> values;
    const uint64_t val = 1ULL << shift;
    // Add 8 values, since at the latest after adding 8 values of (shift + 1)
    // bits length, the "start-bit" repeats.
    for (int i = 0; i < 8; ++i) values.push_back(val);

    CheckBitPack<uint64_t>(
        values, static_cast<int>(BitPackingBytesRequired(8 * (shift + 1))));
  }
}

TEST(BitPackingTest, EmptyBitPackedReaderDebugString) {
  constexpr int bit_width = 0;
  ByteBuffer buffer;
  StoreBitPacked<uint32_t>({}, bit_width, &buffer);
  BitPackedReader<uint32_t> reader(bit_width, buffer.data());
  EXPECT_EQ(reader.DebugString(/*size=*/0), "size: 0, bit-width: 0, bytes: 0");
}

TEST(BitPackingTest, NonEmptyBitPackedReaderDebugString) {
  constexpr int bit_width = 8;
  ByteBuffer buffer;
  StoreBitPacked<uint32_t>({0, 1, 2, 255}, bit_width, &buffer);
  BitPackedReader<uint32_t> reader(bit_width, buffer.data());
  EXPECT_EQ(reader.DebugString(/*size=*/4), "size: 4, bit-width: 8, bytes: 4");
}

}  // namespace
}  // namespace ci
