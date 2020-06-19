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
// File: cuckoo_utils_test.cc
// -----------------------------------------------------------------------------

#include "cuckoo_utils.h"

#include <string>

#include "evaluation_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ci {

TEST(CuckooUtilsTest, GetMinNumBuckets2SlotsPerBucket) {
  // kMaxLoadFactor2SlotsPerBucket: 0.84
  // (1 / 0.84) * 10 values / 2 slots per bucket = 5.95
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/10, /*slots_per_bucket=*/2), 6);
}

TEST(CuckooUtilsTest, GetMinNumBuckets4SlotsPerBucket) {
  // kMaxLoadFactor4SlotsPerBucket: 0.95
  // (1 / 0.95) * 10 values / 4 slots per bucket = 2.63
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/10, /*slots_per_bucket=*/4), 3);
}

TEST(CuckooUtilsTest, GetMinNumBuckets8SlotsPerBucket) {
  // kMaxLoadFactor8SlotsPerBucket: 0.98
  // (1 / 0.98) * 10 values / 2 slots per bucket = 1.28
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/10, /*slots_per_bucket=*/8), 2);
}

TEST(CuckooUtilsTest, GetMinNumBucketsCustomLoadFactor) {
  // (1 / 0.5) * 10 values / 1 slots per bucket = 20
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/10, /*slots_per_bucket=*/1,
                             /*max_load_factor=*/0.5),
            20);
}

TEST(CuckooUtilsTest, GetMinNumBucketsAtLeastOneBucket) {
  // (1 / 0.99) * 1 values / 8 slots per bucket = 0.13
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/1, /*slots_per_bucket=*/8,
                             /*max_load_factor=*/0.99),
            1);
}

TEST(CuckooUtilsTest, GetMinNumBucketsZeroValues) {
  // (1 / 0.5) * 0 values / 1 slots per bucket = 0
  ASSERT_EQ(GetMinNumBuckets(/*num_values=*/0, /*slots_per_bucket=*/1,
                             /*max_load_factor=*/0.5),
            0);
}

TEST(CuckooUtilsTest, GetFingerprintPrefix) {
  EXPECT_EQ(GetFingerprintPrefix(0b1111ULL << 60, /*num_bits=*/0), 0b0);
  EXPECT_EQ(GetFingerprintPrefix(0b1111ULL << 60, /*num_bits=*/1), 0b1);
  EXPECT_EQ(GetFingerprintPrefix(0b1111ULL << 60, /*num_bits=*/2), 0b11);
  EXPECT_EQ(GetFingerprintPrefix(0b1111ULL << 60, /*num_bits=*/3), 0b111);

  EXPECT_EQ(GetFingerprintPrefix(0b1011ULL << 60, /*num_bits=*/0), 0b0);
  EXPECT_EQ(GetFingerprintPrefix(0b1011ULL << 60, /*num_bits=*/1), 0b1);
  EXPECT_EQ(GetFingerprintPrefix(0b1011ULL << 60, /*num_bits=*/2), 0b10);
  EXPECT_EQ(GetFingerprintPrefix(0b1011ULL << 60, /*num_bits=*/3), 0b101);
}

TEST(CuckooUtilsTest, GetMinCollisionFreeFingerprintLength) {
  const std::vector<uint64_t> fingerprints = {0b1, 0b11, 0b111};

  EXPECT_EQ(GetMinCollisionFreeFingerprintLength(fingerprints,
                                                 /*use_prefix_bits=*/false),
            3);
  EXPECT_EQ(GetMinCollisionFreeFingerprintLength(fingerprints,
                                                 /*use_prefix_bits=*/true),
            63);

  // Check whether convenience method finds the same result.
  bool use_prefix_bits;
  EXPECT_EQ(GetMinCollisionFreeFingerprintPrefixOrSuffix(fingerprints,
                                                         &use_prefix_bits),
            3);
  EXPECT_EQ(use_prefix_bits, false);
}

TEST(CuckooUtilsTest, CheckWhetherAllBucketsOnlyContainSameSizeFingerprints) {
  const std::vector<Fingerprint> fingerprints = {
      {/*active=*/false, /*num_bits=*/1, /*fingerprint=*/0b0},
      {/*active=*/true, /*num_bits=*/1, /*fingerprint=*/0b0},
      {/*active=*/true, /*num_bits=*/2, /*fingerprint=*/0b0},
      {/*active=*/true, /*num_bits=*/2, /*fingerprint=*/0b0}};

  EXPECT_TRUE(CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
      fingerprints, /*slots_per_bucket=*/1));
  EXPECT_TRUE(CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
      fingerprints, /*slots_per_bucket=*/2));
  EXPECT_FALSE(CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
      fingerprints, /*slots_per_bucket=*/4));
}

TEST(BucketTest, BucketInsertValue) {
  Bucket bucket(/*num_slots=*/1);
  // Insert should succeed, since `bucket` has capacity for another slot.
  EXPECT_TRUE(bucket.InsertValue({/*value=*/42, /*num_buckets=*/1}));
  // Insert should fail, since `bucket` is full.
  EXPECT_FALSE(bucket.InsertValue({/*value=*/17, /*num_buckets=*/1}));
}

TEST(BucketTest, BucketContainsValue) {
  CuckooValue value(/*value=*/42, /*num_buckets=*/1);
  Bucket bucket(/*num_slots=*/1);
  EXPECT_FALSE(bucket.ContainsValue(value));
  bucket.InsertValue(value);
  EXPECT_TRUE(bucket.ContainsValue(value));
  EXPECT_FALSE(bucket.ContainsValue({/*value=*/17, /*num_buckets=*/1}));
}

TEST(BitmapRank, GetRankSingleBit) {
  ASSERT_EQ(GetRank(CreateBitmap({0}), /*idx=*/0), 0);
  ASSERT_EQ(GetRank(CreateBitmap({1}), /*idx=*/0), 0);
}

TEST(BitmapRank, GetRankAllZerosBitmap) {
  const Bitmap64 bitmap = CreateBitmap({0, 0, 0});
  ASSERT_EQ(GetRank(bitmap, /*idx=*/0), 0);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/1), 0);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/2), 0);
}

TEST(BitmapRank, GetRankAllOnesBitmap) {
  const Bitmap64 bitmap = CreateBitmap({1, 1, 1});
  ASSERT_EQ(GetRank(bitmap, /*idx=*/0), 0);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/1), 1);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/2), 2);
}

TEST(BitmapRank, GetRankRandomBitmap) {
  const Bitmap64 bitmap = CreateBitmap({1, 0, 1});
  ASSERT_EQ(GetRank(bitmap, /*idx=*/0), 0);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/1), 1);
  ASSERT_EQ(GetRank(bitmap, /*idx=*/2), 1);
}

TEST(BitmapRank, GetRankWithRankLookupTable) {
  const size_t num_bits = kRankBlockSize * 2 + kRankBlockSize / 10;
  std::vector<int> bits;
  bits.resize(num_bits);
  for (size_t i = 0; i < num_bits; ++i) {
    bits[i] = (i % 2 == 0) ? 1 : 0;
  }
  Bitmap64 bitmap = CreateBitmap(bits);
  Bitmap64 bitmap_with_rank = CreateBitmap(bits);
  bitmap_with_rank.InitRankLookupTable();

  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_EQ(GetRank(bitmap, i), GetRank(bitmap_with_rank, i));
  }
}

TEST(BitmapSelect, SelectSingleBit) {
  size_t pos;

  ASSERT_FALSE(SelectOne(CreateBitmap({0}), /*ith=*/0, &pos));

  ASSERT_TRUE(SelectZero(CreateBitmap({0}), /*ith=*/0, &pos));
  ASSERT_EQ(pos, 0);

  ASSERT_TRUE(SelectOne(CreateBitmap({1}), /*ith=*/0, &pos));
  ASSERT_EQ(pos, 0);

  ASSERT_FALSE(SelectZero(CreateBitmap({1}), /*ith=*/0, &pos));
}

TEST(BitmapSelect, SelectAllZerosBitmap) {
  const Bitmap64 bitmap = CreateBitmap({0, 0, 0});

  size_t pos;

  ASSERT_FALSE(SelectOne(bitmap, /*ith=*/0, &pos));

  ASSERT_TRUE(SelectZero(bitmap, /*ith=*/0, &pos));
  ASSERT_EQ(pos, 0);

  ASSERT_TRUE(SelectZero(bitmap, /*ith=*/1, &pos));
  ASSERT_EQ(pos, 1);

  ASSERT_TRUE(SelectZero(bitmap, /*ith=*/2, &pos));
  ASSERT_EQ(pos, 2);
}

TEST(BitmapSelect, SelectAllOnesBitmap) {
  const Bitmap64 bitmap = CreateBitmap({1, 1, 1});

  size_t pos;

  ASSERT_TRUE(SelectOne(bitmap, /*ith=*/0, &pos));
  ASSERT_EQ(pos, 0);

  ASSERT_TRUE(SelectOne(bitmap, /*ith=*/1, &pos));
  ASSERT_EQ(pos, 1);

  ASSERT_TRUE(SelectOne(bitmap, /*ith=*/2, &pos));
  ASSERT_EQ(pos, 2);

  ASSERT_FALSE(SelectZero(bitmap, /*ith=*/0, &pos));
}

TEST(BitmapSelect, SelectRandomBitmap) {
  const Bitmap64 bitmap = CreateBitmap({1, 0, 1});

  size_t pos;

  ASSERT_TRUE(SelectOne(bitmap, /*ith=*/0, &pos));
  ASSERT_EQ(pos, 0);

  ASSERT_TRUE(SelectOne(bitmap, /*ith=*/1, &pos));
  ASSERT_EQ(pos, 2);

  ASSERT_FALSE(SelectOne(bitmap, /*ith=*/2, &pos));

  ASSERT_TRUE(SelectZero(bitmap, /*ith=*/0, &pos));
  ASSERT_EQ(pos, 1);
}

TEST(EmptyBucketsBitmap, GetEmptyBucketsBitmap) {
  const Bitmap64 empty_slots_bitmap = CreateBitmap({0, 1, 1, 1});

  EXPECT_EQ(GetEmptyBucketsBitmap(empty_slots_bitmap, /*slots_per_bucket=*/1)
                ->ToString(),
            CreateBitmap({0, 1, 1, 1}).ToString());

  EXPECT_EQ(GetEmptyBucketsBitmap(empty_slots_bitmap, /*slots_per_bucket=*/2)
                ->ToString(),
            CreateBitmap({0, 1}).ToString());

  EXPECT_EQ(GetEmptyBucketsBitmap(empty_slots_bitmap, /*slots_per_bucket=*/4)
                ->ToString(),
            CreateBitmap({0}).ToString());
}

TEST(BitmapSerialization, EncodeAndDecodeBitmap) {
  const size_t num_bits = kRankBlockSize * 2 + kRankBlockSize / 10;
  std::vector<int> bits;
  bits.resize(num_bits);
  for (size_t i = 0; i < num_bits; ++i) {
    bits[i] = (i % 2 == 0) ? 1 : 0;
  }
  Bitmap64 bitmap = CreateBitmap(bits);
  bitmap.InitRankLookupTable();

  std::string encoded;
  Bitmap64::DenseEncode(bitmap, &encoded);
  Bitmap64 decoded = Bitmap64::DenseDecode(encoded);

  ASSERT_EQ(bitmap.bits(), decoded.bits());

  for (size_t i = 0; i < num_bits; ++i) {
    ASSERT_EQ(bitmap.Get(i), decoded.Get(i));
    ASSERT_EQ(GetRank(bitmap, i), GetRank(decoded, i));
  }
}

}  // namespace ci
