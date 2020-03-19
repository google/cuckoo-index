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
// File: evaluation_utils_test.cc
// -----------------------------------------------------------------------------

#include "evaluation_utils.h"

#include <string>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ci {

using ::testing::EndsWith;

TEST(EvaluationUtilsTest, CompressAndUncompress) {
  const std::string orig = "Alain Delon and Jean Paul Belmondo";
  const std::string compressed = Compress(orig);
  const std::string uncompressed = Uncompress(compressed);

  EXPECT_EQ(uncompressed, orig);
}

TEST(EvaluationUtilsTest, GetBitmapDensity) {
  const Bitmap64 none = CreateBitmap({0, 0, 0, 0});
  EXPECT_EQ(GetBitmapDensity(none), 0.0);

  const Bitmap64 quarter = CreateBitmap({0, 0, 0, 1});
  EXPECT_EQ(GetBitmapDensity(quarter), 0.25);

  const Bitmap64 half = CreateBitmap({0, 0, 1, 1});
  EXPECT_EQ(GetBitmapDensity(half), 0.5);

  const Bitmap64 all = CreateBitmap({1, 1, 1, 1});
  EXPECT_EQ(GetBitmapDensity(all), 1.0);
}

TEST(EvaluationUtilsTest, GetBitmapClustering) {
  const Bitmap64 none = CreateBitmap({0, 0, 0, 0});
  EXPECT_EQ(GetBitmapClustering(none), 0.0);

  const Bitmap64 all = CreateBitmap({1, 1, 1, 1});
  EXPECT_EQ(GetBitmapClustering(all), 4.0);

  const Bitmap64 one_fill_1 = CreateBitmap({0, 0, 0, 1});
  EXPECT_EQ(GetBitmapClustering(one_fill_1), 1.0);

  const Bitmap64 one_fill_2 = CreateBitmap({0, 0, 1, 1});
  EXPECT_EQ(GetBitmapClustering(one_fill_2), 2.0);

  const Bitmap64 two_fills_1 = CreateBitmap({1, 0, 1, 0});
  EXPECT_EQ(GetBitmapClustering(two_fills_1), 1.0);

  const Bitmap64 two_fills_1_5 = CreateBitmap({1, 0, 1, 1});
  EXPECT_EQ(GetBitmapClustering(two_fills_1_5), 1.5);
}

TEST(EvaluationUtilsTest, ToRoaring) {
  const Bitmap64 bitmap = CreateBitmap({0, 1, 0, 1});
  const Roaring roaring = ToRoaring(bitmap);

  EXPECT_EQ(bitmap.GetOnesCount(), roaring.cardinality());
  for (size_t i = 0; i < bitmap.bits(); ++i) {
    EXPECT_EQ(bitmap.Get(i), roaring.contains(i));
  }
}

TEST(EvaluationUtilsTest, GetGlobalBitmap) {
  std::vector<Bitmap64Ptr> bitmaps;
  bitmaps.push_back(nullptr);  // Bitmap64Ptrs are allowed to be nullptr.
  bitmaps.push_back(absl::make_unique<Bitmap64>(CreateBitmap({0, 1})));
  bitmaps.push_back(absl::make_unique<Bitmap64>(CreateBitmap({1, 0})));

  const size_t num_stripes = 2;
  const Bitmap64 global_bitmap = GetGlobalBitmap(bitmaps);
  ASSERT_EQ(global_bitmap.bits(), GetNumBitmaps(bitmaps) * num_stripes);
  size_t curr_bitmap_rank = 0;
  for (size_t i = 0; i < bitmaps.size(); ++i) {
    if (bitmaps[i] != nullptr) {
      for (size_t bit_idx = 0; bit_idx < bitmaps[i]->bits(); ++bit_idx) {
        const size_t global_idx = (curr_bitmap_rank * num_stripes) + bit_idx;
        EXPECT_EQ(global_bitmap.Get(global_idx), bitmaps[i]->Get(bit_idx));
      }
      ++curr_bitmap_rank;
    }
  }
}

TEST(EvaluationUtilsTest, BitmapToAndFromFile) {
  const std::string path = "/tmp/bitmap";
  const Bitmap64 bitmap = CreateBitmap({0, 1});
  WriteBitmapToFile(path, bitmap);
  const Bitmap64 decoded = ReadBitmapFromFile(path);
  EXPECT_THAT(decoded.ToString(), EndsWith(bitmap.ToString()));
}

}  // namespace ci
