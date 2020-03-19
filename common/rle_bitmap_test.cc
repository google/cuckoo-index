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
// File: rle_bitmap_test.cc
// -----------------------------------------------------------------------------

#include "common/rle_bitmap.h"

#include "common/bitmap.h"
#include "gtest/gtest.h"

namespace ci {

void CheckBitmap(const Bitmap64& bitmap) {
  // Set a smaller skip-offsets-step to ensure there are some skip-offsets.
  const RleBitmap rle_bitmap(bitmap, /*skip_offsets_step=*/10);

  // For a host of slices, check that Extract(..) fetches the expected bitmap.
  for (size_t offset = 0; offset < bitmap.bits(); ++offset) {
    for (size_t size = 0; size < bitmap.bits() - offset; size = size * 2 + 1) {
      const Bitmap64 extracted = rle_bitmap.Extract(offset, size);
      for (size_t i = 0; i < size; ++i)
        ASSERT_EQ(extracted.Get(i), bitmap.Get(i + offset));
    }
  }
}

TEST(RleBitmapTest, EmptyBitmap) { CheckBitmap(Bitmap64()); }

TEST(RleBitmapTest, SingleValueBitmaps) {
  CheckBitmap(Bitmap64(1, false));
  CheckBitmap(Bitmap64(1, true));

  CheckBitmap(Bitmap64(2, false));
  CheckBitmap(Bitmap64(2, true));

  CheckBitmap(Bitmap64(100, false));
  CheckBitmap(Bitmap64(100, true));

  CheckBitmap(Bitmap64(2000, false));
  CheckBitmap(Bitmap64(2000, true));
}

TEST(RleBitmapTest, SparseBitmaps) {
  Bitmap64 bitmap(4000);

  bitmap.Set(2018, true);
  CheckBitmap(bitmap);
  bitmap.Set(2019, true);
  CheckBitmap(bitmap);
  bitmap.Set(3025, true);
  CheckBitmap(bitmap);
  bitmap.Set(3999, true);
  CheckBitmap(bitmap);
}

TEST(RleBitmapTest, InterleavedBitmap) {
  Bitmap64 bitmap(4000);
  size_t step = 0;
  bool bit = true;
  for (size_t i = 0; i < bitmap.bits(); i += step) {
    ++step;
    for (size_t j = 0; (j < step) && (i + j < bitmap.bits()); ++j)
      bitmap.Set(i + j, bit);
    bit ^= true;
  }
  CheckBitmap(bitmap);
}

}  // namespace ci
