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
// File: rle_bitmap.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_COMMON_RLE_BITMAP_H_
#define CUCKOO_INDEX_COMMON_RLE_BITMAP_H_

#include <cstdint>
#include <memory>
#include <string>

#include "absl/strings/string_view.h"
#include "common/bit_packing.h"
#include "common/bitmap.h"

namespace ci {

class RleBitmap;
using RleBitmapPtr = std::unique_ptr<RleBitmap>;

class RleBitmap {
 public:
  // By default create skip-offsets stepping over 1024 run-lengths.
  explicit RleBitmap(const Bitmap64& bitmap, uint32_t skip_offsets_step = 1024);

  // Forbid copying and moving.
  RleBitmap(const RleBitmap&) = delete;
  RleBitmap& operator=(const RleBitmap&) = delete;
  RleBitmap(RleBitmap&&) = delete;
  RleBitmap& operator=(RleBitmap&&) = delete;

  absl::string_view data() const { return data_; }

  // Returns the slice of the bitmap from `offset` on of the given `size`.
  Bitmap64 Extract(size_t offset, size_t size) const;

  bool Get(size_t pos) const { return Extract(pos, 1).Get(0); }

 private:
  // Extract(..) implementations for the dense and the sparse encoding.
  Bitmap64 ExtractDense(size_t offset, size_t size) const;
  Bitmap64 ExtractSparse(size_t offset, size_t size) const;

  bool is_sparse_;
  size_t size_;
  uint32_t skip_offsets_step_;
  size_t skip_offsets_size_;
  size_t run_lengths_size_;
  size_t bits_size_;
  std::string data_;

  BitPackedReader<uint32_t> skip_offsets_;
  BitPackedReader<uint32_t> run_lengths_;
  BitPackedReader<uint32_t> bits_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_COMMON_RLE_BITMAP_H_
