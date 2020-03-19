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
// File: bitmap.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_COMMON_BITMAP_H_
#define CUCKOO_INDEX_COMMON_BITMAP_H_

#include <cassert>
#include <iostream>
#include <memory>
#include <vector>

#include "boost/dynamic_bitset.hpp"

namespace ci {

class Bitmap64;
using Bitmap64Ptr = std::unique_ptr<Bitmap64>;

class Bitmap64 {
 public:
  static Bitmap64 GetGlobalBitmap(const std::vector<Bitmap64Ptr>& bitmaps) {
    size_t num_bits = 0;
    for (const Bitmap64Ptr& bitmap : bitmaps) {
      if (bitmap != nullptr) num_bits += bitmap->bits();
    }

    Bitmap64 global_bitmap(num_bits);
    size_t base_index = 0;
    for (const Bitmap64Ptr& bitmap : bitmaps) {
      if (bitmap == nullptr) continue;
      for (const size_t index : bitmap->TrueBitIndices()) {
        global_bitmap.Set(base_index + index, true);
      }
      base_index += bitmap->bits();
    }
    return global_bitmap;
  }

  static void DenseEncode(const Bitmap64& bitmap, std::string* out) {
    using Block = boost::dynamic_bitset<>::block_type;
    size_t size_in_bytes = bitmap.bitset_.num_blocks() * sizeof(Block);
    if (out->size() < size_in_bytes) out->resize(size_in_bytes);
    boost::to_block_range(bitmap.bitset_,
                          reinterpret_cast<Block*>(out->data()));
  }

  static Bitmap64 DenseDecode(absl::string_view encoded) {
    using Block = boost::dynamic_bitset<>::block_type;
    const size_t num_blocks = encoded.size() / sizeof(Block);
    const Block* begin = reinterpret_cast<const Block*>(encoded.data());
    Bitmap64 decoded(num_blocks * boost::dynamic_bitset<>::bits_per_block);
    boost::from_block_range(begin, begin + num_blocks, decoded.bitset_);

    return decoded;
  }

  Bitmap64() = default;

  explicit Bitmap64(size_t num_bits) : bitset_(num_bits) {}

  Bitmap64(size_t num_bits, bool fill_value) : Bitmap64(num_bits) {
    for (size_t i = 0; i < bits(); ++i) bitset_[i] = fill_value;
  }

  size_t bits() const { return bitset_.size(); }

  bool Get(size_t pos) const { return bitset_[pos]; }

  size_t GetOnesCountBeforeLimit(size_t limit) const {
    assert(limit <= bits());
    size_t ones_count = 0;
    for (size_t i = 0; i < limit; ++i) ones_count += bitset_[i];
    return ones_count;
  }

  size_t GetOnesCount() const { return GetOnesCountBeforeLimit(bits()); }

  size_t GetZeroesCount() const { return bits() - GetOnesCount(); }

  bool IsAllZeroes() const { return bitset_.none(); }

  void Set(size_t pos, bool value) { bitset_[pos] = value; }

  std::vector<size_t> TrueBitIndices() const {
    std::vector<size_t> indices;

    for (size_t i = 0; i < bits(); ++i) {
      if (bitset_[i]) indices.push_back(i);
    }

    return indices;
  }

  std::string ToString() const {
    std::string result;
    boost::to_string(bitset_, result);
    return result;
  }

 private:
  boost::dynamic_bitset<> bitset_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_COMMON_BITMAP_H_
