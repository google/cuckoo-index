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
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

#include "absl/strings/string_view.h"
#include "boost/dynamic_bitset.hpp"

namespace ci {

// Note: Rank implementation is adapted from SuRF:
// https://github.com/efficient/SuRF/blob/master/include/rank.hpp
// Like SuRF, we precompute the ranks of bit-blocks of size `kRankBlockSize`
// (512 by default), which adds around 6% of size overhead.

// Number of bits in a rank block.
static constexpr size_t kRankBlockSize = 512;

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

  // Calls copy constructor of underlying boost dynamic bitset.
  Bitmap64(const Bitmap64& other) : bitset_(other.bitset_) {}

  size_t bits() const { return bitset_.size(); }

  bool Get(size_t pos) const { return bitset_[pos]; }

  // Initializes `rank_lookup_table_`. Precomputes the ranks of bit-blocks of
  // size `kRankBlockSize`.
  void InitRankLookupTable() {
    // Do not build lookup table if there is only a single block.
    if (bits() <= kRankBlockSize) return;

    const size_t num_rank_blocks = bits() / kRankBlockSize + 1;
    rank_lookup_table_.resize(num_rank_blocks);
    size_t cumulative_rank = 0;
    for (size_t i = 0; i < num_rank_blocks - 1; ++i) {
      rank_lookup_table_[i] = cumulative_rank;
      // Add number of set bits of current block to `cumulative_rank`.
      cumulative_rank +=
          GetOnesCountInRankBlock(/*rank_block_id=*/i, /*limit_within_block*/
                                                    kRankBlockSize);
    }
    rank_lookup_table_[num_rank_blocks - 1] = cumulative_rank;
  }

  // Returns rank of `limit`, i.e., the number of set bits in [0, limit).
  size_t GetOnesCountBeforeLimit(size_t limit) const {
    assert(limit <= bits());

    if (limit == 0) return 0;

    if (rank_lookup_table_.empty()) {
      // No precomputed ranks. Compute rank manually.
      size_t ones_count = 0;
      for (size_t i = 0; i < limit; ++i) ones_count += bitset_[i];
      return ones_count;
    }

    // Get rank from `rank_lookup_table_` and add rank of last rank block.
    const size_t last_pos = limit - 1;
    const size_t rank_block_id = last_pos / kRankBlockSize;
    const size_t limit_within_block = (last_pos & (kRankBlockSize - 1)) + 1;
    return rank_lookup_table_[rank_block_id]
        + GetOnesCountInRankBlock(rank_block_id, limit_within_block);
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
  // Returns the number of set bits in rank block `rank_block_id` before
  // `limit_within_block`.
  size_t GetOnesCountInRankBlock(const size_t rank_block_id,
                                 const size_t limit_within_block) const {
    const size_t start = rank_block_id * kRankBlockSize;
    const size_t end = start + limit_within_block;
    assert(end <= bits());
    size_t ones_count = 0;
    for (size_t i = start; i < end; ++i) {
      ones_count += bitset_[i];
    }
    return ones_count;
  }

  boost::dynamic_bitset<> bitset_;
  // Stores precomputed ranks of bit-blocks of size `kRankBlockSize`.
  std::vector<uint32_t> rank_lookup_table_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_COMMON_BITMAP_H_
