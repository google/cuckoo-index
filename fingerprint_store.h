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
// File: fingerprint_store.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_FINGERPRINT_STORE_H_
#define CUCKOO_INDEX_FINGERPRINT_STORE_H_

#include <cassert>
#include <climits>
#include <cstdlib>
#include <memory>

#include "absl/container/flat_hash_map.h"
#include "cuckoo_utils.h"
#include "evaluation_utils.h"

namespace ci {

// Stores fingerprints with a fixed number of bits (`num_bits`). The maximum bit
// width of `fingerprints` has to be at most `num_bits` bits.
class Block {
 public:
  explicit Block(const size_t num_bits,
                 const std::vector<uint64_t>& fingerprints);

  // Forbid copying and moving.
  Block(const Block&) = delete;
  Block& operator=(const Block&) = delete;
  Block(Block&&) = delete;
  Block& operator=(Block&&) = delete;

  size_t num_bits() const { return num_bits_; }

  // Returns the fingerprint bits stored at `idx`.
  uint64_t Get(const size_t idx) const {
    assert(idx < num_fingerprints_);
    return fingerprints_.Get(idx);
  }

  const std::string& GetData() const { return data_; }

 private:
  // The number of bits of fingerprints stored in this block.
  const size_t num_bits_;
  const size_t num_fingerprints_;

  std::string data_;
  BitPackedReader<uint64_t> fingerprints_;
};

// Stores variable-sized fingerprints in different blocks, each block storing
// fingerprints of a fixed length. For each block, we maintain a bitmap
// indicating which buckets are stored in this block. The individual blocks
// allow for random access (i.e., they do not need to be decompressed to
// reconstruct individual fingerprints).
//
// As an optimization, we compact consecutive block bitmaps such that a bitmap
// only contains the zero-bits of its predecessor. In other words, a bitmap only
// has a bit for each remaining bucket (i.e., all buckets that are NOT stored in
// previous blocks).
//
// To increase the effect of this optimization, we order the individual blocks
// based on decreasing cardinality (i.e., number of buckets stored in a block).
//
// Example encoding for the fingerprints {1, 101, 01, 0, 001} with one slot per
// bucket:
//
// Block 0: 101001       -- bitpacked fingerprints 101 and 001
// Block 1: 10           -- bitpacked fingerprints 1 and 0
// Block 2: 01           -- bitpacked fingerprint
//
// Block bitmap 0: 01001 -- fingerprints no. 1 and 4 are stored in this block
// Block bitmap 1: 101   -- of the 3 remaining fingerprints no. 0 and 2 are here
// Block bitmap 2: 1     -- only one remaining fingerprint
//
// Specifically, we encode all block bitmaps back-to-back as a single RLE
// bitmap.
//
// Individual blocks are stored as follows:
//
// uint32_t num_bits       -- number of bits of fingerprints in this block
// uint32_t bit_width      -- actual bit width of fingerprints in this block
// (could theoretically be lower)
// .. bitpacked fingerprints ..
// 8 'slop' bytes        -- to be able to read bit-packed 64-bit values, we need
// to ensure that a whole uword_t can be read from the position of the last
// encoded diff, hence we need to be able to read at most 7 bytes past it
class FingerprintStore {
  using BlockPtr = std::unique_ptr<Block>;

  // A helper struct used during block creation.
  struct BlockContent {
    Bitmap64Ptr block_bitmap;
    std::vector<uint64_t> fingerprints;
  };

 public:
  // Decodes FingerprintStore from bytes.
  static void Decode(const std::string& data);

  // The fingerprints passed here have a 1:1 correspondence to the slots in the
  // Cuckoo table. Individual fingerprints can be `inactive`, which means that
  // the corresponding slot is empty (i.e., doesn't contain a fingerprint).
  explicit FingerprintStore(const std::vector<Fingerprint>& fingerprints,
                            const size_t slots_per_bucket,
                            const bool use_rle_to_encode_block_bitmaps);

  // Returns fingerprint stored in slot `slot_idx`.
  Fingerprint GetFingerprint(const size_t slot_idx) const;

  // Encodes FingerprintStore as bytes. For `bitmaps_only` = true, only the
  // bitmaps will be encoded. This is only used for printing stats.
  std::string Encode(bool bitmaps_only = false) const;

  size_t num_slots() const { return num_slots_; }

  // Returns the bitmap indicating empty slots;
  const Bitmap64& EmptySlotsBitmap() const { return *empty_slots_bitmap_; }

  size_t GetSizeInBytes(bool bitmaps_only) const {
    return Encode(bitmaps_only).size();
  }

  size_t GetZstdCompressedSizeInBytes(bool bitmaps_only) const {
    return Compress(Encode(bitmaps_only)).size();
  }

  double GetBitsPerFingerprint(bool bitmaps_only) const {
    return static_cast<double>(GetSizeInBytes(bitmaps_only) * CHAR_BIT) /
           num_stored_fingerprints_;
  }

  double GetBitsPerFingerprintZstdCompressed(bool bitmaps_only) const {
    return static_cast<double>(GetZstdCompressedSizeInBytes(bitmaps_only) *
                               CHAR_BIT) /
           num_stored_fingerprints_;
  }

  size_t GetNumBlocks() const { return blocks_.size(); }

  void PrintStats() const;

 private:
  // Returns the bucket index that the bit `bit_idx` in block bitmap `block_idx`
  // corresponds to.
  size_t GetBucketIndex(const size_t block_idx, const size_t bit_idx) const;

  // Returns the number of non-empty slots in bucket `bucket_idx`.
  size_t GetNumItemsInBucket(const size_t bucket_idx) const;

  // Returns the index of fingerprint `slot_idx` in block `block_idx` (the
  // offset to the fingerprint bits in the bitpacked storage).
  // `idx_in_compacted_bitmap` is the index of the fingerprint in the compacted
  // bitmap `block_idx`.
  size_t GetIndexOfFingerprintInBlock(const size_t block_idx,
                                      const size_t idx_in_compacted_bitmap,
                                      const size_t slot_idx) const;

  // Maps `bucket_idx` to its corresponding index (bit) in the block bitmap
  // `block_bitmap_idx`.
  size_t MapBucketIndexToBitInBlockBitmap(const size_t bucket_idx,
                                          const size_t block_bitmap_idx) const;

  // Creates and compacts block bitmaps in `lengths` order. The idea is to
  // "leave out" bits in subsequent block bitmaps, specifically those that are
  // set in the previous (already compacted) block bitmap.
  void CreateAndCompactBlockBitmaps(
      const std::vector<size_t>& lengths,
      absl::flat_hash_map<size_t, BlockContent>* blocks);

  // A bitmap indicating empty slots.
  Bitmap64Ptr empty_slots_bitmap_;

  // Bitmaps indicating which slot is stored in which block. A subsequent bitmap
  // has `prev.GetOnesCount()` fewer bits than its predecessor.
  // TODO: Replace with a single RLE bitmap.
  std::vector<Bitmap64Ptr> block_bitmaps_;
  std::vector<BlockPtr> blocks_;

  const size_t num_slots_;
  size_t num_stored_fingerprints_;

  size_t slots_per_bucket_;
  bool use_rle_to_encode_block_bitmaps_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_FINGERPRINT_STORE_H_
