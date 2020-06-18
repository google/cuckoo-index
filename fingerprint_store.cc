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
// File: fingerprint_store.cc
// -----------------------------------------------------------------------------

#include "fingerprint_store.h"

#include <iostream>

#include "absl/strings/str_cat.h"
#include "common/bitmap.h"
#include "common/rle_bitmap.h"
#include "cuckoo_utils.h"

namespace ci {

constexpr size_t kEmptyBucketsBlockMarker = 999;

Block::Block(const size_t num_bits, const std::vector<uint64_t>& fingerprints)
    : num_bits_(num_bits), num_fingerprints_(fingerprints.size()) {
  // Write to a ByteBuffer.
  ByteBuffer result;

  // Needed to re-construct original fingerprints since the bit width in
  // bitpacked format could be smaller.
  PutVarint32(num_bits, &result);

  // No need to encode `num_fingerprints`. Can be reconstructed from
  // `block_bitmap` in FingerprintStore.

  const uint32_t bit_width =
      MaxBitWidth<uint64_t>(absl::MakeConstSpan(fingerprints));
  if (bit_width > num_bits) {
    std::cerr << "Maximum bit width is " << bit_width
              << ", but expected at most " << num_bits << " bits.";
    std::exit(-1);
  }

  PutVarint32(bit_width, &result);
  const size_t fingerprints_pos = result.pos();
  StoreBitPacked<uint64_t>(fingerprints, bit_width, &result);
  // Note: We could avoid multiple slop bytes by storing all blocks
  // consecutively.
  PutSlopBytes(&result);

  // Copy the serialized encoding to the string `data_`.
  data_ = std::string(result.data(), result.pos());

  // Set BitPackedReader.
  fingerprints_ =
      BitPackedReader<uint64_t>(bit_width, data_.data() + fingerprints_pos);
}

void FingerprintStore::Decode(const std::string& data) {
  // TODO: Make method return FingerprintStore + implement decode.
}

FingerprintStore::FingerprintStore(const std::vector<Fingerprint>& fingerprints,
                                   const size_t slots_per_bucket,
                                   const bool use_rle_to_encode_block_bitmaps)
    : num_slots_(fingerprints.size()),
      slots_per_bucket_(slots_per_bucket),
      use_rle_to_encode_block_bitmaps_(use_rle_to_encode_block_bitmaps) {
  assert((fingerprints.size() % slots_per_bucket_) == 0);
  if (slots_per_bucket_ > 1) {
    // Check that all fingerprints in a bucket share the same length.
    assert(CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
        fingerprints, slots_per_bucket_));
    // << "All fingerprints in a bucket need to share the same length.";
  }

  // Mark empty slots in `empty_slots_bitmap_`.
  empty_slots_bitmap_ =
      absl::make_unique<Bitmap64>(/*size=*/fingerprints.size());
  for (size_t i = 0; i < fingerprints.size(); ++i) {
    if (!fingerprints[i].active) empty_slots_bitmap_->Set(i, true);
  }
  empty_slots_bitmap_->InitRankLookupTable();
  num_stored_fingerprints_ = empty_slots_bitmap_->GetZeroesCount();

  // Build a map from fingerprint length to BlockContent.
  absl::flat_hash_map<size_t, BlockContent> blocks;

  // Create a special block `empty_buckets_block`. The purpose of this "virtual"
  // block is to simplify the lookup logic. An alternative lookup implementation
  // without this block would need to do a "per-empty bucket rank" on the
  // `empty_slots_bitmap_`. Since this auxiliary block can be re-constructed
  // from the `empty_slots_bitmap_` at any time, we only maintain it at runtime
  // and do not serialize it later on.
  BlockContent& empty_buckets_block = blocks[kEmptyBucketsBlockMarker];
  empty_buckets_block.block_bitmap =
      GetEmptyBucketsBitmap(*empty_slots_bitmap_, slots_per_bucket_);

  // Add fingerprints to their corresponding block.
  for (size_t i = 0; i < fingerprints.size(); ++i) {
    const Fingerprint& fp = fingerprints[i];
    if (!fp.active) continue;
    BlockContent& block = blocks[fp.num_bits];
    if (block.block_bitmap == nullptr)
      block.block_bitmap = absl::make_unique<Bitmap64>(
          /*size=*/fingerprints.size() / slots_per_bucket_);
    block.block_bitmap->Set(i / slots_per_bucket_, true);
    block.fingerprints.push_back(
        GetFingerprintSuffix(fp.fingerprint, fp.num_bits));
  }

  // Build a permutation vector `lengths` that orders blocks based on decreasing
  // cardinality to allow for smaller block bitmaps.
  std::vector<size_t> lengths;
  lengths.reserve(blocks.size());
  for (const auto& [num_bits, _] : blocks) lengths.push_back(num_bits);
  std::function<bool(size_t, size_t)> comparator = [&](size_t length,
                                                       size_t other_length) {
    // Keep "empty block" in front to allow for simple re-construction from
    // `empty_slots_bitmap_`. The effect of this is that the empty block bitmap
    // will not be "compacted" in CreateAndCompactBlockBitmaps() below (the
    // first block bitmap is never compacted).
    if (length == kEmptyBucketsBlockMarker) return true;
    if (other_length == kEmptyBucketsBlockMarker) return false;

    // Order other blocks based on decreasing cardinality.
    return blocks[length].block_bitmap->GetOnesCount() >
           blocks[other_length].block_bitmap->GetOnesCount();
  };
  std::sort(lengths.begin(), lengths.end(), comparator);

  // Allocate one block per fingerprint length.
  for (const size_t length : lengths) {
    blocks_.push_back(
        absl::make_unique<Block>(length, blocks[length].fingerprints));
  }

  CreateAndCompactBlockBitmaps(lengths, &blocks);

  PrintStats();
}

Fingerprint FingerprintStore::GetFingerprint(const size_t slot_idx) const {
  assert(slot_idx < empty_slots_bitmap_->bits());

  if (empty_slots_bitmap_->Get(slot_idx)) {
    // Slot is empty. Return dummy.
    return Fingerprint{.active = false};
  }

  const size_t bucket_idx = slot_idx / slots_per_bucket_;

  // Search blocks for fingerprint.
  size_t idx_in_compacted_bitmap = bucket_idx;
  for (size_t block_idx = 0; block_idx < blocks_.size(); ++block_idx) {
    const Bitmap64Ptr& block_bitmap = block_bitmaps_[block_idx];
    const BlockPtr& block = blocks_[block_idx];

    if (block_idx > 0) {
      // Map `bucket_idx` to index in compacted block bitmap. Re-use
      // `idx_in_compacted_bitmap` across loop iterations, i.e., only map it
      // from one block bitmap to the next.
      idx_in_compacted_bitmap -=
          GetRank(*(block_bitmaps_[block_idx - 1]), idx_in_compacted_bitmap);
    }

    // Fingerprint can't be part of "empty buckets block" (this case is already
    // taken care of by checking `empty_slots_bitmap_` above).
    if (block->num_bits() == kEmptyBucketsBlockMarker) continue;

    if (block_bitmap->Get(idx_in_compacted_bitmap)) {
      // Block `block_idx` contains fingerprints of bucket `bucket_idx`.

      const size_t idx_in_block = GetIndexOfFingerprintInBlock(
          block_idx, idx_in_compacted_bitmap, slot_idx);

      return Fingerprint{/*active=*/true, block->num_bits(),
                         /*fingerprint=*/block->Get(idx_in_block)};
    }
  }

  // Unreachable.
  std::cerr << "Couldn't find block for slot_idx " << slot_idx;
  std::exit(1);
}

std::string FingerprintStore::Encode(bool bitmaps_only) const {
  ByteBuffer result;

  // Encode number of blocks.
  const uint32_t num_blocks = blocks_.size();
  PutVarint32(num_blocks, &result);

  // ** Bitmaps.

  // Encode num bits of `empty_slots_bitmap_`.
  PutVarint32(empty_slots_bitmap_->bits(), &result);
  // Encode `empty_slots_bitmap_`.
  if (use_rle_to_encode_block_bitmaps_) {
    const RleBitmap rle_bitmap(*empty_slots_bitmap_);
    PutString(rle_bitmap.data(), &result);
  } else {
    std::string bitmap_encoded;
    Bitmap64::DenseEncode(*empty_slots_bitmap_, &bitmap_encoded);
    PutString(bitmap_encoded, &result);
  }

  // Encode block bitmaps, except "empty buckets block" which can be
  // re-constructed from `empty_slots_bitmap_` using
  // cuckoo_utils.h:GetEmptyBucketsBitmap(..).
  std::vector<Bitmap64Ptr> block_bitmaps_without_empty_block;
  for (size_t i = 0; i < block_bitmaps_.size(); ++i) {
    if (blocks_[i]->num_bits() == kEmptyBucketsBlockMarker) continue;
    const Bitmap64Ptr& curr_bitmap = block_bitmaps_[i];
    Bitmap64Ptr new_bitmap = absl::make_unique<Bitmap64>(curr_bitmap->bits());
    for (const size_t bit : curr_bitmap->TrueBitIndices())
      new_bitmap->Set(bit, true);
    block_bitmaps_without_empty_block.push_back(std::move(new_bitmap));
  }

  // Encode num bits of block bitmaps.
  for (size_t i = 0; i < block_bitmaps_without_empty_block.size(); ++i)
    PutVarint32(block_bitmaps_without_empty_block[i]->bits(), &result);

  // Encode block bitmaps.
  const Bitmap64 global_bitmap =
      Bitmap64::GetGlobalBitmap(block_bitmaps_without_empty_block);
  if (use_rle_to_encode_block_bitmaps_) {
    const RleBitmap rle_bitmap(global_bitmap);
    PutString(rle_bitmap.data(), &result);
  } else {
    std::string bitmap_encoded;
    Bitmap64::DenseEncode(global_bitmap, &bitmap_encoded);
    PutString(bitmap_encoded, &result);
  }

  std::string encoded(result.data(), result.pos());
  if (!bitmaps_only) {
    // Encode blocks.
    for (const BlockPtr& block : blocks_)
      absl::StrAppend(&encoded, block->GetData());
  }
  return encoded;
}

void FingerprintStore::PrintStats() const {
  for (size_t i = 0; i < blocks_.size(); ++i) {
    std::cout << "block " << i << ": bits: " << blocks_[i]->num_bits()
              << ", buckets: " << block_bitmaps_[i]->GetOnesCount()
              << std::endl;
  }
  std::cout << "GetSizeInBytes(bitmaps_only = false): "
            << GetSizeInBytes(/*bitmaps_only=*/false) << std::endl;
  std::cout << "GetBitsPerFingerprint(bitmaps_only = false): "
            << GetBitsPerFingerprint(/*bitmaps_only=*/false) << std::endl;
  std::cout << "GetZstdCompressedSizeInBytes(bitmaps_only = false): "
            << GetZstdCompressedSizeInBytes(/*bitmaps_only=*/false)
            << std::endl;
  std::cout << "GetBitsPerFingerprintZstdCompressed(bitmaps_only = false): "
            << GetBitsPerFingerprintZstdCompressed(/*bitmaps_only=*/false)
            << std::endl;

  std::cout << "GetSizeInBytes(bitmaps_only = true): "
            << GetSizeInBytes(/*bitmaps_only=*/true) << std::endl;
  std::cout << "GetBitsPerFingerprint(bitmaps_only = true): "
            << GetBitsPerFingerprint(/*bitmaps_only=*/true) << std::endl;
  std::cout << "GetZstdCompressedSizeInBytes(bitmaps_only = true): "
            << GetZstdCompressedSizeInBytes(/*bitmaps_only=*/true) << std::endl;
  std::cout << "GetBitsPerFingerprintZstdCompressed(bitmaps_only = true): "
            << GetBitsPerFingerprintZstdCompressed(/*bitmaps_only=*/true)
            << std::endl;
}

size_t FingerprintStore::GetBucketIndex(const size_t block_idx,
                                        const size_t bit_idx) const {
  size_t pos = bit_idx;
  for (int i = block_idx - 1; i >= 0; --i) {
    if (!SelectZero(*(block_bitmaps_[i]), pos, &pos)) {
      std::cerr << "Insufficient number of zeros in block bitmap " << i
                << std::endl;
      exit(EXIT_FAILURE);
    }
  }
  return pos;
}

size_t FingerprintStore::GetNumItemsInBucket(const size_t bucket_idx) const {
  size_t count = 0;
  const size_t first_slot_idx = bucket_idx * slots_per_bucket_;
  assert(first_slot_idx + slots_per_bucket_ <= empty_slots_bitmap_->bits());
  for (size_t i = first_slot_idx; i < first_slot_idx + slots_per_bucket_; ++i)
    count += !empty_slots_bitmap_->Get(i);
  return count;
}

size_t FingerprintStore::GetIndexOfFingerprintInBlock(
    const size_t block_idx, const size_t idx_in_compacted_bitmap,
    const size_t slot_idx) const {
  assert(block_idx < block_bitmaps_.size());
  const Bitmap64Ptr& block_bitmap = block_bitmaps_[block_idx];
  assert(idx_in_compacted_bitmap < block_bitmap->bits());

  // For one slot per bucket, the index is simply the rank of
  // `idx_in_compacted_bitmap` in the block bitmap `block_idx`.
  if (slots_per_bucket_ == 1)
    return GetRank(*block_bitmap, idx_in_compacted_bitmap);

  // For multiple slots per bucket, we need to perform a few extra steps (these
  // are required since we only maintain one bit per bucket in the block bitmaps
  // and we need to account for empty slots in prior buckets that are stored in
  // the same block, i.e., have the same fingerprint length):

  // (1) For each set bit in `block_bitmap` up to (exclusive) bit
  // `idx_in_compacted_bitmap`, we determine the corresponding bucket (we
  // essentially partially "de-compact" the block bitmaps in this step).

  // (2) We count the number occupied slots in these buckets (=> `count`).

  size_t count = 0;
  for (const size_t bit_idx : block_bitmap->TrueBitIndices()) {
    if (bit_idx >= idx_in_compacted_bitmap) break;
    const size_t corr_bucket_idx = GetBucketIndex(block_idx, bit_idx);  // (1)
    count += GetNumItemsInBucket(corr_bucket_idx);                      // (2)
  }

  // (3) We count the number of empty slots in the bucket `slot_idx /
  // slots_per_bucket` up to (exclusive) `slot_idx`.

  const size_t bucket_idx = slot_idx / slots_per_bucket_;
  const size_t first_slot_in_bucket = bucket_idx * slots_per_bucket_;
  size_t num_empty_slots = 0;
  for (size_t i = first_slot_in_bucket; i < slot_idx; ++i)  // (3)
    num_empty_slots += empty_slots_bitmap_->Get(i);

  // (4) The fingerprint is at offset `count` - `num_empty_slots` + (`slot_idx`
  // mod `slots_per_bucket_`).

  return count - num_empty_slots + (slot_idx % slots_per_bucket_);  // (4)
}

size_t FingerprintStore::MapBucketIndexToBitInBlockBitmap(
    const size_t bucket_idx, const size_t block_bitmap_idx) const {
  assert(block_bitmap_idx <= block_bitmaps_.size());
  size_t curr_idx = bucket_idx;
  // Keep subtracting curr_bitmap.Rank(curr_idx) from `curr_idx`. In one
  // iteration, we map `curr_idx` (which corresponds to a bit in the current
  // bitmap) to its index (bit) in the next bitmap. We continue this procedure
  // up to (exclusive) the bitmap at `block_bitmap_idx`.
  for (size_t i = 0; i < block_bitmap_idx; ++i) {
    const Bitmap64Ptr& curr_bitmap = block_bitmaps_[i];
    const size_t rank = GetRank(*curr_bitmap, curr_idx);
    assert(curr_idx >= rank);
    curr_idx -= rank;
  }
  return curr_idx;
}

void FingerprintStore::CreateAndCompactBlockBitmaps(
    const std::vector<size_t>& lengths,
    absl::flat_hash_map<size_t, BlockContent>* blocks) {
  // Create block bitmap for first block (which cannot be compacted).
  if (!lengths.empty()) {
    Bitmap64Ptr& first_block_bitmap = (*blocks)[lengths[0]].block_bitmap;
    first_block_bitmap->InitRankLookupTable();
    block_bitmaps_.push_back(std::move(first_block_bitmap));
  }

  // Create and compact block bitmaps for all remaining blocks.
  for (size_t i = 1; i < lengths.size(); ++i) {
    const size_t length = lengths[i];
    const Bitmap64Ptr& curr_bitmap = (*blocks)[length].block_bitmap;
    const size_t num_bits_compacted_bitmap =
        block_bitmaps_.back()->GetZeroesCount();
    Bitmap64Ptr compacted_bitmap =
        absl::make_unique<Bitmap64>(/*size=*/num_bits_compacted_bitmap);
    for (const size_t bucket_idx : curr_bitmap->TrueBitIndices()) {
      // Map `bucket_idx` to index in compacted block bitmap.
      const size_t idx_in_compacted_bitmap =
          MapBucketIndexToBitInBlockBitmap(bucket_idx, block_bitmaps_.size());
      compacted_bitmap->Set(idx_in_compacted_bitmap, true);
    }
    compacted_bitmap->InitRankLookupTable();
    block_bitmaps_.push_back(std::move(compacted_bitmap));
  }
}

}  // namespace ci
