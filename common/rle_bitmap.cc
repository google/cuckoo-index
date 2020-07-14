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
// File: rle_bitmap.cc
// -----------------------------------------------------------------------------

#include "common/rle_bitmap.h"

#include <cmath>
#include <numeric>
#include <vector>

namespace ci {
namespace {

// Each time a repeated entry is added, it costs in the worst case 8 + 8 bits
// for the 2 additional run-lengths (raw & repeated) and 1 bit for the value
// to repeat => only add such an entry if more than 17 bits are saved.
constexpr uint32_t kMinDenseRunLength = 18;
// Set the maximum run-length such that 8 bits are used per entry. This leads
// to a good trade-off in size and helps ZSTD when compressing the entries.
constexpr uint32_t kMaxDenseRunLength = 128;

// The "fudge factor" to apply when deciding whether to use the sparse encoding.
// Slightly prefer sparse, since it tends to compress better with zstd.
constexpr double kSparseFudgeFactor = 1.1;
// The maximum run-length for the sparse encoding.
constexpr uint32_t kMaxSparseRunLength = 255;

// Encode the given bitmap in runs using a "dense encoding". The lengths of each
// run is encoded in run_lengths: l..llr where the bits l..ll gives the length
// of the run - 1 (raw-values) and run - kMinRunLength (repeated value). r = 0
// stands for a run where a single value should be repeated and r = 1 means it
// is a run of raw-values (copy verbatim).
void EncodeDenseRunLengths(const Bitmap64& bitmap,
                           std::vector<uint32_t>* run_lengths,
                           std::vector<uint32_t>* bits) {
  size_t i = 0;
  while (i < bitmap.bits()) {
    // From `i` on search for a repeated run of length at least `kMinRunLength`.
    // Once found add a verbatim run if necessary and then the repeated run.
    uint32_t count_rep = 1;
    uint32_t count_raw = 0;
    for (size_t j = i + 1; j < bitmap.bits(); ++j) {
      if (count_rep >= kMaxDenseRunLength + kMinDenseRunLength - 1 ||
          count_raw >= kMaxDenseRunLength)
        break;
      if (bitmap.Get(j) != bitmap.Get(j - 1)) {
        // Did we find a repeated run that is long enough?
        if (count_rep >= kMinDenseRunLength) break;
        count_raw += count_rep;
        count_rep = 1;
      } else {
        count_rep++;
      }
    }
    // Adjust count_raw / count_rep if they are too large / too small.
    if (count_rep < kMinDenseRunLength) {
      count_raw += count_rep;
      count_rep = 0;
    }
    if (count_raw > kMaxDenseRunLength) {
      count_raw = kMaxDenseRunLength;
      count_rep = 0;
    }
    // Store the (possibly adjusted) `count_rep` and/or `count_raw` entries.
    // The scheme is that there may be a couple of raw-values to store before
    // storing a run of repeated values.
    if (count_raw > 0) {
      // Subtract 1 from `count_raw` to save space.
      run_lengths->push_back((count_raw - 1) << 1 | 1);
      for (size_t j = 0; j < count_raw; ++j) bits->push_back(bitmap.Get(i + j));
    }
    if (count_rep > 0) {
      assert(count_rep >= kMinDenseRunLength);
      // Subtract `kMinRunLength` from `count_rep` to save space.
      run_lengths->push_back((count_rep - kMinDenseRunLength) << 1 | 0);
      bits->push_back(bitmap.Get(i + count_raw));
    }
    // Increment `i` by the number of values treated by the added 1 or 2 runs.
    i += count_raw + count_rep;
  }
}

// Fills `run_lengths` with the offsets from one 1-bit to the next. In case
// the offset is > `kMaxSparseRunLength` a 0 is inserted which stands for
// skipping `kMaxSparseRunLength` bits (w/o setting the following bit to 1).
void EncodeSparseRunLengths(const Bitmap64& bitmap,
                            std::vector<uint32_t>* run_lengths) {
  const auto indices = bitmap.TrueBitIndices();
  // Always add a virtual 1-bit at position `bitmaps.bits()`. This avoids
  // special cases for handling the final run of 0-bits at the end of `bitmap`.
  std::vector<size_t> indices_with_sentinel(indices.begin(), indices.end());
  indices_with_sentinel.push_back(bitmap.bits());
  int64_t prev_index = -1;
  for (const size_t index : indices_with_sentinel) {
    int64_t offset = static_cast<int64_t>(index) - prev_index;
    prev_index = index;
    while (offset > kMaxSparseRunLength) {
      // 0 has the special role of marking a run of `kMaxSparseRunLength` 0-bits
      // which is *not* terminated by a 1-bit.
      run_lengths->push_back(0);
      offset -= kMaxSparseRunLength;
    }
    assert(offset >= 1);
    assert(offset <= kMaxSparseRunLength);
    run_lengths->push_back(offset);
  }
}

// Returns a list of dense skip-offsets which can be used to skip over
// run_lengths entries with a "stride" of `skip_offsets_step`. Even entries give
// the count in the uncompressed, original `bitmap` and odd entries the
// corresponding count in the compressed `bits` (see above). E.g.,
// `skip_offsets[0]` is the sum of entries `run_lengths[0]` ..
// `run_lengths[skip_offsets_step - 1]`. `skip_offsets[1]` gives the position in
// `bits` to which `run_lengths[skip_offsets_step]` is referring to.
std::vector<uint32_t> ComputeDenseSkipOffsets(
    const std::vector<uint32_t>& run_lengths, uint32_t skip_offsets_step) {
  std::vector<uint32_t> skip_offsets;
  for (size_t i = 0; i < run_lengths.size(); i += skip_offsets_step) {
    uint32_t uncompressed_count = 0;
    uint32_t compressed_count = 0;
    const size_t end_block =
        std::min(run_lengths.size(), i + skip_offsets_step);
    for (size_t j = i; j < end_block; ++j) {
      const bool is_raw = run_lengths[j] & 1;
      const uint32_t count =
          (run_lengths[j] >> 1) + (is_raw ? 1 : kMinDenseRunLength);
      uncompressed_count += count;
      compressed_count += is_raw ? count : 1;
    }
    skip_offsets.push_back(uncompressed_count);
    skip_offsets.push_back(compressed_count);
  }
  return skip_offsets;
}

// Returns a list of sparse skip-offsets which can be used to skip over
// run_lengths entries with a "stride" of `skip_offsets_step`. `skip_offsets[i]`
// is the sum of entries `run_lengths[i]` .. `run_lengths[i + skip_offsets_step
// - 1]`
std::vector<uint32_t> ComputeSparseSkipOffsets(
    const std::vector<uint32_t>& run_lengths, uint32_t skip_offsets_step) {
  std::vector<uint32_t> skip_offsets;
  for (size_t i = 0; i < run_lengths.size(); i += skip_offsets_step) {
    uint32_t count = 0;
    const size_t end_block =
        std::min(run_lengths.size(), i + skip_offsets_step);
    for (size_t j = i; j < end_block; ++j)
      count += run_lengths[j] == 0 ? kMaxSparseRunLength : run_lengths[j];
    skip_offsets.push_back(count);
  }
  return skip_offsets;
}

}  // namespace

RleBitmap::RleBitmap(const Bitmap64& bitmap) {
  std::vector<uint32_t> run_lengths;
  std::vector<uint32_t> bits;
  std::vector<uint32_t> skip_offsets;
  EncodeDenseRunLengths(bitmap, &run_lengths, &bits);
  // Decide whether the sparse encoding is a better match. Note that for each
  // 1 bit there is "roughly" one entry in run_lengths.
  if (bitmap.GetOnesCount() <
      kSparseFudgeFactor * run_lengths.size() + bits.size() / 8) {
    is_sparse_ = true;
    run_lengths.clear();
    bits.clear();
    EncodeSparseRunLengths(bitmap, &run_lengths);
    skip_offsets_step_ = std::sqrt(run_lengths.size());
    skip_offsets = ComputeSparseSkipOffsets(run_lengths, skip_offsets_step_);
  } else {
    is_sparse_ = false;
    skip_offsets_step_ = std::sqrt(run_lengths.size());
    skip_offsets = ComputeDenseSkipOffsets(run_lengths, skip_offsets_step_);
  }

  // Write everything to a ByteBuffer.
  ByteBuffer result;
  // ** Flag whether the encoding is sparse or dense.
  PutVarint32(is_sparse_ ? 1 : 0, &result);
  // ** The size, i.e., number of uncompressed bits.
  size_ = bitmap.bits();
  PutVarint32(size_, &result);
  // ** Step of `skip_offsets`.
  PutVarint32(skip_offsets_step_, &result);
  // ** Length of the `skip_offsets`.
  skip_offsets_size_ = skip_offsets.size();
  PutVarint32(skip_offsets_size_, &result);
  // ** Size of `run_lengths`.
  run_lengths_size_ = run_lengths.size();
  PutVarint32(run_lengths_size_, &result);
  // ** Size of `bits`.
  bits_size_ = bits.size();
  PutVarint32(bits_size_, &result);
  // ** The `skip_offsets`.
  const int32_t skip_offsets_bit_width = MaxBitWidth<uint32_t>(skip_offsets);
  PutVarint32(static_cast<uint32_t>(skip_offsets_bit_width), &result);
  const size_t skip_offsets_pos = result.pos();
  StoreBitPacked<uint32_t>(skip_offsets, skip_offsets_bit_width, &result);
  // ** The `run_lengths`.
  const int32_t run_lengths_bit_width = MaxBitWidth<uint32_t>(run_lengths);
  assert(run_lengths_bit_width >= 0);
  assert(run_lengths_bit_width < 9);
  PutVarint32(static_cast<uint32_t>(run_lengths_bit_width), &result);
  const size_t run_lengths_pos = result.pos();
  if (run_lengths_size_ > 0)
    StoreBitPacked<uint32_t>(run_lengths, run_lengths_bit_width, &result);
  // ** The bits.
  const size_t bits_pos = result.pos();
  if (bits_size_ > 0) StoreBitPacked<uint32_t>(bits, 1, &result);
  PutSlopBytes(&result);

  // Copy the serialized encoding to the string `data_`.
  data_ = std::string(result.data(), result.pos());

  // Set all three BitPackedReaders for convenient & fast access.
  skip_offsets_ = BitPackedReader<uint32_t>(skip_offsets_bit_width,
                                            data_.data() + skip_offsets_pos);
  run_lengths_ = BitPackedReader<uint32_t>(run_lengths_bit_width,
                                           data_.data() + run_lengths_pos);
  bits_ = BitPackedReader<uint32_t>(1, data_.data() + bits_pos);
}

Bitmap64 RleBitmap::Extract(size_t offset, size_t size) const {
  return is_sparse_ ? ExtractSparse(offset, size) : ExtractDense(offset, size);
}

Bitmap64 RleBitmap::ExtractDense(size_t offset, size_t size) const {
  Bitmap64 result(size);

  size_t rle_pos = 0;
  size_t bits_pos = 0;
  // Use the skip-list to find where to start scanning run_lengths_ and bits_.
  assert(skip_offsets_size_ % 2 == 0);
  for (size_t i = 0; i < skip_offsets_size_; i += 2) {
    if (skip_offsets_.Get(i) > offset) break;
    offset -= skip_offsets_.Get(i);
    rle_pos += skip_offsets_step_;
    bits_pos += skip_offsets_.Get(i + 1);
  }

  // Scan from rle_pos and bits_pos on.
  size_t count_rep = 0;
  size_t count_raw = 0;
  for (size_t i = 0; i < offset + size; ++i) {
    if (count_rep == 0 && count_raw == 0) {
      const uint32_t rle_entry = run_lengths_.Get(rle_pos++);
      if (rle_entry & 1) {
        count_raw = (rle_entry >> 1) + 1;
      } else {
        count_rep = (rle_entry >> 1) + kMinDenseRunLength;
      }
    }
    bool bit;
    if (count_rep > 0) {
      count_rep--;
      bit = bits_.Get(bits_pos);
      if (count_rep == 0) bits_pos++;
    } else {
      assert(count_raw > 0);
      count_raw--;
      bit = bits_.Get(bits_pos++);
    }
    // Set the bit accordingly (once we've reached at least the offset).
    if (i >= offset && bit) result.Set(i - offset, true);
  }
  return result;
}

Bitmap64 RleBitmap::ExtractSparse(size_t offset, size_t size) const {
  Bitmap64 result(size);

  size_t rle_pos = 0;
  // Use the skip-list to find where to start scanning run_lengths_.
  for (size_t i = 0; i < skip_offsets_size_; ++i) {
    if (skip_offsets_.Get(i) > offset) break;
    offset -= skip_offsets_.Get(i);
    rle_pos += skip_offsets_step_;
  }

  // Scan from rle_pos on.
  int64_t i = -1;
  while (i < static_cast<int64_t>(offset + size) &&
         rle_pos < run_lengths_size_) {
    const uint32_t count = run_lengths_.Get(rle_pos++);
    if (count == 0) {
      i += kMaxSparseRunLength;
    } else {
      i += count;
      if (i >= static_cast<int64_t>(offset) &&
          i < static_cast<int64_t>(offset + size)) {
        result.Set(i - offset, true);
      }
    }
  }
  return result;
}

}  // namespace ci
