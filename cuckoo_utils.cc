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
// File: cuckoo_utils.cc
// -----------------------------------------------------------------------------

#include "cuckoo_utils.h"

#include <cstdint>
#include <cstdlib>
#include <limits>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "common/bit_packing.h"
#include "common/byte_coding.h"

namespace ci {

size_t GetMinNumBuckets(const size_t num_values, const size_t slots_per_bucket,
                        double max_load_factor) {
  assert(max_load_factor > 0.0 && max_load_factor < 1.0);
  const size_t min_num_buckets = static_cast<size_t>(std::ceil(
      (static_cast<double>(num_values) / max_load_factor) / slots_per_bucket));
  return min_num_buckets;
}

size_t GetMinNumBuckets(const size_t num_values,
                        const size_t slots_per_bucket) {
  if (slots_per_bucket == 1) {
    return GetMinNumBuckets(num_values, slots_per_bucket,
                            kMaxLoadFactor1SlotsPerBucket);
  } else if (slots_per_bucket == 2) {
    return GetMinNumBuckets(num_values, slots_per_bucket,
                            kMaxLoadFactor2SlotsPerBucket);
  } else if (slots_per_bucket == 4) {
    return GetMinNumBuckets(num_values, slots_per_bucket,
                            kMaxLoadFactor4SlotsPerBucket);
  } else if (slots_per_bucket == 8) {
    return GetMinNumBuckets(num_values, slots_per_bucket,
                            kMaxLoadFactor8SlotsPerBucket);
  }
  std::cerr << "No default max load factor for " << slots_per_bucket
            << " slots per bucket." << std::endl;
  exit(EXIT_FAILURE);
}

size_t GetMinCollisionFreeFingerprintLength(
    const std::vector<uint64_t>& fingerprints, const bool use_prefix_bits) {
  if (fingerprints.size() < 2) return 0;
  int num_bits = 1;
  for (; num_bits <= 64; ++num_bits) {
    absl::flat_hash_set<uint64_t> unique_fingerprints;
    bool success = true;
    for (const uint64_t fp : fingerprints) {
      // Extract `num_bits` prefix or suffix bits.
      const uint64_t fp_bits = use_prefix_bits
                                   ? GetFingerprintPrefix(fp, num_bits)
                                   : GetFingerprintSuffix(fp, num_bits);

      if (auto [_, was_inserted] = unique_fingerprints.insert(fp_bits);
          !was_inserted) {
        // Detected conflict. Try next higher number of bits.
        success = false;
        break;
      }
    }
    if (success) break;
  }
  if (num_bits == 65) {
    std::cerr << "Exhaused all 64 bits and still having collisions."
              << std::endl;
    exit(EXIT_FAILURE);
  }
  return num_bits;
}

size_t GetMinCollisionFreeFingerprintPrefixOrSuffix(
    const std::vector<uint64_t>& fingerprints, bool* use_prefix_bits) {
  // Determine minimum number of bits starting with lowest bit.
  const int num_suffix_bits = GetMinCollisionFreeFingerprintLength(
      fingerprints, /*use_prefix_bits=*/false);

  if (num_suffix_bits <= 1) {
    // No need to check prefix bits.
    *use_prefix_bits = false;
    return num_suffix_bits;
  }

  // Determine minimum number of bits starting with highest bit.
  const int num_prefix_bits = GetMinCollisionFreeFingerprintLength(
      fingerprints, /*use_prefix_bits=*/true);

  if (num_suffix_bits <= num_prefix_bits) {
    // Prefer using suffix bits.
    *use_prefix_bits = false;
    return num_suffix_bits;
  }

  *use_prefix_bits = true;
  return num_prefix_bits;
}

bool CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
    const std::vector<Fingerprint>& fingerprints,
    const size_t slots_per_bucket) {
  for (size_t i = 0; i < fingerprints.size(); i += slots_per_bucket) {
    bool found_active = false;
    size_t num_bits;
    for (size_t j = 0; j < slots_per_bucket; ++j) {
      const ci::Fingerprint& fp = fingerprints[i + j];
      if (!fp.active) continue;
      if (!found_active) {
        num_bits = fp.num_bits;
        found_active = true;
        continue;
      }
      if (fp.num_bits != num_bits) return false;
    }
  }
  return true;
}

bool Bucket::InsertValue(const CuckooValue& value) {
  if (slots_.size() < num_slots_) {
    slots_.push_back(value);
    return true;
  }
  return false;
}

namespace {

bool ContainsValue(absl::Span<const CuckooValue> values,
                   const CuckooValue& value) {
  for (const CuckooValue& val : values) {
    if (val.orig_value == value.orig_value) return true;
  }
  return false;
}

}  // namespace

bool Bucket::ContainsValue(const CuckooValue& value) const {
  return ci::ContainsValue(slots_, value);
}

bool LookupValueInBuckets(absl::Span<const Bucket> buckets,
                          const CuckooValue value, bool* in_primary) {
  if (buckets[value.primary_bucket].ContainsValue(value)) {
    *in_primary = true;
    return true;
  }
  if (buckets[value.secondary_bucket].ContainsValue(value)) {
    *in_primary = false;
    return true;
  }
  return false;
}

void FillKicked(absl::Span<const CuckooValue> values,
                absl::Span<Bucket> buckets) {
  for (const CuckooValue& value : values) {
    bool in_primary;
    LookupValueInBuckets(buckets, value, &in_primary);
    std::vector<CuckooValue>& kicked = buckets[value.primary_bucket].kicked_;
    // If the value isn't in its primary bucket and not in `kicked`, add it.
    if (!in_primary && !ContainsValue(kicked, value)) kicked.push_back(value);
  }
}

size_t GetRank(const Bitmap64& bitmap, const size_t idx) {
  assert(idx < bitmap.bits());
  return bitmap.GetOnesCountBeforeLimit(/*limit=*/idx);
}

namespace {
// Helper function that implements SelectOne() or SelectZero() depending on
// whether `count_ones` is set.
bool Select(const Bitmap64& bitmap, const size_t ith, const bool count_ones,
            size_t* pos) {
  size_t count = 0;
  for (size_t i = 0; i < bitmap.bits(); ++i) {
    if (bitmap.Get(i) == count_ones) {
      if (count == ith) {
        *pos = i;
        return true;
      }
      ++count;
    }
  }
  return false;
}
}  // namespace

bool SelectOne(const Bitmap64& bitmap, const size_t ith, size_t* pos) {
  return Select(bitmap, ith, /*count_ones=*/true, pos);
}

bool SelectZero(const Bitmap64& bitmap, const size_t ith, size_t* pos) {
  return Select(bitmap, ith, /*count_ones=*/false, pos);
}

Bitmap64Ptr GetEmptyBucketsBitmap(const Bitmap64& empty_slots_bitmap,
                                  const size_t slots_per_bucket) {
  assert(empty_slots_bitmap.bits() % slots_per_bucket == 0);
  Bitmap64Ptr empty_buckets_bitmap = absl::make_unique<Bitmap64>(
      /*size=*/empty_slots_bitmap.bits() / slots_per_bucket);
  for (size_t i = 0; i < empty_slots_bitmap.bits(); i += slots_per_bucket) {
    bool empty_bucket = true;
    for (size_t j = 0; j < slots_per_bucket; ++j) {
      if (!empty_slots_bitmap.Get(i + j)) {
        empty_bucket = false;
        break;
      }
    }
    if (empty_bucket) empty_buckets_bitmap->Set(i / slots_per_bucket, true);
  }
  return empty_buckets_bitmap;
}

}  // namespace ci
