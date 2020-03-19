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
// File: cuckoo_utils.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_CUCKOO_UTILS_H_
#define CUCKOO_INDEX_CUCKOO_UTILS_H_

#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "absl/hash/internal/city.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "common/bit_packing.h"
#include "common/bitmap.h"

namespace ci {

// The seeds for the primary & secondary buckets and the fingerprint.
constexpr uint64_t kSeedPrimaryBucket = 17;
constexpr uint64_t kSeedSecondaryBucket = 23;
constexpr uint64_t kSeedFingerprint = 42;

// Maximum load factors (in terms of occupied vs. all slots). Obtained from the
// Cuckoo filter paper:
// https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf.
// Since we don't use partial-key Cuckoo hashing, we could theoretically achieve
// slightly higher load factors.
// However, empirical testing showed that this is not the case (at least an
// extra % is not possible with the current kicking implementation, see
// `cuckoo_kicker_test.cc` for test code). Using a victim stash might allow for
// slightly higher load factors.
constexpr inline double kMaxLoadFactor1SlotsPerBucket = 0.49;
constexpr inline double kMaxLoadFactor2SlotsPerBucket = 0.84;
constexpr inline double kMaxLoadFactor4SlotsPerBucket = 0.95;
constexpr inline double kMaxLoadFactor8SlotsPerBucket = 0.98;

// Returns the minimum number of buckets required to accommodate `num_values`
// values with `slots_per_bucket` slots per bucket under a load factor of
// `max_load_factor`.
size_t GetMinNumBuckets(const size_t num_values, const size_t slots_per_bucket,
                        double max_load_factor);

// Uses empirically obtained max load factors.
size_t GetMinNumBuckets(const size_t num_values, const size_t slots_per_bucket);

struct Fingerprint {
  // Indicates whether the corresponding slot in the Cuckoo table is active
  // (i.e., filled with a fingerprint).
  bool active;
  // Number of significant bits (counting from least significant).
  size_t num_bits;
  // Variable-sized fingerprint. May use up to 64 bits. Non-significant bits
  // have to be cleared.
  uint64_t fingerprint;
};

// Returns a mask which has the lowest `num_bits` set.
inline uint64_t FingerprintSuffixMask(const size_t num_bits) {
  return num_bits >= 64 ? std::numeric_limits<uint64_t>::max()
                        : (1ULL << num_bits) - 1ULL;
}

// Returns `num_bits` suffix (lowest) bits.
inline uint64_t GetFingerprintSuffix(const uint64_t fingerprint,
                                     const size_t num_bits) {
  return fingerprint & FingerprintSuffixMask(num_bits);
}

// Returns `num_bits` prefix (highest) bits.
inline uint64_t GetFingerprintPrefix(const uint64_t fingerprint,
                                     const size_t num_bits) {
  if (num_bits == 0) return 0;
  return num_bits >= 64 ? fingerprint : fingerprint >> (64 - num_bits);
}

// Determines the minimum number of bits to make `fingerprints` collision free.
// Uses either prefix or suffix bits (depending on `use_prefix_bits`).
size_t GetMinCollisionFreeFingerprintLength(
    const std::vector<uint64_t>& fingerprints, const bool use_prefix_bits);

// Convenience method that tries both prefix and suffix bits to determine the
// minimum number of bits to make `fingerprints` collision free. Sets
// `use_prefix_bits` depending on whether it used prefix or suffix bits (prefers
// suffix bits and only chooses prefix bits if it would result in fewer bits).
size_t GetMinCollisionFreeFingerprintPrefixOrSuffix(
    const std::vector<uint64_t>& fingerprints, bool* use_prefix_bits);

// Returns true if all buckets only contain equally long fingerprints (i.e., all
// non-empty slots of a bucket contain fingerprints that have the same length).
bool CheckWhetherAllBucketsOnlyContainSameSizeFingerprints(
    const std::vector<Fingerprint>& fingerprints,
    const size_t slots_per_bucket);

// Representation of a value as its two buckets and fingerprint.
struct CuckooValue {
  CuckooValue(int value, size_t num_buckets) {
    orig_value = value;

    // *** POSSIBLY CHOOSE ANOTHER HASHING ALGORITHM ***
    auto value_data = reinterpret_cast<const char*>(&value);
    primary_bucket = absl::hash_internal::CityHash64WithSeed(
                         value_data, sizeof(value), kSeedPrimaryBucket) %
                     num_buckets;
    secondary_bucket = absl::hash_internal::CityHash64WithSeed(
                           value_data, sizeof(value), kSeedSecondaryBucket) %
                       num_buckets;
    fingerprint = absl::hash_internal::CityHash64WithSeed(
        value_data, sizeof(value), kSeedFingerprint);
  }

  std::string ToString() const {
    return absl::StrFormat("{v=%d fp=%llx (%llu | %llu)}", orig_value,
                           fingerprint, primary_bucket, secondary_bucket);
  }

  int orig_value;
  size_t primary_bucket;
  size_t secondary_bucket;
  uint64_t fingerprint;
};

// Class for temporary usage when assigning values to buckets. In particular,
// it keeps a list of values which could *not* be assigned to this bucket even
// though it was their primary choice.
class Bucket {
 public:
  explicit Bucket(size_t num_slots) : num_slots_(num_slots) {}

  // Returns false if bucket is full (i.e. all `num_slots_` slots are occupied).
  bool InsertValue(const CuckooValue& value);

  // Checks whether bucket contains `value`.
  bool ContainsValue(const CuckooValue& value) const;

  size_t num_slots() const { return num_slots_; }

  // The actually assigned values -- up to `num_slots_` entries.
  std::vector<CuckooValue> slots_;
  // The values which were kicked out of the bucket even though it was their
  // primary choice.
  std::vector<CuckooValue> kicked_;

 private:
  size_t num_slots_;
};

// Searches for `value` in its primary and secondary bucket. Returns true if
// value was found and sets `in_primary` to true if `value` resides in its
// primary bucket, and to false otherwise.
bool LookupValueInBuckets(absl::Span<const Bucket> buckets,
                          const CuckooValue value, bool* in_primary);

// Goes over the given `values` and CHECKs for each value that it has been added
// to its primary or secondary bucket. If it was added to the secondary bucket,
// makes sure that the `kicked_` vector in its primary bucket contains the value
// as expected.
void FillKicked(absl::Span<const CuckooValue> values,
                absl::Span<Bucket> buckets);

// Returns the rank of `idx` in `bitmap`.
size_t GetRank(const Bitmap64& bitmap, const size_t idx);

// Sets `pos` according to the `ith` one-bit in `bitmap`. For example,
// Select(bitmap=0010, ith=1, &pos) would set `pos` to the zero-based position
// of the 1st one-bit (2 in this case). Returns true if `ith` one-bit was
// found and false otherwise.
bool SelectOne(const Bitmap64& bitmap, const size_t ith, size_t* pos);

// Sets `pos` according to the `ith` zero-bit in `bitmap`. Returns true if `ith`
// zero-bit was found and false otherwise.
bool SelectZero(const Bitmap64& bitmap, const size_t ith, size_t* pos);

// Creates an empty buckets bitmap from an `empty_slots_bitmap`.
Bitmap64Ptr GetEmptyBucketsBitmap(const Bitmap64& empty_slots_bitmap,
                                  const size_t slots_per_bucket);

}  // namespace ci

#endif  // CUCKOO_INDEX_CUCKOO_UTILS_H_
