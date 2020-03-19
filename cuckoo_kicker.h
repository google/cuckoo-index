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
// File: cuckoo_kicker.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_CUCKOO_KICKER_H_
#define CUCKOO_INDEX_CUCKOO_KICKER_H_

#include "absl/container/flat_hash_map.h"
#include "absl/random/random.h"
#include "cuckoo_utils.h"

namespace ci {

// Used to skew the kicking procedure towards items that reside in their
// secondary bucket (by using a value greater than 1.0). Note that skewed
// kicking affects build performance and may lead to build failures. Below
// constants were obtained empirically using a random test set of 1M items.
constexpr double kKickSkewFactor1SlotsPerBucket = 1.1;
constexpr double kKickSkewFactor2SlotsPerBucket = 16.0;
constexpr double kKickSkewFactor4SlotsPerBucket = 128.0;
constexpr double kKickSkewFactor8SlotsPerBucket = 1024.0;
static const absl::flat_hash_map<size_t, double> GetSkewFactorMap() {
  return absl::flat_hash_map<size_t, double>{
      {1, kKickSkewFactor1SlotsPerBucket},
      {2, kKickSkewFactor2SlotsPerBucket},
      {4, kKickSkewFactor4SlotsPerBucket},
      {8, kKickSkewFactor8SlotsPerBucket}};
}

// Distributes values to `buckets` using the kicking algorithm.
class CuckooKicker {
 public:
  static constexpr size_t kDefaultMaxKicks = 50000;

  // Setting `skew_kicking` may lead to a smaller index (since items in
  // secondary buckets may affect the minimum fingerprint lengths of the
  // corresponding primary buckets). Another positive effect is that positive
  // lookups are more likely to find a match in their primary bucket. On the
  // contrary, users should be aware that this increases build time and may lead
  // to build failures.
  CuckooKicker(size_t slots_per_bucket, absl::Span<Bucket> buckets,
               bool skew_kicking = false, size_t max_kicks = kDefaultMaxKicks)
      : gen_(absl::SeedSeq({42})),
        slots_per_bucket_(slots_per_bucket),
        buckets_(buckets),
        skew_kicking_(skew_kicking),
        kick_skew_factor_(GetSkewFactorMap().at(slots_per_bucket)),
        max_kicks_(max_kicks),
        max_kicks_observed_(0),
        successful_inserts_(0) {}

  // Returns false if `values` couldn't be distributed to `buckets` with
  // kicking.
  bool InsertValues(absl::Span<const CuckooValue> values) {
    for (const CuckooValue& value : values) {
      if (!InsertValueWithKicking(value)) return false;
      ++successful_inserts_;
    }
    return true;
  }

  void PrintStats() {
    std::cout << "slots per bucket: " << slots_per_bucket_ << std::endl;
    std::cout << "max kicks observed: " << max_kicks_observed_ << std::endl;
    std::cout << "successful inserts: " << successful_inserts_ << std::endl;
    std::cout << "load factor: "
              << static_cast<double>(successful_inserts_) /
                     (buckets_.size() * slots_per_bucket_)
              << std::endl;
  }

 private:
  // Returns a "random" bool with the probability of drawing true being
  // `true_probability`. By default, performs an unbiased toin coss.
  bool GetRandomBool(const double true_probability = 0.5) {
    return absl::Bernoulli(gen_, true_probability);
  }

  // Returns a uniformly chosen victim index between 0 and `size` (exclusive).
  size_t GetRandomVictimIndex(const size_t size) {
    return absl::Uniform(gen_, 0u, size);
  }

  // Returns a uniformly chosen victim index between 0 and `slots_per_bucket_`
  // (exclusive).
  size_t GetRandomVictimIndex() {
    return GetRandomVictimIndex(slots_per_bucket_);
  }

  // Returns the number of items in bucket `bucket_idx` for which this bucket is
  // their secondary bucket.
  size_t GetNumSecondaryItems(const size_t bucket_idx) const;

  // Finds `victim_idx` in the set of primary or secondary items (depending on
  // whether `kick_secondary` is set) in both buckets (`primary_bucket_idx` and
  // `secondary_bucket_idx`) and sets the output params `victim_bucket_idx` &
  // `idx_within_victim_bucket` accordingly.
  void FindVictim(const size_t victim_idx, const size_t primary_bucket_idx,
                  const size_t secondary_bucket_idx, const bool kick_secondary,
                  size_t* victim_bucket_idx,
                  size_t* idx_within_victim_bucket) const;

  // Swaps `value` with value at `bucket->slots_[victim_idx]` and returns the
  // victim.
  CuckooValue SwapWithValue(Bucket* bucket, const size_t victim_idx,
                            const CuckooValue& value);

  // Swaps `value` with a random value inside its primary or secondary bucket.
  // May only be used when both buckets are full (i.e. all `slots_per_bucket_`
  // slots are occupied).
  CuckooValue SwapWithRandomValue(const CuckooValue& value,
                                  size_t* victim_bucket_idx);

  // Performs a single "kick". Returns true if the kicked value could be
  // inserted into its alternative bucket. Sets `*value` to the victim value.
  bool InsertValueWithKick(CuckooValue* value);

  // Tries to insert `value` into `buckets_`. Does not check for duplicates
  // (i.e., would insert duplicate fingerprints in the unlikely event of a
  // 64-bit hash collision). Duplicates either need to be removed prior to
  // calling this method or when determining minimal per-bucket fingerprint
  // lengths. Returns false if insertion failed (i.e., we exceeded
  // `kNumMaxKicks` kicks).
  bool InsertValueWithKicking(const CuckooValue& value);

  absl::BitGen gen_;
  const size_t slots_per_bucket_;
  absl::Span<Bucket> buckets_;

  // Used to skew kicking towards items that reside in their secondary bucket.
  const bool skew_kicking_;
  // A factor that defines how much more likely it is for a value that resides
  // in its secondary bucket to be kicked (compared to a value that resides in
  // its primary bucket). Only used if `skew_kicking_` is set.
  const double kick_skew_factor_;
  // Maximum number of kicks allowed before an insertion fails.
  const size_t max_kicks_;

  // ** Statistics.
  // Maximum number of kicks observed.
  size_t max_kicks_observed_;
  // Number of successfully inserted items.
  size_t successful_inserts_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_CUCKOO_KICKER_H_
