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
// File: cuckoo_kicker.cc
// -----------------------------------------------------------------------------

#include "cuckoo_kicker.h"

#include <cstdlib>

#include "cuckoo_utils.h"

namespace ci {

constexpr size_t CuckooKicker::kDefaultMaxKicks;

size_t CuckooKicker::GetNumSecondaryItems(const size_t bucket_idx) const {
  const Bucket& bucket = buckets_[bucket_idx];
  return absl::c_count_if(bucket.slots_,
                          [bucket_idx](const CuckooValue& value) {
                            return value.secondary_bucket == bucket_idx;
                          });
}

void CuckooKicker::FindVictim(const size_t victim_idx,
                              const size_t primary_bucket_idx,
                              const size_t secondary_bucket_idx,
                              const bool kick_secondary,
                              size_t* victim_bucket_idx,
                              size_t* idx_within_victim_bucket) const {
  // Iterate through potential victims in primary and secondary bucket until
  // we've found `victim_idx`.
  size_t curr_victim_idx = 0;

  auto search_bucket = [&](size_t bucket_idx) {
    const Bucket& bucket = buckets_[bucket_idx];
    for (size_t i = 0; i < bucket.slots_.size(); ++i) {
      const CuckooValue& curr_val = bucket.slots_[i];
      const size_t bucket_idx_to_compare =
          kick_secondary ? curr_val.secondary_bucket : curr_val.primary_bucket;
      if (bucket_idx_to_compare == bucket_idx) {
        // Slot `i` contains a potential victim.
        if (curr_victim_idx == victim_idx) {
          *victim_bucket_idx = bucket_idx;
          *idx_within_victim_bucket = i;
          return true;
        }
        ++curr_victim_idx;
      }
    }
    return false;
  };

  if (search_bucket(primary_bucket_idx)) return;
  if (search_bucket(secondary_bucket_idx)) return;

  assert(false);  // "Couldn't find victim with idx " << victim_idx;
}

CuckooValue CuckooKicker::SwapWithValue(Bucket* bucket, const size_t victim_idx,
                                        const CuckooValue& value) {
  const CuckooValue victim = bucket->slots_[victim_idx];
  bucket->slots_[victim_idx] = value;
  return victim;
}

CuckooValue CuckooKicker::SwapWithRandomValue(const CuckooValue& value,
                                              size_t* victim_bucket_idx) {
  Bucket* primary_bucket = &buckets_[value.primary_bucket];
  Bucket* secondary_bucket = &buckets_[value.secondary_bucket];

  // Method may only be called when both buckets are full.
  assert(primary_bucket->slots_.size() == slots_per_bucket_);
  assert(secondary_bucket->slots_.size() == slots_per_bucket_);

  if (!skew_kicking_) {
    // Select victim bucket.
    *victim_bucket_idx =
        GetRandomBool() ? value.primary_bucket : value.secondary_bucket;
    Bucket* victim_bucket = &buckets_[*victim_bucket_idx];
    // Choose any value as victim (irrespective of whether it resides in its
    // primary or secondary bucket).
    return SwapWithValue(victim_bucket, GetRandomVictimIndex(), value);
  }

  // Skew kicking.

  const size_t num_slots_both_buckets = 2 * slots_per_bucket_;

  // Count number of items that reside in their secondary bucket.
  const size_t num_in_secondary = GetNumSecondaryItems(value.primary_bucket) +
                                  GetNumSecondaryItems(value.secondary_bucket);

  if (num_in_secondary == 0 || num_in_secondary == num_slots_both_buckets) {
    // Can't perform skewed kick. Just kick any item.
    *victim_bucket_idx =
        GetRandomBool() ? value.primary_bucket : value.secondary_bucket;
    Bucket* victim_bucket = &buckets_[*victim_bucket_idx];
    return SwapWithValue(victim_bucket, GetRandomVictimIndex(), value);
  }
  const size_t num_in_primary = num_slots_both_buckets - num_in_secondary;

  // "Weigh" probability according to the ratio of items in secondary buckets
  // vs. items in primary buckets. For example, if the ratio is 3:1, we give
  // three times as much weight to the set of items in secondary buckets to have
  // equal probabilities for all items under a `kick_skew_factor_` of 1.0. The
  // reason for that is that we below first decide whether to kick a primary or
  // a secondary item and then choose a victim within the respective set.
  double secondary_weight_factor =
      static_cast<double>(num_in_secondary) / num_in_primary;

  // Add `kick_skew_factor_`.
  secondary_weight_factor *= kick_skew_factor_;

  // Set probability such that it is `secondary_weight_factor` times more likely
  // to kick an item from the secondary vs. one from the primary set.
  const double weighted_probability =
      secondary_weight_factor / (secondary_weight_factor + 1);
  assert(weighted_probability > 0.0 && weighted_probability < 1.0);

  // Roll a (skewed) dice to decide whether we want to kick a primary or a
  // secondary item.
  const bool kick_secondary = GetRandomBool(weighted_probability);

  // Choose victim index (within set of primary/secondary items).
  const size_t num_potential_victims =
      kick_secondary ? num_in_secondary : num_in_primary;
  const size_t victim_idx = GetRandomVictimIndex(num_potential_victims);

  // Find and kick victim.
  size_t idx_within_victim_bucket;
  FindVictim(victim_idx, value.primary_bucket, value.secondary_bucket,
             kick_secondary, victim_bucket_idx, &idx_within_victim_bucket);
  Bucket* victim_bucket = &buckets_[*victim_bucket_idx];
  return SwapWithValue(victim_bucket, idx_within_victim_bucket, value);
}

bool CuckooKicker::InsertValueWithKick(CuckooValue* value) {
  // Swap `value` with random value inside its primary or secondary bucket.
  size_t victim_bucket_idx;
  const CuckooValue victim = SwapWithRandomValue(*value, &victim_bucket_idx);

  // Try to insert `victim` into its alternative bucket.
  const size_t alternative_bucket_idx =
      victim_bucket_idx == victim.primary_bucket ? victim.secondary_bucket
                                                 : victim.primary_bucket;
  Bucket* alternative_bucket = &buckets_[alternative_bucket_idx];
  if (alternative_bucket->InsertValue(victim)) return true;

  // Alternative bucket is full. Victim becomes new in-flight value.
  *value = victim;
  return false;
}

bool CuckooKicker::InsertValueWithKicking(const CuckooValue& value) {
  Bucket* primary_bucket = &buckets_[value.primary_bucket];
  Bucket* secondary_bucket = &buckets_[value.secondary_bucket];

  if (primary_bucket->InsertValue(value)) return true;
  if (secondary_bucket->InsertValue(value)) return true;

  // Both buckets are full. Try to insert with kicking.
  CuckooValue in_flight_value = value;
  for (size_t num_kicks = 0; num_kicks <= max_kicks_; ++num_kicks) {
    if (InsertValueWithKick(&in_flight_value)) {
      if (num_kicks > max_kicks_observed_) max_kicks_observed_ = num_kicks;
      return true;
    }
  }

  // Exceeded `max_kicks_` kicks. Insertion failed.
  return false;
}

}  // namespace ci
