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
// File: cuckoo_kicker_test.cc
// -----------------------------------------------------------------------------

#include "cuckoo_kicker.h"

#include <string>

#include "absl/types/span.h"
#include "cuckoo_utils.h"
#include "gtest/gtest.h"

namespace ci {

constexpr size_t kNumValues = 1e5;
constexpr size_t kSlotsPerBucket = 2;

constexpr size_t kMaxNumRetries = 10;

// **** Helper methods ****

std::vector<int> CreateValues(const size_t num_values) {
  std::vector<int> values(num_values);
  for (size_t i = 0; i < num_values; ++i) values[i] = i;
  return values;
}

// Returns true if all `values` could be found in `buckets`. Sets
// `in_primary_ratio` according to the ratio of items residing in primary
// buckets.
bool LookupValuesInBuckets(const std::vector<Bucket>& buckets,
                           const std::vector<int>& values,
                           double* in_primary_ratio) {
  size_t num_in_primary = 0;
  for (const int value : values) {
    CuckooValue cuckoo_value(value, buckets.size());
    bool in_primary_flag;
    if (!LookupValueInBuckets(buckets, cuckoo_value, &in_primary_flag))
      return false;
    num_in_primary += in_primary_flag;
  }
  *in_primary_ratio = static_cast<double>(num_in_primary) / values.size();
  return true;
}

// **** Test cases ****

// Starts with the minimum number of buckets required for `kSlotsPerBucket`
// slots and `values.size()`. If construction fails, increases the number of
// buckets and retries (one additional bucket at a time).
std::vector<Bucket> DistributeValuesByKicking(const std::vector<int>& values,
                                              const bool skew_kicking) {
  size_t num_buckets = GetMinNumBuckets(kNumValues, kSlotsPerBucket);

  for (size_t i = 0; i < kMaxNumRetries; ++i) {
    std::vector<Bucket> buckets(num_buckets, Bucket(kSlotsPerBucket));
    std::vector<CuckooValue> cuckoo_values;
    cuckoo_values.reserve(values.size());
    for (const int value : values)
      cuckoo_values.push_back(CuckooValue(value, num_buckets));
    CuckooKicker kicker(kSlotsPerBucket, absl::MakeSpan(buckets), skew_kicking);
    if (kicker.InsertValues(cuckoo_values)) return buckets;
    ++num_buckets;
  }

  std::cerr << "Exceeded kMaxNumRetries: " << kMaxNumRetries << std::endl;
  exit(EXIT_FAILURE);
}

TEST(CuckooKickerTest, InsertValues) {
  const std::vector<int> values = CreateValues(kNumValues);

  // Distribute values by kicking.
  const std::vector<Bucket> buckets =
      DistributeValuesByKicking(values, /*skew_kicking=*/false);

  // Lookup values.
  double in_primary_ratio;
  ASSERT_TRUE(LookupValuesInBuckets(buckets, values, &in_primary_ratio));
  ASSERT_GT(in_primary_ratio, 0.0);
}

TEST(CuckooKickerTest, InsertValuesWithSkewedKicking) {
  const std::vector<int> values = CreateValues(kNumValues);

  // Distribute values by kicking.
  const std::vector<Bucket> buckets =
      DistributeValuesByKicking(values, /*skew_kicking=*/true);

  // Lookup values.
  double in_primary_ratio;
  ASSERT_TRUE(LookupValuesInBuckets(buckets, values, &in_primary_ratio));
  ASSERT_GT(in_primary_ratio, 0.6);
}

TEST(CuckooKickerTest, CheckForDeterministicBehavior) {
  const std::vector<int> values = CreateValues(kNumValues);

  // Distribute values twice using skewed kicker.
  const std::vector<Bucket> buckets =
      DistributeValuesByKicking(values, /*skew_kicking=*/true);
  const std::vector<Bucket> buckets2 =
      DistributeValuesByKicking(values, /*skew_kicking=*/true);

  // Check that both bucket vectors contain the same CuckooValues in `slots_`
  // and `kicked_`.
  ASSERT_EQ(buckets.size(), buckets2.size());
  for (size_t i = 0; i < buckets.size(); ++i) {
    ASSERT_EQ(buckets[i].slots_.size(), buckets2[i].slots_.size());
    ASSERT_EQ(buckets[i].kicked_.size(), buckets2[i].kicked_.size());
    for (size_t j = 0; j < buckets[i].slots_.size(); j++) {
      EXPECT_EQ(buckets[i].slots_[j].ToString(),
                buckets2[i].slots_[j].ToString());
    }
    for (size_t j = 0; j < buckets[i].kicked_.size(); j++) {
      EXPECT_EQ(buckets[i].kicked_[j].ToString(),
                buckets2[i].kicked_[j].ToString());
    }
  }
}

}  // namespace ci
