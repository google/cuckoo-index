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
// File: cuckoo_index_test.cc
// -----------------------------------------------------------------------------

#include "cuckoo_index.h"

#include <limits>
#include <vector>

#include "cuckoo_utils.h"
#include "data.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "index_structure.h"

namespace ci {

// For all tests we create CuckooIndexes with 900 rows and 3 rows per stripe.
constexpr size_t kNumRows = 300;
constexpr size_t kNumRowsPerStripe = 3;
// Number of negative lookups to perform.
constexpr int kNumNegativeLookups = 10 * 1000;

// Returns a column with `num_rows` entries and `num_values` different values,
// set in increasing manner of `num_rows / num_values` runs with the same value.
ColumnPtr FillColumn(size_t num_rows, size_t num_values) {
  assert(num_values > 0);
  assert(num_rows >= num_values);
  assert(num_rows % num_values == 0);
  const size_t factor = num_rows / num_values;
  std::vector<int> data(num_rows, 0);
  for (size_t i = 0; i < num_rows; ++i) data[i] = i / factor;
  return Column::IntColumn("int-column", std::move(data));
}

// Checks that positive lookups are exact (for all values in the column).
void CheckPositiveLookups(const Column& column, const IndexStructure* index) {
  const size_t num_stripes = column.num_rows() / kNumRowsPerStripe;
  for (const int value : column.distinct_values()) {
    for (size_t stripe_id = 0; stripe_id < num_stripes; ++stripe_id)
      EXPECT_EQ(column.StripeContains(kNumRowsPerStripe, stripe_id, value),
                index->StripeContains(stripe_id, value));
  }
}

// Returns the average scan-rate of `kNumNegativeLookups` negative lookups.
double ScanRateNegativeLookups(const Column& column,
                               const IndexStructure* index) {
  const size_t num_stripes = column.num_rows() / kNumRowsPerStripe;
  const int start = column.max() + 1;
  assert(start + kNumNegativeLookups <= std::numeric_limits<int32_t>::max());
  size_t num_false_positive_stripes = 0;
  for (int value = start; value < start + kNumNegativeLookups; ++value) {
    for (size_t stripe_id = 0; stripe_id < num_stripes; ++stripe_id)
      if (index->StripeContains(stripe_id, value)) ++num_false_positive_stripes;
  }
  return static_cast<double>(num_false_positive_stripes) /
         (num_stripes * kNumNegativeLookups);
}

// Helper for the PositiveLookups* tests below: checks lookups of all existing
// values are exact.
void PositiveLookups(const size_t num_values,
                     const bool prefix_bits_optimization) {
  const ColumnPtr column = FillColumn(kNumRows, num_values);

  for (const CuckooAlgorithm alg :
       std::vector<CuckooAlgorithm>{CuckooAlgorithm::KICKING}) {
    const IndexStructurePtr index =
        CuckooIndexFactory(alg, kMaxLoadFactor2SlotsPerBucket,
                           /*scan_rate=*/0.05,
                           /*slots_per_bucket=*/2, prefix_bits_optimization)
            .Create(*column, kNumRowsPerStripe);
    CheckPositiveLookups(*column, index.get());
  }
}

// Creates two different cuckoo-indexes with scan-rate 0.1 and 0.01 and checks
// that the scan-rate is bounded as expected.
void NegativeLookups(const size_t num_values,
                     const bool prefix_bits_optimization) {
  const ColumnPtr column = FillColumn(kNumRows, num_values);

  for (const CuckooAlgorithm alg :
       std::vector<CuckooAlgorithm>{CuckooAlgorithm::KICKING}) {
    const IndexStructurePtr index1 =
        CuckooIndexFactory(alg, kMaxLoadFactor2SlotsPerBucket,
                           /*scan_rate=*/0.1, /*slots_per_bucket=*/2,
                           prefix_bits_optimization)
            .Create(*column, kNumRowsPerStripe);
    const double scan_rate1 = ScanRateNegativeLookups(*column, index1.get());
    EXPECT_LE(scan_rate1, 0.101);
    EXPECT_GT(scan_rate1, 0.0);

    // TODO: The following test is very time-consuming, causing the suite to
    // time out after 5 minutes. Find a way to parallelize it or move to
    // a separate test suite.
    // const IndexStructurePtr index2 =
    // CuckooIndexFactory(alg, kMaxLoadFactor2SlotsPerBucket,
    // [>scan_rate=*/0.01, /*slots_per_bucket=<]2,
    // prefix_bits_optimization)
    // .Create(*column, kNumRowsPerStripe);
    // const double scan_rate2 = ScanRateNegativeLookups(*column, index2.get());
    // EXPECT_LE(scan_rate2, 0.0101);
    // EXPECT_GT(scan_rate2, 0.0);
  }
}

// *** The actual tests: ***

TEST(CuckooIndexTest, PositiveLookupsSingleValue) {
  PositiveLookups(/*num_values=*/1, /*prefix_bits_optimization=*/false);
}

TEST(CuckooIndexTest, PositiveLookupsSingleValueWithPrefixBitsOptimization) {
  PositiveLookups(/*num_values=*/1, /*prefix_bits_optimization=*/true);
}

TEST(CuckooIndexTest, NegativeLookupsSingleValue) {
  NegativeLookups(/*num_values=*/1, /*prefix_bits_optimization=*/false);
}

TEST(CuckooIndexTest, NegativeLookupsSingleValueWithPrefixBitsOptimization) {
  NegativeLookups(/*num_values=*/1, /*prefix_bits_optimization=*/true);
}

TEST(CuckooIndexTest, PositiveLookupsFewValues) {
  PositiveLookups(/*num_values=*/30, /*prefix_bits_optimization=*/false);
}

TEST(CuckooIndexTest, PositiveLookupsFewValuesWithPrefixBitsOptimization) {
  PositiveLookups(/*num_values=*/30, /*prefix_bits_optimization=*/true);
}

TEST(CuckooIndexTest, NegativeLookupsFewValues) {
  NegativeLookups(/*num_values=*/30, /*prefix_bits_optimization=*/false);
}

TEST(CuckooIndexTest, NegativeLookupsFewValuesWithPrefixBitsOptimization) {
  NegativeLookups(/*num_values=*/30, /*prefix_bits_optimization=*/true);
}

TEST(CuckooIndexTest, PositiveLookupsAllUniques) {
  PositiveLookups(/*num_values=*/kNumRows, /*prefix_bits_optimization=*/false);
}

TEST(CuckooIndexTest, PositiveLookupsAllUniquesWithPrefixBitsOptimization) {
  PositiveLookups(/*num_values=*/kNumRows, /*prefix_bits_optimization=*/true);
}

TEST(CuckooIndexTest, NegativeLookupsAllUniques) {
  NegativeLookups(/*num_values=*/kNumRows, /*prefix_bits_optimization=*/false);
}

// TEST(CuckooIndexTest, NegativeLookupsAllUniquesWithPrefixBitsOptimization) {
// NegativeLookups([>num_values=<]kNumRows, [>prefix_bits_optimization=<]true);
// }

TEST(CuckooIndexTest, LastRowDropped) {
  // The last row will be dropped, since only stripes with `kNumRowsPerStripe`
  // (= 3) rows are created.
  const ColumnPtr column = FillColumn(/*num_rows=*/4, /*num_values=*/4);
  const IndexStructurePtr index =
      CuckooIndexFactory(CuckooAlgorithm::KICKING,
                         kMaxLoadFactor2SlotsPerBucket,
                         /*scan_rate=*/0.1, /*slots_per_bucket=*/2,
                         /*prefix_bits_optimization=*/false)
          .Create(*column, kNumRowsPerStripe);
  EXPECT_EQ(reinterpret_cast<const CuckooIndex&>(*index).active_slots(), 3);
}

}  // namespace ci
