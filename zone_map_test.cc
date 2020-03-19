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
// File: zone_map_test.h
// -----------------------------------------------------------------------------

#include "zone_map.h"

#include "gtest/gtest.h"

namespace ci {

TEST(ZoneMapTest, StripeContainsSequentialValues) {
  ZoneMap zone_map(/*data=*/{1, 2, 3, 4}, /*num_rows_per_stripe=*/2);

  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/1));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/2));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/3));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/4));

  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/0));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/3));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/2));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/5));
}

TEST(ZoneMapTest, StripeContainsShuffledValues) {
  ZoneMap zone_map(/*data=*/{2, 1, 4, 3}, /*num_rows_per_stripe=*/2);

  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/1));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/2));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/3));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/4));

  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/0));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/3));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/2));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/5));
}

TEST(ZoneMapTest, StripeContainsDuplicateValues) {
  ZoneMap zone_map(/*data=*/{1, 1, 2, 2}, /*num_rows_per_stripe=*/2);

  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/1));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/2));

  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/0));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/2));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/1));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/3));
}

TEST(ZoneMapTest, NullValuesAreIgnored) {
  ZoneMap zone_map(
      /*data=*/{1, Column::kIntNullSentinel, 3, 4, Column::kIntNullSentinel, 6},
      /*num_rows_per_stripe=*/3);

  // Check the first stripe.
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/0));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/2));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/0, /*value=*/4));

  // Check the second stripe.
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/2));
  EXPECT_TRUE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/5));
  EXPECT_FALSE(zone_map.StripeContains(/*stripe_id=*/1, /*value=*/7));
}

}  // namespace ci
