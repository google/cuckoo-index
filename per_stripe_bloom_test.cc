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
// File: per_stripe_bloom_test.cc
// -----------------------------------------------------------------------------

#include "per_stripe_bloom.h"

#include "gtest/gtest.h"

namespace ci {

TEST(PerStripeBloomTest, StripeContains) {
  PerStripeBloom per_stripe_bloom(
      /*data=*/{1, 2, 3, 4}, /*num_rows_per_stripe=*/2,
      /*num_bits_per_key=*/10);

  EXPECT_TRUE(per_stripe_bloom.StripeContains(/*stripe_id=*/0, /*value=*/1));
  EXPECT_TRUE(per_stripe_bloom.StripeContains(/*stripe_id=*/0, /*value=*/2));
  EXPECT_TRUE(per_stripe_bloom.StripeContains(/*stripe_id=*/1, /*value=*/3));
  EXPECT_TRUE(per_stripe_bloom.StripeContains(/*stripe_id=*/1, /*value=*/4));
}

}  // namespace ci
