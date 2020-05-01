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
// File: xor_filter_test.cc
// -----------------------------------------------------------------------------

#include "xor_filter.h"

#include <cstdint>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ci {

static constexpr size_t kNumKeys = 1000;

TEST(Xor8Test, Contains) {
  Xor8 xor8(/*keys=*/{1, 2, 3, 4});

  EXPECT_TRUE(xor8.Contains(/*key=*/1));
  EXPECT_TRUE(xor8.Contains(/*key=*/2));
  EXPECT_TRUE(xor8.Contains(/*key=*/3));
  EXPECT_TRUE(xor8.Contains(/*key=*/4));
}

TEST(Xor8Test, CheckForLowFalsePositiveProbability) {
  std::vector<uint64_t> keys;
  keys.reserve(kNumKeys);
  for (uint64_t i = 0; i < kNumKeys; ++i) keys.push_back(i);
  Xor8 xor8(keys);
  for (const uint64_t key : keys) EXPECT_TRUE(xor8.Contains(key));

  size_t num_false_positives = 0;
  for (uint64_t i = kNumKeys; i < 2 * kNumKeys; ++i)
    num_false_positives += xor8.Contains(i);
  const double false_positive_probability =
      static_cast<double>(num_false_positives) / kNumKeys;
  EXPECT_LT(false_positive_probability, 0.01);
}

}  // namespace ci
