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
// File: data.cc
// -----------------------------------------------------------------------------

#include <random>

#include "data.h"

namespace ci {

const int Column::kIntNullSentinel;
const char* const Column::kStringNullSentinel;

std::unique_ptr<Table> GenerateUniformData(const size_t generate_num_values,
                                           const size_t num_unique_values) {
  std::mt19937 gen(42);

  // Generate unique values.
  std::unordered_set<int> set;
  set.reserve(num_unique_values);
  std::uniform_int_distribution<int>
      d_int(std::numeric_limits<int>::min(), std::numeric_limits<int>::max());
  while (set.size() < num_unique_values) {
    set.insert(d_int(gen));
  }
  // Copy to vector.
  std::vector<int> unique_values(set.begin(), set.end());

  // Draw each unique value once to ensure `num_unique_values`. Without
  // this, we might miss out on certain unique values.
  std::vector<int> values(unique_values.begin(), unique_values.end());

  // Fill up remaining values by drawing random `unique_values`.
  values.reserve(generate_num_values);
  std::uniform_int_distribution<size_t> d_unique(0, unique_values.size() - 1);
  while (values.size() < generate_num_values) {
    values.push_back(unique_values[d_unique(gen)]);
  }

  // Shuffle resulting vector to avoid skew.
  std::shuffle(values.begin(), values.end(), gen);

  // Create column & return table.
  ColumnPtr column = ci::Column::IntColumn(absl::StrCat("uni_",
                                                        generate_num_values
                                                            / 1000,
                                                        "K_val_",
                                                        num_unique_values,
                                                        "_uniq"), values);
  std::vector<std::unique_ptr<ci::Column>> columns;
  columns.push_back(std::move(column));
  return ci::Table::Create(/*name=*/"", std::move(columns));
}

}  // namespace ci
