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
// File: zone_map.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_ZONE_MAP_H_
#define CUCKOO_INDEX_ZONE_MAP_H_

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <limits>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "data.h"
#include "evaluation_utils.h"
#include "index_structure.h"

namespace ci {

class ZoneMap : public IndexStructure {
 public:
  ZoneMap(const std::vector<int>& data, std::size_t num_rows_per_stripe) {
    if (data.size() % num_rows_per_stripe != 0) {
      std::cout << "WARNING: Number of values is not a multiple of "
                   "`num_rows_per_stripe`. Ignoring last stripe."
                << std::endl;
    }
    num_stripes_ = data.size() / num_rows_per_stripe;
    minimums_.reserve(num_stripes_);
    maximums_.reserve(num_stripes_);
    for (size_t stripe_id = 0; stripe_id < num_stripes_; ++stripe_id) {
      const std::size_t stripe_begin = num_rows_per_stripe * stripe_id;
      const std::size_t stripe_end = stripe_begin + num_rows_per_stripe;

      int per_stripe_min = std::numeric_limits<int>::max();
      int per_stripe_max = std::numeric_limits<int>::min();

      for (size_t row_id = stripe_begin; row_id < stripe_end; ++row_id) {
        if (data[row_id] == Column::kIntNullSentinel) continue;

        per_stripe_min = std::min(per_stripe_min, data[row_id]);
        per_stripe_max = std::max(per_stripe_max, data[row_id]);
      }

      minimums_.push_back(per_stripe_min);
      maximums_.push_back(per_stripe_max);
    }
  }

  ZoneMap(const Column& column, std::size_t num_rows_per_stripe)
      : ZoneMap(column.data(), num_rows_per_stripe) {}

  void PrintZones() {
    for (size_t i = 0; i < num_stripes_; ++i) {
      std::cout << "Stripe " << i << ": " << minimums_[i] << ", "
                << maximums_[i] << std::endl;
    }
  }

  bool StripeContains(std::size_t stripe_id, int value) const override {
    if (stripe_id >= num_stripes_) {
      std::cerr << "`stripe_id` is out of bounds." << std::endl;
      exit(EXIT_FAILURE);
    }
    return value >= minimums_[stripe_id] && value <= maximums_[stripe_id];
  }

  std::string name() const override { return "ZoneMap"; }

  size_t byte_size() const override {
    return sizeof(int) * minimums_.size() + sizeof(int) * maximums_.size();
  }

  size_t compressed_byte_size() const override {
    std::string data;
    absl::StrAppend(&data, absl::string_view(
                               reinterpret_cast<const char*>(minimums_.data()),
                               sizeof(minimums_[0]) * minimums_.size()));
    absl::StrAppend(&data, absl::string_view(
                               reinterpret_cast<const char*>(maximums_.data()),
                               sizeof(maximums_[0]) * maximums_.size()));

    return Compress(data).size();
  }

  std::size_t num_stripes() { return num_stripes_; }

 private:
  std::size_t num_stripes_;
  std::vector<int> minimums_, maximums_;
};

class ZoneMapFactory : public IndexStructureFactory {
 public:
  std::unique_ptr<IndexStructure> Create(
      const Column& column, size_t num_rows_per_stripe) const override {
    return absl::make_unique<ZoneMap>(column.data(), num_rows_per_stripe);
  }

  std::string index_name() const { return "ZoneMap"; }
};

}  // namespace ci

#endif  // CUCKOO_INDEX_ZONE_MAP_H_
