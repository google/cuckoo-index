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
// File: per_stripe_xor.h
// -----------------------------------------------------------------------------

#ifndef CI_PER_STRIPE_XOR_H_
#define CI_PER_STRIPE_XOR_H_

#include <algorithm>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "absl/strings/str_cat.h"
#include "data.h"
#include "evaluation_utils.h"
#include "index_structure.h"
#include "xor_filter.h"

namespace ci {

// Creates one Xor8 filter per stripe.
class PerStripeXor : public IndexStructure {
 public:
  PerStripeXor(const std::vector<int>& data, std::size_t num_rows_per_stripe) {
    num_stripes_ = data.size() / num_rows_per_stripe;
    filters_.reserve(num_stripes_);
    for (size_t stripe_id = 0; stripe_id < num_stripes_; ++stripe_id) {
      const std::size_t stripe_begin = num_rows_per_stripe * stripe_id;
      const std::size_t stripe_end = stripe_begin + num_rows_per_stripe;
      const std::size_t num_values = stripe_end - stripe_begin;
      // Collect values of current stripe.
      absl::flat_hash_set<uint64_t> values;
      values.reserve(num_values);
      for (size_t i = stripe_begin; i < stripe_end; ++i) values.insert(data[i]);

      // Create the filter for the current stripe.
      const std::vector<uint64_t> keys(values.begin(), values.end());
      filters_.push_back(absl::make_unique<Xor8>(keys));
    }
  }

  bool StripeContains(std::size_t stripe_id, int value) const override {
    if (stripe_id >= num_stripes_) {
      std::cerr << "`stripe_id` is out of bounds." << std::endl;
      exit(EXIT_FAILURE);
    }
    // Probe corresponding filter.
    return filters_[stripe_id]->Contains(value);
  }

  std::string name() const override { return std::string("PerStripeXor"); }

  size_t byte_size() const override {
    size_t result = 0;
    for (size_t i = 0; i < filters_.size(); ++i)
      result += filters_[i]->SizeInBytes();
    return result;
  }

  size_t compressed_byte_size() const override {
    std::string data;
    for (size_t i = 0; i < filters_.size(); ++i)
      absl::StrAppend(&data, filters_[i]->Data());
    return Compress(data).size();
  }

  std::size_t num_stripes() { return num_stripes_; }

 private:
  std::size_t num_stripes_;
  std::vector<std::unique_ptr<Xor8>> filters_;
};

class PerStripeXorFactory : public IndexStructureFactory {
 public:
  explicit PerStripeXorFactory() {}
  std::unique_ptr<IndexStructure> Create(
      const Column& column, size_t num_rows_per_stripe) const override {
    return absl::make_unique<PerStripeXor>(column.data(), num_rows_per_stripe);
  }

  std::string index_name() const override {
    return std::string("PerStripeXor");
  }
};

}  // namespace ci

#endif  // CI_PER_STRIPE_XOR_H_
