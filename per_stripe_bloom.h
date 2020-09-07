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
// File: per_stripe_bloom.h
// -----------------------------------------------------------------------------

#ifndef CI_PER_STRIPE_BLOOM_H_
#define CI_PER_STRIPE_BLOOM_H_

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <vector>

#include "absl/strings/str_cat.h"
#include "data.h"
#include "evaluation_utils.h"
#include "index_structure.h"
#include "leveldb/filter_policy.h"
#include "leveldb/slice.h"

namespace ci {

class PerStripeBloom : public IndexStructure {
 public:
  PerStripeBloom(const std::vector<int>& data, std::size_t num_rows_per_stripe,
                 std::size_t num_bits_per_key)
      : num_bits_per_key_(num_bits_per_key),
        policy_(leveldb::NewBloomFilterPolicy(num_bits_per_key_)) {
    num_stripes_ = data.size() / num_rows_per_stripe;
    // Pre-allocate `num_stripes_` strings to store filters.
    filters_.resize(num_stripes_);
    for (size_t stripe_id = 0; stripe_id < num_stripes_; ++stripe_id) {
      const std::size_t stripe_begin = num_rows_per_stripe * stripe_id;
      const std::size_t stripe_end = stripe_begin + num_rows_per_stripe;
      const std::size_t num_values = stripe_end - stripe_begin;
      // Collect values of current stripe. (LevelDB's Bloom filter only accepts
      // leveldb::Slice.)
      absl::flat_hash_set<std::string> string_values;
      string_values.reserve(num_values);
      for (size_t i = stripe_begin; i < stripe_end; ++i) {
        string_values.insert(std::to_string(data[i]));
      }

      // Create the filter for the current stripe.
      const std::vector<leveldb::Slice> slices(string_values.begin(),
                                               string_values.end());
      policy_->CreateFilter(slices.data(), static_cast<int>(slices.size()),
                            &filters_[stripe_id]);
    }
  }

  bool StripeContains(std::size_t stripe_id, int value) const override {
    if (stripe_id >= num_stripes_) {
      std::cerr << "`stripe_id` is out of bounds." << std::endl;
      exit(EXIT_FAILURE);
    }
    // Probe corresponding filter.
    return policy_->KeyMayMatch(leveldb::Slice(std::to_string(value)),
                                filters_[stripe_id]);
  }

  std::string name() const override {
    return std::string("PerStripeBloom/") + std::to_string(num_bits_per_key_);
  }

  size_t byte_size() const override {
    std::size_t result = 0;
    for (const std::string filter : filters_) {
      result += filter.size();
    }
    return result;
  }

  size_t compressed_byte_size() const override {
    std::string data;
    for (const std::string& filter : filters_) {
      absl::StrAppend(&data, filter);
    }

    return Compress(data).size();
  }

  std::size_t num_stripes() { return num_stripes_; }

 private:
  std::size_t num_stripes_;
  std::size_t num_bits_per_key_;
  std::unique_ptr<const leveldb::FilterPolicy> policy_;
  std::vector<std::string> filters_;
};

class PerStripeBloomFactory : public IndexStructureFactory {
 public:
  explicit PerStripeBloomFactory(size_t num_bits_per_key)
      : num_bits_per_key_(num_bits_per_key) {}
  std::unique_ptr<IndexStructure> Create(
      const Column& column, size_t num_rows_per_stripe) const override {
    return absl::make_unique<PerStripeBloom>(column.data(), num_rows_per_stripe,
                                             num_bits_per_key_);
  }

  std::string index_name() const override {
    return std::string("PerStripeBloom/") + std::to_string(num_bits_per_key_);
  }

  const size_t num_bits_per_key_;
};

// A version of `PerStripeBloom` factory that allows to pass another filter's
// factory in order to build a Bloom filter of a comparable size. This is
// useful, e.g. when trying to compare scan rates of filters with roughly the
// same size.
class PerStripeBloomComparableSizeFactory : public IndexStructureFactory {
 public:
  explicit PerStripeBloomComparableSizeFactory(
      std::unique_ptr<IndexStructureFactory> other_index_factory)
      : other_index_factory_(std::move(other_index_factory)) {}
  std::unique_ptr<IndexStructure> Create(
      const Column& column, size_t num_rows_per_stripe) const override {
    constexpr size_t kMaxBitsPerKey = 20;

    IndexStructurePtr other_index =
        other_index_factory_->Create(column, num_rows_per_stripe);
    const size_t target_size = other_index->byte_size();

    // Find the number of bits per key that minimizes the difference in size
    // between the filters.
    size_t argmin_num_bits_per_key;
    size_t min_size_diff = std::numeric_limits<size_t>::max();
    size_t min_bits = 1;
    size_t max_bits = kMaxBitsPerKey;
    while (min_bits <= max_bits) {
      const size_t num_bits_per_key = (min_bits + max_bits) / 2;

      auto bloom_index = absl::make_unique<PerStripeBloom>(
          column.data(), num_rows_per_stripe, num_bits_per_key);
      size_t abs_size_diff = target_size < bloom_index->byte_size()
                                 ? bloom_index->byte_size() - target_size
                                 : target_size - bloom_index->byte_size();

      if (abs_size_diff < min_size_diff) {
        min_size_diff = abs_size_diff;
        argmin_num_bits_per_key = num_bits_per_key;
      }

      if (target_size < bloom_index->byte_size()) {
        max_bits = num_bits_per_key - 1;
      } else {
        min_bits = num_bits_per_key + 1;
      }
    }

    return absl::make_unique<PerStripeBloom>(column.data(), num_rows_per_stripe,
                                             argmin_num_bits_per_key);
  }

  std::string index_name() const override {
    return std::string("PerStripeBloomComparableSize/" +
                       other_index_factory_->index_name());
  }

  std::unique_ptr<IndexStructureFactory> other_index_factory_;
};

}  // namespace ci

#endif  // CI_PER_STRIPE_BLOOM_H_
