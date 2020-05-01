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
// File: xor_filter.h
// -----------------------------------------------------------------------------

#ifndef CI_XOR_FILTER_H_
#define CI_XOR_FILTER_H_

#include <iostream>
#include <vector>

#include "absl/strings/str_cat.h"
#include "xor_singleheader/include/xorfilter.h"

namespace ci {

// A wrapper for the Xor filter with 8-bit fingerprints. This filter uses close
// to 10 bits per element and achieves a false positive probability of ~0.3%.
//
// Adapted from https://github.com/FastFilter/xor_singleheader.
class Xor8 {
 public:
  // `keys` has to be duplicate free.
  explicit Xor8(const std::vector<uint64_t>& keys) {
    if (!xor8_allocate(keys.size(), &filter_)) {
      std::cerr << "Couldn't allocate Xor filter." << std::endl;
      exit(EXIT_FAILURE);
    }
    if (!xor8_buffered_populate(reinterpret_cast<const uint64_t*>(keys.data()),
                                keys.size(), &filter_)) {
      std::cerr << "Couldn't populate Xor filter." << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  // Forbid copying and moving.
  Xor8(const Xor8&) = delete;
  Xor8& operator=(const Xor8&) = delete;
  Xor8(Xor8&&) = delete;
  Xor8& operator=(Xor8&&) = delete;

  ~Xor8() { xor8_free(&filter_); }

  inline bool Contains(const uint64_t key) const {
    return xor8_contain(key, &filter_);
  }

  std::string Data() const {
    std::string data;
    absl::StrAppend(&data, filter_.seed);
    absl::StrAppend(&data, filter_.blockLength);
    const size_t num_fingerprints = 3 * filter_.blockLength;
    for (size_t i = 0; i < num_fingerprints; ++i)
      absl::StrAppend(&data, filter_.fingerprints[i]);
    return data;
  }

  size_t SizeInBytes() const { return xor8_size_in_bytes(&filter_); }

 private:
  xor8_s filter_;
};

}  // namespace ci

#endif  // CI_XOR_FILTER_H_
