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
// File: index_structure.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_INDEX_STRUCTURE_H_
#define CUCKOO_INDEX_INDEX_STRUCTURE_H_

#include <cstddef>
#include <memory>
#include <string>

#include "data.h"
#include "evaluation.pb.h"

namespace ci {

// Class representing an index structure, e.g. based on a Bloom filter.
class IndexStructure {
 public:
  IndexStructure() {}
  virtual ~IndexStructure() {}

  // Returns true if the stripe with `stripe_id` contains the given `value`.
  virtual bool StripeContains(size_t stripe_id, int value) const = 0;

  // Returns a bitmap indicating possibly qualifying stripes for the given
  // `value`. Probes up to `num_stripes` stripes.
  // Note: classes extending IndexStructure can override this method when they
  // can provide an optimized approach here (see CuckooIndex for an example).
  virtual Bitmap64 GetQualifyingStripes(int value, size_t num_stripes) const {
    // Default implementation for per-stripe index structures.
    Bitmap64 result(/*size=*/num_stripes);
    for (size_t stripe_id = 0; stripe_id < static_cast<size_t>(num_stripes);
         ++stripe_id) {
      if (StripeContains(stripe_id, value))
        result.Set(stripe_id, true);
    }
    return result;
  }

  // Returns the name of the index structure.
  virtual std::string name() const = 0;

  // Returns the in-memory size of the index structure.
  virtual size_t byte_size() const = 0;

  // Returns the in-memory size of the compressed index structure.
  virtual size_t compressed_byte_size() const = 0;

  // Returns statistics about internal data structures using bitmaps. Should
  // be implemented only by CLT-based index structures.
  virtual ci::BitmapStats bitmap_stats() { return ci::BitmapStats(); }
};

using IndexStructurePtr = std::unique_ptr<IndexStructure>;

class IndexStructureFactory {
 public:
  IndexStructureFactory() {}
  virtual ~IndexStructureFactory() {}

  // Creates an index structure for the given `column`.
  virtual IndexStructurePtr Create(const Column& column,
                                   size_t num_rows_per_stripe) const = 0;

  // Returns the name of the index that can be created using the factory.
  virtual std::string index_name() const = 0;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_INDEX_STRUCTURE_H_
