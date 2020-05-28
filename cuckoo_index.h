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
// File: cuckoo_index.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_CUCKOO_INDEX_H_
#define CUCKOO_INDEX_CUCKOO_INDEX_H_

#include "absl/container/flat_hash_map.h"
#include "cuckoo_utils.h"
#include "fingerprint_store.h"
#include "index_structure.h"

namespace ci {

class CuckooIndex : public IndexStructure {
 public:
  bool StripeContains(size_t stripe_id, int value) const override;

  Bitmap64 GetQualifyingStripes(int value, int num_stripes) const override;

  std::string name() const override { return name_; }

  // Returns the in-memory size of the index structure.
  size_t byte_size() const override { return byte_size_; }

  // Returns the in-memory size of the compressed index structure.
  size_t compressed_byte_size() const override { return compressed_byte_size_; }

  size_t active_slots() const {
    size_t active_slots = 0;
    for (size_t i = 0; i < fingerprint_store_->num_slots(); ++i) {
      if (fingerprint_store_->GetFingerprint(i).active) ++active_slots;
    }
    return active_slots;
  }

 private:
  friend class CuckooIndexFactory;

  CuckooIndex(std::string name, size_t slots_per_bucket,
              std::unique_ptr<FingerprintStore> fingerprint_store,
              Bitmap64Ptr use_prefix_bits_bitmap,
              std::vector<Bitmap64Ptr> slot_bitmaps, size_t byte_size,
              size_t compressed_byte_size)
      : name_(name),
        num_buckets_(fingerprint_store->num_slots() / slots_per_bucket),
        slots_per_bucket_(slots_per_bucket),
        fingerprint_store_(std::move(fingerprint_store)),
        use_prefix_bits_bitmap_(std::move(use_prefix_bits_bitmap)),
        slot_bitmaps_(std::move(slot_bitmaps)),
        byte_size_(byte_size),
        compressed_byte_size_(compressed_byte_size) {
    assert(fingerprint_store_->num_slots() % slots_per_bucket_ == 0);
  }

  // Returns true if the given bucket contains the fingerprint (taking only
  // the relevant bits into account). In case it does, `slot` is set to the
  // slot which contains it (one of the `slots_per_bucket_` possible ones).
  bool BucketContains(size_t bucket, uint64_t fingerprint, size_t* slot) const;

  const std::string name_;
  const size_t num_buckets_;
  const size_t slots_per_bucket_;

  const std::unique_ptr<FingerprintStore> fingerprint_store_;
  // Indicates for every bucket whether prefix or suffix bits of hash
  // fingerprints were used.
  const Bitmap64Ptr use_prefix_bits_bitmap_;
  // For each *active* slot its Bitmap, nullptr otherwise.
  const std::vector<Bitmap64Ptr> slot_bitmaps_;

  // The sizes of the encoded data-structures.
  // TODO: after fine-tuning the encodings, actually store the encoded
  // data-structures and add methods which serialize / deserialize for testing
  // purposes. As follow-up also do the lookups based on these data-structures
  // instead of the expanded Fingerprints and Bitmaps above.
  const size_t byte_size_;
  const size_t compressed_byte_size_;
};

// How the distribution of values to their primary / secondary bucket is chosen:
// "Classically" by kicking out existing values (KICKING), using a biased coin
// toss during the kicking procedure to increase the ratio of primary-bucket
// placements (SKEWED_KICKING), or by finding an optimal solution via a
// weighted-matching algorithm (MATCHING).
enum class CuckooAlgorithm { KICKING, SKEWED_KICKING, MATCHING };

class CuckooIndexFactory : public IndexStructureFactory {
 public:
  explicit CuckooIndexFactory(CuckooAlgorithm cuckoo_alg,
                              double max_load_factor, double scan_rate,
                              size_t slots_per_bucket,
                              bool prefix_bits_optimization)
      : cuckoo_alg_(cuckoo_alg),
        max_load_factor_(max_load_factor),
        scan_rate_(scan_rate),
        slots_per_bucket_(slots_per_bucket),
        prefix_bits_optimization_(prefix_bits_optimization) {}

  std::unique_ptr<IndexStructure> Create(
      const Column& column, size_t num_rows_per_stripe) const override;

  std::string index_name() const override;

 private:
  const CuckooAlgorithm cuckoo_alg_;
  const double max_load_factor_;
  const double scan_rate_;
  const size_t slots_per_bucket_;
  // If set, dynamically uses either prefix or suffix bits of hash fingerprints
  // on a bucket basis (depending on which of the two requires fewer bits to
  // make fingerprints collision free).
  const bool prefix_bits_optimization_;
};

}  // namespace ci

#endif  // CUCKOO_INDEX_CUCKOO_INDEX_H_
