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
// File: cuckoo_index.cc
// -----------------------------------------------------------------------------

#include "cuckoo_index.h"

#include <cmath>
#include <cstddef>
#include <iostream>
#include <string>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "common/byte_coding.h"
#include "common/rle_bitmap.h"
#include "cuckoo_kicker.h"
#include "cuckoo_utils.h"
#include "evaluation_utils.h"
#include "fingerprint_store.h"

namespace ci {
namespace {

// When distributing values to buckets fails (see Distribute(..), below),
// increase the number of requested buckets by this factor.
constexpr double kNumBucketsGrowFactor = 1.01;

// Returns a map from values to their stripe-bitmaps.
const absl::flat_hash_map<int, Bitmap64Ptr> ValueToStripeBitmaps(
    const Column& column, size_t num_rows_per_stripe) {
  absl::flat_hash_map<int, Bitmap64Ptr> bitmaps;
  // Round down the number of rows to the next multiple of
  // `num_rows_per_stripe`, i.e., ignore the last stripe as elsewhere.
  const size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  const size_t num_rows = num_stripes * num_rows_per_stripe;
  for (size_t row = 0; row < num_rows; ++row) {
    const int value = column[row];
    if (!bitmaps.contains(value))
      bitmaps[value] = absl::make_unique<Bitmap64>(/*size=*/num_stripes);
    bitmaps[value]->Set(row / num_rows_per_stripe, true);
  }
  return bitmaps;
}

// Distributes the `values` to buckets with the "kicking algorithm". Returns an
// empty vector if this failed, i.e., if there were too few buckets.
std::vector<Bucket> DistributeByKicking(size_t num_buckets,
                                        size_t slots_per_bucket,
                                        const std::vector<CuckooValue>& values,
                                        bool skew_kicking) {
  std::vector<Bucket> buckets;
  buckets.reserve(num_buckets);
  for (size_t i = 0; i < num_buckets; ++i)
    buckets.push_back(Bucket(/*num_slots=*/slots_per_bucket));

  // Try to insert values with kicking.
  CuckooKicker kicker(slots_per_bucket, absl::MakeSpan(buckets), skew_kicking);
  const bool success = kicker.InsertValues(values);
  kicker.PrintStats();
  if (!success) return std::vector<Bucket>();

  // Look up values and set Bucket::kicked accordingly (such that `kicked`
  // contains all values that reside in their secondary bucket).
  for (const CuckooValue& value : values) {
    bool in_primary;
    LookupValueInBuckets(buckets, value, &in_primary);
    if (!in_primary) buckets[value.primary_bucket].kicked_.push_back(value);
  }

  return buckets;
}

// Distributes the `distinct_values` to buckets. Returns an empty vector if this
// failed, i.e., if there were too few buckets.
std::vector<Bucket> Distribute(
    size_t num_buckets, size_t slots_per_bucket, CuckooAlgorithm cuckoo_alg,
    const absl::flat_hash_set<int>& distinct_values) {
  std::vector<CuckooValue> values;
  values.reserve(distinct_values.size());
  for (size_t value : distinct_values)
    values.push_back(CuckooValue(value, num_buckets));
  switch (cuckoo_alg) {
    case CuckooAlgorithm::KICKING:
      return DistributeByKicking(num_buckets, slots_per_bucket, values,
                                 /*skew_kicking=*/false);
    case CuckooAlgorithm::SKEWED_KICKING:
      return DistributeByKicking(num_buckets, slots_per_bucket, values,
                                 /*skew_kicking=*/true);
    case CuckooAlgorithm::MATCHING:
      std::cerr << "Not implemented." << std::endl;
      std::exit(-1);
  }

  std::cerr << "Unknown algorithm: " << static_cast<int32_t>(cuckoo_alg)
            << std::endl;
  std::exit(-1);
}

// Computes the minimum `num_bits` which can be used per bucket and fills
// `slot_fingerprints` accordingly, also moves the `value_to_bitmap` entries to
// the corresponding `slot_bitmaps` entries.
void CreateSlots(double scan_rate, size_t slots_per_bucket,
                 const std::vector<Bucket>& buckets,
                 absl::flat_hash_map<int, Bitmap64Ptr>* value_to_bitmap,
                 std::vector<Fingerprint>* slot_fingerprints,
                 const bool prefix_bits_optimization,
                 Bitmap64Ptr* use_prefix_bits_bitmap,
                 std::vector<Bitmap64Ptr>* slot_bitmaps) {
  const size_t num_buckets = buckets.size();
  const size_t num_slots = num_buckets * slots_per_bucket;
  size_t num_empty_buckets = 0;
  for (const Bucket& b : buckets)
    if (b.slots_.empty()) ++num_empty_buckets;
  const double bucket_density =
      1.0 - static_cast<double>(num_empty_buckets) / num_buckets;

  slot_fingerprints->resize(num_slots);
  if (prefix_bits_optimization)
    *use_prefix_bits_bitmap = absl::make_unique<Bitmap64>(num_buckets);
  slot_bitmaps->resize(num_slots);
  for (size_t bucket_id = 0; bucket_id < num_buckets; ++bucket_id) {
    const Bucket& bucket = buckets[bucket_id];

    // Start by determining the minimum number of bits needed to avoid
    // collisions of values which are contained in the bucket or were kicked
    // from this bucket (which was their primary bucket).
    std::vector<uint64_t> possibly_colliding_fingerprints;
    for (const CuckooValue& value : bucket.slots_)
      possibly_colliding_fingerprints.push_back(value.fingerprint);
    for (const CuckooValue& value : bucket.kicked_)
      possibly_colliding_fingerprints.push_back(value.fingerprint);
    bool use_prefix_bits;
    size_t num_bits;
    if (prefix_bits_optimization) {
      num_bits = GetMinCollisionFreeFingerprintPrefixOrSuffix(
          possibly_colliding_fingerprints, &use_prefix_bits);
      (*use_prefix_bits_bitmap)->Set(bucket_id, use_prefix_bits);
    } else {
      num_bits = GetMinCollisionFreeFingerprintLength(
          possibly_colliding_fingerprints, /*use_prefix_bits=*/false);
    }

    // Now add more bits if needed to ensure the desired `scan_rate`.
    for (; num_bits <= 65; ++num_bits) {
      // Compute `actual_scan_rate` of `bucket` by averaging the
      // local scan rates of all items in `bucket`. The intuition here is that a
      // lookup can only match with a single fingerprint & that for an infinite
      // number of lookups we expect the scan rate to average out.
      const double fp_prob = 1.0 / std::pow(2, num_bits);
      double sum_scan_rate = 0.0;
      for (const CuckooValue& value : bucket.slots_) {
        const Bitmap64Ptr& bitmap = (*value_to_bitmap)[value.orig_value];
        assert(bitmap != nullptr);
        sum_scan_rate += (fp_prob * bitmap->GetOnesCount()) / bitmap->bits();
      }
      double actual_scan_rate = sum_scan_rate / bucket.slots_.size();
      // Adjust the scan rate by: 1) taking the density (aka load-factor) into
      // account and 2) taking into account that for every lookup we may
      // actually check two buckets: the primary and the secondary.
      actual_scan_rate *= bucket_density * 2;
      if (actual_scan_rate <= scan_rate) break;
    }
    // Check if can reach the desired scan rate.
    assert(num_bits != 65);

    // We have successfully determined `num_bits` => set the actual slots.
    for (size_t i = 0; i < slots_per_bucket; ++i) {
      const size_t slot = bucket_id * slots_per_bucket + i;
      Fingerprint& fp = (*slot_fingerprints)[slot];
      if (i >= bucket.slots_.size()) {
        fp.active = false;
        fp.num_bits = 0;
        fp.fingerprint = 0ULL;
      } else {
        fp.active = true;
        fp.num_bits = num_bits;
        fp.fingerprint =
            prefix_bits_optimization && use_prefix_bits
                ? GetFingerprintPrefix(bucket.slots_[i].fingerprint, num_bits)
                : GetFingerprintSuffix(bucket.slots_[i].fingerprint, num_bits);
        (*slot_bitmaps)[slot] =
            std::move((*value_to_bitmap)[bucket.slots_[i].orig_value]);
      }
    }
  }
}

// Returns the fingerprints and bitmaps encoded in a compact manner.
std::string Encode(const FingerprintStore& fingerprint_store,
                   const size_t slots_per_bucket,
                   const bool prefix_bits_optimization,
                   const Bitmap64Ptr& prefix_bits_bitmap,
                   const std::vector<Bitmap64Ptr>& slot_bitmaps) {
  ByteBuffer result;
  PutString(fingerprint_store.Encode(), &result);
  const size_t fp_size = result.pos();
  std::cout << "Encoded fingerprints: " << fp_size << std::endl;

  // Flag that denotes whether we use the prefix bits optimization. If set, the
  // flag is followed by the prefix bits bitmap.
  PutPrimitive(true, &result);
  if (prefix_bits_optimization) {
    // Encode prefix bits bitmap as RleBitmap (util::bitmap::DenseEncode() needs
    // significantly more space in sparse cases).
    const RleBitmap rle_bitmap(*prefix_bits_bitmap);
    PutString(rle_bitmap.data(), &result);
    std::cout << "Encoded prefix bits bitmap: " << result.pos() - fp_size
              << std::endl;
  }

  // Add the global bitmap, encoded as RleBitmap.
  const RleBitmap bitmap(GetGlobalBitmap(slot_bitmaps));
  const size_t before_global_bitmap = result.pos();
  PutString(bitmap.data(), &result);
  std::cout << "Encoded bitmaps: " << result.pos() - before_global_bitmap
            << std::endl;
  return std::string(result.data(), result.pos());
}

}  // namespace

bool CuckooIndex::StripeContains(size_t stripe_id, int value) const {
  const CuckooValue val(value, num_buckets_);
  size_t slot;
  if (!BucketContains(val.primary_bucket, val.fingerprint, &slot)) {
    if (!BucketContains(val.secondary_bucket, val.fingerprint, &slot))
      return false;
  }
  assert(slot_bitmaps_[slot] != nullptr);
  return slot_bitmaps_[slot]->Get(stripe_id);
}

Bitmap64 CuckooIndex::GetQualifyingStripes(int value,
                                           size_t num_stripes) const {
  const CuckooValue val(value, num_buckets_);
  size_t slot;
  if (!BucketContains(val.primary_bucket, val.fingerprint, &slot)) {
    if (!BucketContains(val.secondary_bucket, val.fingerprint, &slot)) {
      // Not found. Return an empty bitmap.
      return Bitmap64(/*size=*/num_stripes);
    }
  }
  assert(slot_bitmaps_[slot] != nullptr);
  const Bitmap64& bitmap = *slot_bitmaps_[slot];
  return Bitmap64(bitmap);
}

bool CuckooIndex::BucketContains(size_t bucket, uint64_t fingerprint,
                                 size_t* slot) const {
  const bool use_prefix_bits = use_prefix_bits_bitmap_ == nullptr
                                   ? false
                                   : use_prefix_bits_bitmap_->Get(bucket);
  for (*slot = bucket * slots_per_bucket_;
       *slot < (bucket + 1) * slots_per_bucket_; ++(*slot)) {
    const Fingerprint& fp = fingerprint_store_->GetFingerprint(*slot);
    if (!fp.active) continue;
    if (use_prefix_bits) {
      if (fp.fingerprint == GetFingerprintPrefix(fingerprint, fp.num_bits))
        return true;
    } else {
      if (fp.fingerprint == GetFingerprintSuffix(fingerprint, fp.num_bits))
        return true;
    }
  }
  return false;
}

std::unique_ptr<IndexStructure> CuckooIndexFactory::Create(
    const Column& column, size_t num_rows_per_stripe) const {
  absl::flat_hash_map<int, Bitmap64Ptr> value_to_bitmap =
      ValueToStripeBitmaps(column, num_rows_per_stripe);
  // Fetch the set of distinct-values in `value_to_bitmap`. Note that this is
  // not necessarily the same as column.distinct_values(), since rows may have
  // been dropped at the end (so each stripe has the same size).
  absl::flat_hash_set<int> distinct_values;
  distinct_values.reserve(value_to_bitmap.size());
  for (const auto& [value, _] : value_to_bitmap) distinct_values.insert(value);

  size_t num_buckets = GetMinNumBuckets(distinct_values.size(),
                                        slots_per_bucket_, max_load_factor_);

  std::vector<Bucket> buckets;
  while (buckets.empty()) {
    std::cout << "Attempting to distribute " << distinct_values.size()
              << " values to " << num_buckets << " buckets with "
              << slots_per_bucket_ << " slots each. I.e., load-factor: "
              << static_cast<double>(distinct_values.size()) /
                     (slots_per_bucket_ * num_buckets)
              << std::endl;
    buckets = Distribute(num_buckets, slots_per_bucket_, cuckoo_alg_,
                         distinct_values);
    num_buckets =
        std::max(static_cast<size_t>(num_buckets * kNumBucketsGrowFactor),
                 num_buckets + 1);
  }

  std::vector<Fingerprint> slot_fingerprints;
  Bitmap64Ptr use_prefix_bits_bitmap;
  std::vector<Bitmap64Ptr> slot_bitmaps;
  CreateSlots(scan_rate_, slots_per_bucket_, buckets, &value_to_bitmap,
              &slot_fingerprints, prefix_bits_optimization_,
              &use_prefix_bits_bitmap, &slot_bitmaps);
  auto fingerprint_store = absl::make_unique<FingerprintStore>(
      slot_fingerprints, slots_per_bucket_,
      /*use_rle_to_encode_block_bitmaps=*/false);
  const std::string data =
      Encode(*fingerprint_store, slots_per_bucket_, prefix_bits_optimization_,
             use_prefix_bits_bitmap, slot_bitmaps);

  // Need to use WrapUnique<>(..) since we're calling a private c'tor.
  return absl::WrapUnique<CuckooIndex>(new CuckooIndex(
      absl::StrCat("CuckooIndex:", cuckoo_alg_, ":", max_load_factor_, ":",
                   scan_rate_),
      slots_per_bucket_, std::move(fingerprint_store),
      std::move(use_prefix_bits_bitmap), std::move(slot_bitmaps), data.size(),
      Compress(data).size()));
}

}  // namespace ci
