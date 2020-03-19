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
// File: fingerprint_store_test.cc
// -----------------------------------------------------------------------------

#include "fingerprint_store.h"

#include <string>

#include "absl/types/span.h"
#include "cuckoo_utils.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ci {

constexpr uint32_t kMurmurHashConstant = 0x5bd1e995;
constexpr size_t kNumFingerprints = 1e3;

// **** Helper methods ****

// Creates `n` random fingerprints with different `lengths` such that all
// `slots_per_bucket` fingerprints in a bucket share the same length.
std::vector<Fingerprint> CreateRandomFingerprints(const size_t n,
                                                  const size_t slots_per_bucket,
                                                  std::vector<size_t> lengths) {
  assert(lengths.size() > 0);
  std::sort(lengths.begin(), lengths.end());

  // Insert `length[i]` lengths.size()-i times such that shorter lengths are
  // more likely to be chosen.
  std::vector<size_t> lengths_to_draw_from;
  for (size_t i = 0; i < lengths.size(); ++i) {
    for (size_t j = 0; j < lengths.size() - i; ++j)
      lengths_to_draw_from.push_back(lengths[i]);
  }

  std::vector<Fingerprint> fingerprints;
  fingerprints.reserve(n);
  for (size_t i = 0; i < n; i += slots_per_bucket) {
    const size_t hash_bucket = i * kMurmurHashConstant;
    const size_t num_bits =
        lengths_to_draw_from[hash_bucket % lengths_to_draw_from.size()];

    // Fill all slots.
    for (size_t j = 0; j < slots_per_bucket; ++j) {
      const size_t hash_slot = (i + j) * kMurmurHashConstant;
      Fingerprint fp{/*active=*/(i + j) % 10 == 0 ? false : true, num_bits,
                     /*fingerprint=*/hash_slot % (1 << num_bits)};
      fingerprints.push_back(fp);
    }
  }
  return fingerprints;
}

// **** Test cases ****

// Creates fingerprints with different `lengths`, stores them in a
// FingerprintStore, and calls GetFingerprint(..) on each of them.
void CreateStoreAndGetFingerprints(const std::vector<size_t>& lengths,
                                   const size_t slots_per_bucket,
                                   const bool use_rle_to_encode_block_bitmaps) {
  const std::vector<Fingerprint> fingerprints =
      CreateRandomFingerprints(kNumFingerprints, slots_per_bucket, lengths);
  const FingerprintStore store(fingerprints, slots_per_bucket,
                               use_rle_to_encode_block_bitmaps);
  for (size_t i = 0; i < fingerprints.size(); ++i) {
    const Fingerprint fp = store.GetFingerprint(i);
    ASSERT_EQ(fp.active, fingerprints[i].active);
    if (fp.active) {
      ASSERT_EQ(fp.num_bits, fingerprints[i].num_bits);
      ASSERT_EQ(fp.fingerprint, fingerprints[i].fingerprint);
    }
  }
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintSingleBlock) {
  CreateStoreAndGetFingerprints(/*lengths=*/{8}, /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/false);
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintSingleBlockRLE) {
  CreateStoreAndGetFingerprints(/*lengths=*/{8}, /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/true);
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintFiveBlocks) {
  CreateStoreAndGetFingerprints(/*lengths=*/{1, 2, 4, 8, 16},
                                /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/false);
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintFiveBlocksRLE) {
  CreateStoreAndGetFingerprints(/*lengths=*/{1, 2, 4, 8, 16},
                                /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/true);
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintZeroBits) {
  CreateStoreAndGetFingerprints(/*lengths=*/{0},
                                /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/false);
}

TEST(FingerprintStore, GetFingerprintReturnsCorrectFingerprintZeroAndOneBits) {
  CreateStoreAndGetFingerprints(/*lengths=*/{0, 1},
                                /*slots_per_bucket=*/1,
                                /*use_rle_to_encode_block_bitmaps=*/false);
}

TEST(FingerprintStore,
     GetFingerprintReturnsCorrectFingerprintTwoSlotsPerBucket) {
  CreateStoreAndGetFingerprints(/*lengths=*/{1, 2, 4, 8, 16},
                                /*slots_per_bucket=*/2,
                                /*use_rle_to_encode_block_bitmaps=*/false);
}

}  // namespace ci
