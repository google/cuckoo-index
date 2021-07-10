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
// File: evaluation_utils.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_EVALUATION_UTILS_H_
#define CUCKOO_INDEX_EVALUATION_UTILS_H_

#include <memory>
#include <string>

#include "absl/memory/memory.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "common/bitmap.h"
#include "evaluation.pb.h"
#include "roaring/roaring.h"

using namespace roaring; 

namespace ci {

// Writes `evaluation_results` to a CSV file at the given `path`.
void WriteToCsv(const std::string path,
                const std::vector<EvaluationResults>& evaluation_results);

// Compresses the given bytes `in`.
std::string Compress(absl::string_view in);

// Uncompresses the given bytes `in` (which were compressed with ZSTD).
std::string Uncompress(absl::string_view in);

// Serializes the given `bitmap` to a string.
std::string SerializeBitmap(const Bitmap64& bitmap);

// Returns a bitmap with the given `bits`. bits[i] can be 0 or 1 and determines
// whether the i-th bit is set. For example, CreateBitmap({1,0}) would return a
// two-bit bitmap with the first one being set.
Bitmap64 CreateBitmap(absl::Span<const int> bits);

// Returns the density d of the given `bitmap`. d is the share of 1 bits (e.g.,
// 0.1 means that 10% of the bits are set).
double GetBitmapDensity(const Bitmap64& bitmap);

// Returns the clustering factor f of the given `bitmap`. f is the average
// length of all 1-fills (i.e., consecutive 1s).
double GetBitmapClustering(const Bitmap64& bitmap);

// Returns a Roaring bitmap for the given `bitmap`.
Roaring ToRoaring(const Bitmap64& bitmap);

// Returns the number of Bitmap64Ptrs that are not set to nullptr.
size_t GetNumBitmaps(const std::vector<Bitmap64Ptr>& bitmaps);

// Returns byte size assuming bitpacked `bitmaps`.
size_t GetBitmapsByteSize(const std::vector<Bitmap64Ptr>& bitmaps,
                          const size_t num_stripes);

// Returns a bitmap that encompasses all individual `bitmaps` (back-to-back).
Bitmap64 GetGlobalBitmap(const std::vector<Bitmap64Ptr>& bitmaps);

// Returns stats for the given `bitmaps`. Individual entries may be empty (i.e.,
// set to nullptr) and will be skipped.
::ci::BitmapStats GetBitmapStats(const std::vector<Bitmap64Ptr>& bitmaps,
                                 const size_t num_stripes);

// Prints stats for the given `bitmaps`. Individual entries may be empty (i.e.,
// set to nullptr) and will be skipped.
void PrintBitmapStats(const std::vector<Bitmap64Ptr>& bitmaps,
                      const size_t num_stripes);

// Writes `bitmap` to a file at the given `path`.
void WriteBitmapToFile(const std::string& path, const Bitmap64& bitmap);

// Returns a bitmap read from a file at the given `path`.
Bitmap64 ReadBitmapFromFile(const std::string& path);

}  // namespace ci

#endif  // CUCKOO_INDEX_EVALUATION_UTILS_H_
