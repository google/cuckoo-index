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
// File: evaluation_utils.cc
// -----------------------------------------------------------------------------

#include "evaluation_utils.h"

#include <cstddef>
#include <fstream>
#include <numeric>
#include <sstream>

#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "boost/iostreams/device/back_inserter.hpp"
#include "boost/iostreams/filter/zstd.hpp"
#include "boost/iostreams/filtering_stream.hpp"
#include "common/rle_bitmap.h"

namespace ci {

using ::ci::BitmapStats;
using ::ci::EvaluationResults;
using TestCase = ::ci::EvaluationResults::TestCase;

namespace {

const std::vector<std::string> GetResultsHeader() {
  static const auto* header = new std::vector<std::string>(
      {"index_structure:string",             //
       "num_rows_per_stripe:long",           //
       "num_stripes:long",                   //
       "column_name:string",                 //
       "column_type:string",                 //
       "column_cardinality:long",            //
       "column_compressed_size_bytes:long",  //
       "index_size_bytes:long",              //
       "index_compressed_size_bytes:long",   //
       // Bitmap stats.
       "bitmap_density:double",                           //
       "bitmap_clustering:double",                        //
       "bitmap_bitpacked_size:long",                      //
       "bitmap_bitpacked_compressed_size:long",           //
       "bitmap_roaring_size:long",                        //
       "bitmap_roaring_compressed_size:long",             //
       "bitmap_roaring_individual_size:long",             //
       "bitmap_roaring_individual_compressed_size:long",  //
       "bitmap_rle_size:long",                            //
       "bitmap_rle_compressed_size:long",                 //
       // Test cases.
       "test_case_name:string",     //
       "num_lookups:long",          //
       "num_false_positives:long",  //
       "num_true_negatives:long"});

  return *header;
}

// Converts `bitmap` to a Roaring bitmap and populates `size` and
// `compressed_size` for it.
void GetRoaringSize(const Bitmap64& bitmap, int64_t* size,
                    int64_t* compressed_size) {
  const Roaring roaring_bitmap = ToRoaring(bitmap);
  // portable = false may lead to better compression for sparse bitmaps.
  *size = roaring_bitmap.getSizeInBytes(/*portable=*/false);
  std::string serialized_roaring_bitmap(*size, ' ');
  assert(static_cast<size_t>(*size) ==
         roaring_bitmap.write(serialized_roaring_bitmap.data(),
                              /*portable=*/false));
  *compressed_size = Compress(serialized_roaring_bitmap).size();
}

// Converts `bitmap` to an RleBitmap and populates `size` and
// `compressed_size` for it.
void GetRleBitmapSize(const Bitmap64& bitmap, int64_t* size,
                      int64_t* compressed_size) {
  RleBitmap rle_bitmap(bitmap);
  *size = rle_bitmap.data().size();
  *compressed_size = Compress(rle_bitmap.data()).size();
}

}  // namespace

void WriteToCsv(const std::string path,
                const std::vector<EvaluationResults>& evaluation_results) {
  std::ofstream file(path);
  assert(file.is_open());

  file << absl::StrJoin(GetResultsHeader(), ",") << std::endl;

  for (const EvaluationResults& result : evaluation_results) {
    std::vector<std::string> base_row = {
        //absl::CEscape(result.index_structure()),
        result.index_structure(),
        absl::StrCat(result.num_rows_per_stripe()),
        absl::StrCat(result.num_stripes()),
        //absl::CEscape(result.column_name()),
        result.column_name(),
        //absl::CEscape(result.column_type()),
        result.column_type(),
        absl::StrCat(result.column_cardinality()),
        absl::StrCat(result.column_compressed_size_bytes()),
        absl::StrCat(result.index_size_bytes()),
        absl::StrCat(result.index_compressed_size_bytes()),
        // Bitmap stats.
        absl::StrCat(result.bitmap_stats().density()),
        absl::StrCat(result.bitmap_stats().clustering()),
        absl::StrCat(result.bitmap_stats().bitpacked_size()),
        absl::StrCat(result.bitmap_stats().bitpacked_compressed_size()),
        absl::StrCat(result.bitmap_stats().roaring_size()),
        absl::StrCat(result.bitmap_stats().roaring_compressed_size()),
        absl::StrCat(result.bitmap_stats().roaring_individual_size()),
        absl::StrCat(
            result.bitmap_stats().roaring_individual_compressed_size()),
        absl::StrCat(result.bitmap_stats().rle_size()),
        absl::StrCat(result.bitmap_stats().rle_compressed_size()),
    };

    for (const TestCase& test_case : result.test_cases()) {
      std::vector<std::string> test_case_row = base_row;
      test_case_row.insert(test_case_row.end(),
                           {
                               //absl::CEscape(test_case.name()),
                               test_case.name(),
                               absl::StrCat(test_case.num_lookups()),
                               absl::StrCat(test_case.num_false_positives()),
                               absl::StrCat(test_case.num_true_negatives()),
                           });

      file << absl::StrJoin(test_case_row, ",") << std::endl;
    }
  }

  file.close();
}

constexpr uint32_t kZstdLevel = 1;

std::string Compress(absl::string_view in) {
  std::string result;

  namespace bio = boost::iostreams;
  bio::filtering_ostream out;
  out.push(bio::zstd_compressor(bio::zstd_params{.level = kZstdLevel}));
  out.push(bio::back_inserter(result));
  out << in;
  out.flush();

  return result;
}

std::string Uncompress(absl::string_view in) {
  std::string result;

  namespace bio = boost::iostreams;
  bio::filtering_ostream out;
  out.push(bio::zstd_decompressor(bio::zstd_params{.level = kZstdLevel}));
  out.push(bio::back_inserter(result));
  out << in;
  out.flush();

  return result;
}

std::string SerializeBitmap(const Bitmap64& bitmap) {
  std::string result;
  Bitmap64::DenseEncode(bitmap, &result);
  return result;
}

Bitmap64 CreateBitmap(absl::Span<const int> bits) {
  Bitmap64 bitmap(bits.size());
  for (size_t i = 0; i < bits.size(); ++i) {
    bitmap.Set(i, static_cast<bool>(bits[i]));
  }
  return bitmap;
}

double GetBitmapDensity(const Bitmap64& bitmap) {
  return static_cast<double>(bitmap.GetOnesCount()) / bitmap.bits();
}

double GetBitmapClustering(const Bitmap64& bitmap) {
  if (bitmap.IsAllZeroes()) return 0.0;

  // Collect lengths of individual 1-fills.
  std::vector<size_t> lengths;
  size_t curr_length = 1;
  size_t prev_index;
  bool first_true_bit = true;
  for (const size_t index : bitmap.TrueBitIndices()) {
    if (first_true_bit) {
      prev_index = index;
      first_true_bit = false;
      continue;
    }
    // Check whether index is consecutive.
    if (index == prev_index + 1) {
      // Increase 1-fill length.
      ++curr_length;
    } else {
      // Start new 1-fill.
      lengths.push_back(curr_length);
      curr_length = 1;
    }
    prev_index = index;
  }
  // Flush last 1-fill.
  lengths.push_back(curr_length);

  // Return average length.
  return static_cast<double>(
             std::accumulate(lengths.begin(), lengths.end(), size_t(0))) /
         lengths.size();
}

Roaring ToRoaring(const Bitmap64& bitmap) {
  std::vector<uint32_t> indexes;
  for (const uint32_t index : bitmap.TrueBitIndices()) {
    indexes.push_back(index);
  }
  return Roaring(indexes.size(), indexes.data());
}

size_t GetNumBitmaps(const std::vector<Bitmap64Ptr>& bitmaps) {
  size_t num_bitmaps = 0;
  for (const Bitmap64Ptr& bitmap : bitmaps) {
    if (bitmap != nullptr) ++num_bitmaps;
  }
  return num_bitmaps;
}

size_t GetBitmapsByteSize(const std::vector<Bitmap64Ptr>& bitmaps,
                          const size_t num_stripes) {
  return std::ceil(static_cast<double>(GetNumBitmaps(bitmaps) * num_stripes) /
                   CHAR_BIT);
}

Bitmap64 GetGlobalBitmap(const std::vector<Bitmap64Ptr>& bitmaps) {
  size_t num_bits = 0;
  for (const auto& bitmap : bitmaps)
    if (bitmap != nullptr) num_bits += bitmap->bits();
  Bitmap64 global_bitmap(num_bits);
  size_t base_index = 0;
  for (const Bitmap64Ptr& bitmap : bitmaps) {
    if (bitmap == nullptr) continue;
    for (const size_t index : bitmap->TrueBitIndices())
      global_bitmap.Set(base_index + index, true);
    base_index += bitmap->bits();
  }
  return global_bitmap;
}

BitmapStats GetBitmapStats(const std::vector<Bitmap64Ptr>& bitmaps,
                           const size_t num_stripes) {
  BitmapStats bitmap_stats;

  const Bitmap64 global_bitmap = GetGlobalBitmap(bitmaps);
  bitmap_stats.set_density(GetBitmapDensity(global_bitmap));
  bitmap_stats.set_clustering(GetBitmapClustering(global_bitmap));
  bitmap_stats.set_bitpacked_size(GetBitmapsByteSize(bitmaps, num_stripes));
  bitmap_stats.set_bitpacked_compressed_size(
      Compress(SerializeBitmap(global_bitmap)).size());

  int64_t roaring_size;
  int64_t roaring_compressed_size;
  GetRoaringSize(global_bitmap, &roaring_size, &roaring_compressed_size);
  bitmap_stats.set_roaring_size(roaring_size);
  bitmap_stats.set_roaring_compressed_size(roaring_compressed_size);

  int64_t roaring_individual_size = 0;
  int64_t roaring_individual_compressed_size = 0;
  for (const Bitmap64Ptr& bitmap : bitmaps) {
    if (bitmap == nullptr) continue;

    int64_t individual_size;
    int64_t individual_compressed_size;
    GetRoaringSize(*bitmap, &individual_size, &individual_compressed_size);
    roaring_individual_size += individual_size;
    roaring_individual_compressed_size += individual_compressed_size;
  }
  bitmap_stats.set_roaring_individual_size(roaring_individual_size);
  bitmap_stats.set_roaring_individual_compressed_size(
      roaring_individual_compressed_size);

  int64_t rle_size = 0;
  int64_t rle_compressed_size = 0;
  GetRleBitmapSize(global_bitmap, &rle_size, &rle_compressed_size);
  bitmap_stats.set_rle_size(rle_size);
  bitmap_stats.set_rle_compressed_size(rle_compressed_size);

  return bitmap_stats;
}

void PrintBitmapStats(const std::vector<Bitmap64Ptr>& bitmaps,
                      const size_t num_stripes) {
  BitmapStats stats = GetBitmapStats(bitmaps, num_stripes);

  std::cout << std::string(80, '-') << std::endl;

  std::cout << "density:double,"                           //
            << "clustering:double,"                        //
            << "bitpacked_size:long,"                      //
            << "bitpacked_compressed_size:long,"           //
            << "roaring_size:long,"                        //
            << "roaring_compressed_size:long,"             //
            << "roaring_individual_size:long,"             //
            << "roaring_individual_compressed_size:long,"  //
            << "rle_size:long,"                            //
            << "rle_compressed_size:long"                  //
            << std::endl;

  std::cout << stats.density() << ","                             //
            << stats.clustering() << ","                          //
            << stats.bitpacked_size() << ","                      //
            << stats.bitpacked_compressed_size() << ","           //
            << stats.roaring_size() << ","                        //
            << stats.roaring_compressed_size() << ","             //
            << stats.roaring_individual_size() << ","             //
            << stats.roaring_individual_compressed_size() << ","  //
            << stats.rle_size() << ","                            //
            << stats.rle_compressed_size()                        //
            << std::endl;

  std::cout << std::string(80, '-') << std::endl;
}

void WriteBitmapToFile(const std::string& path, const Bitmap64& bitmap) {
  // Use DenseEncode rather than encoding individual words to get some
  // compression. Note that DenseEncode doesn't encode the size, so we encode
  // that separately.
  const uint32_t num_bits = bitmap.bits();
  std::string bits;
  Bitmap64::DenseEncode(bitmap, &bits);

  std::string encoded;
  absl::StrAppend(&encoded,
                  absl::string_view(reinterpret_cast<const char*>(&num_bits),
                                    sizeof(uint32_t)));
  absl::StrAppend(&encoded, bits);

  std::ofstream file(path);
  file << encoded;
  file.close();
}

Bitmap64 ReadBitmapFromFile(const std::string& path) {
  std::ifstream file(path);
  assert(file.is_open());
  std::string encoded = (static_cast<std::stringstream const&>(
                             std::stringstream() << file.rdbuf())
                             .str());
  file.close();

  const char* ptr = encoded.data();
  // Get size.
  ptr += sizeof(uint32_t);
  // Get bitmap.
  Bitmap64 bitmap =
      Bitmap64::DenseDecode({ptr, encoded.size() - sizeof(uint32_t)});
  return bitmap;
}

}  // namespace ci
