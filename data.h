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
// File: data.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_DATA_H_
#define CUCKOO_INDEX_DATA_H_

#include <algorithm>
#include <cmath>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <iterator>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "absl/container/flat_hash_set.h"
#include "absl/memory/memory.h"
#include "absl/random/random.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "boost/math/tools/univariate_statistics.hpp"
#include "common/byte_coding.h"
#include "evaluation_utils.h"
#include "single_include/csv.hpp"

namespace ci {

enum class DataType { STRING, INT };

inline std::string DataTypeName(DataType data_type) {
  switch (data_type) {
    case DataType::STRING:
      return "STRING";
    case DataType::INT:
      return "INT";
  }

  return "UNKNOWN";
}

class Column;
using ColumnPtr = std::unique_ptr<Column>;

// Holds data and provides stats.
class Column {
 public:
  // Note that we dict-encode NULL strings as int 0. Thus, the sentinel value
  // should not be changed without changing the dict encoding in the `Column`
  // constructor below.
  static constexpr int kIntNullSentinel = 0;
  static constexpr const char* kStringNullSentinel = "NULL";

  static ColumnPtr IntColumn(const std::string& name, std::vector<int> data) {
    return ColumnPtr(new Column(name, DataType::INT, std::move(data)));
  }

  Column(const std::string& name, const DataType type,
         const std::vector<std::string>& str_data)
      : name_(name), type_(type), str_data_(str_data) {
    if (type == DataType::INT) {
      // Convert string to int.
      data_.reserve(str_data.size());
      for (const std::string& str : str_data) data_.push_back(std::stoi(str));
    } else if (type == DataType::STRING) {
      // Dict-encode strings. Essentially, encode strings as dense integers in
      // an order-preserving way. Also called order-preserving minimal perfect
      // hashing: https://en.wikipedia.org/wiki/Perfect_hash_function.
      // Such a mapping is possible since we know all indexed strings at build
      // time. The advantage over regular, non-order preserving hash functions
      // is that we can use ZoneMaps (min/max checks) for data pruning.
      // Note: Due to the dense mapping, we cannot have negative lookup keys
      // (strings that do not exist in the indexed data) that fall within the
      // range of dense integers. Thus, ZoneMaps will be 100% effective in such
      // cases. That is, because we need to choose an integer outside of the
      // range for negative lookups keys (e.g., int::max).
      //
      // We make sure that NULL values get an ID of 0 – that way we can detect
      // and ignore them when building some data structures, e.g. ZoneMaps.
      data_.reserve(str_data.size());
      absl::flat_hash_set<std::string> distinct_strings(str_data.begin(),
                                                        str_data.end());
      distinct_strings.erase(kStringNullSentinel);
      std::vector<std::string> distinct_strings_v(distinct_strings.begin(),
                                                  distinct_strings.end());
      std::sort(distinct_strings_v.begin(), distinct_strings_v.end());
      distinct_strings_v.insert(distinct_strings_v.begin(),
                                kStringNullSentinel);
      int i = 0;
      for (const std::string& str : distinct_strings_v) {
        string_dict_[str] = i++;
      }
      // Convert strings to ints using the mapping.
      for (const std::string& str : str_data) {
        auto it = string_dict_.find(str);
        if (it == string_dict_.end()) {
          std::cerr << "Error during dict encoding." << std::endl;
          exit(EXIT_FAILURE);
        }
        data_.push_back(it->second);
      }
    } else {
      std::cerr << "Unsupported data type." << std::endl;
      exit(EXIT_FAILURE);
    }

    // Initialize `distinct_values_`.
    distinct_values_ = std::unordered_set<int>(data_.begin(), data_.end());

    // Set `min_` and `max_`.
    const auto min_max = std::minmax_element(begin(data_), end(data_));
    min_ = *min_max.first;
    max_ = *min_max.second;

    // Compute standard moments.
    mean_ = boost::math::tools::mean(data_);
    variance_ = boost::math::tools::variance(data_);
    skewness_ = boost::math::tools::skewness(data_);
    kurtosis_ = boost::math::tools::kurtosis(data_);

    PrintStats();
  }

  void PrintStats() const {
    std::cout << "column: " << name_ << " (" << DataTypeName(type_)
              << "), min: " << min_ << ", max: " << max_
              << ", #rows: " << num_rows()
              << ", cardinality: " << num_distinct_values()
              << ", mean: " << mean_ << ", variance: " << variance_
              << ", skewness: " << skewness_ << ", kurtosis: " << kurtosis_
              << std::endl;
  }

  bool Contains(int value) const {
    return distinct_values_.find(value) != distinct_values_.end();
  }

  bool StripeContains(std::size_t num_rows_per_stripe, std::size_t stripe_id,
                      int value) const {
    const std::size_t num_stripes = data_.size() / num_rows_per_stripe;
    if (stripe_id >= num_stripes) {
      std::cerr << "`stripe_id` is out of bounds." << std::endl;
      exit(EXIT_FAILURE);
    }
    const std::size_t stripe_begin = num_rows_per_stripe * stripe_id;
    const std::size_t stripe_end = stripe_begin + num_rows_per_stripe;
    for (size_t i = stripe_begin; i < stripe_end; ++i) {
      if (data_[i] == value) return true;
    }
    return false;
  }

  // Reorders the rows in the column according to the given list of indexes.
  //
  // At position i the list should specify the row that should be moved to that
  // position.
  void Reorder(absl::Span<const size_t> indexes) {
    assert(data_.size() == indexes.size());
    std::vector<int> new_data(data_.size());
    for (size_t i = 0; i < data_.size(); ++i) {
      new_data[i] = data_[indexes[i]];
    }
    data_.swap(new_data);
  }

  std::string name() const { return name_; }
  DataType type() const { return type_; }
  const std::vector<int>& data() const { return data_; }
  int operator[](std::size_t idx) const { return data_[idx]; }

  // Returns the original value (not an encoded ID) at the given position.
  std::string ValueAt(std::size_t idx) const {
    // For INT column just return the value.
    if (string_dict_.empty()) return absl::StrCat(data_[idx]);

    // For STRING column reverse the mapping.
    return std::find_if(
               string_dict_.begin(), string_dict_.end(),
               [this, idx](const auto& kv) { return kv.second == data_[idx]; })
        ->first;
  }

  std::vector<int> distinct_values() const {
    return std::vector<int>(distinct_values_.begin(), distinct_values_.end());
  }
  std::size_t num_rows() const { return data_.size(); }
  std::size_t num_distinct_values() const { return distinct_values_.size(); }
  int min() const { return min_; }
  int max() const { return max_; }
  std::size_t compressed_size_bytes(size_t num_rows_per_stripe) const {
    const size_t num_stripes = data_.size() / num_rows_per_stripe;
    size_t compressed_size = 0;
    // Compress the stripes individually.
    for (size_t stripe = 0; stripe < num_stripes; ++stripe) {
      const size_t start_row = stripe * num_rows_per_stripe;
      if (type_ == DataType::INT) {
        assert(start_row + num_rows_per_stripe <= data_.size());
        const absl::string_view data_view =
            absl::string_view(reinterpret_cast<const char*>(&data_[start_row]),
                              sizeof(data_[0]) * num_rows_per_stripe);
        compressed_size += ci::Compress(data_view).size();
      } else {
        assert(start_row + num_rows_per_stripe <= str_data_.size());
        // Encode the var-length strings, each as var-int32 length followed by
        // the actual string-data.
        ByteBuffer buffer;
        for (size_t i = 0; i < num_rows_per_stripe; ++i)
          PutString(str_data_[start_row + i], &buffer);
        compressed_size +=
            ci::Compress(absl::string_view(buffer.data(), buffer.pos())).size();
      }
    }
    return compressed_size;
  }

 private:
  Column(const std::string& name, const DataType type, std::vector<int> data)
      : name_(name), type_(type), data_(std::move(data)) {
    assert(type <= DataType::INT);
    // Initialize `distinct_values_`.
    distinct_values_ = std::unordered_set<int>(data_.begin(), data_.end());

    // Set `min_` and `max_`.
    const auto min_max = std::minmax_element(begin(data_), end(data_));
    min_ = *min_max.first;
    max_ = *min_max.second;

    // Compute standard moments.
    mean_ = boost::math::tools::mean(data_);
    variance_ = boost::math::tools::variance(data_);
    skewness_ = boost::math::tools::skewness(data_);
    kurtosis_ = boost::math::tools::kurtosis(data_);

    PrintStats();
  }

  std::string name_;
  DataType type_;
  std::vector<int> data_;
  std::unordered_set<int> distinct_values_;
  // Used to map strings to ints in an order-preserving way.
  std::unordered_map<std::string, int> string_dict_;
  // The original vector of strings if given to the c'tor.
  const std::vector<std::string> str_data_;

  // Stats.
  int min_, max_;
  // Standard moments: mean, variance, skewness, and excess kurtosis.
  // https://www.gnu.org/software/gsl/doc/html/statistics.html
  double mean_, variance_, skewness_, kurtosis_;
};

// Used for parsing a column from a CSV file.
struct CsvColumnInfo {
  CsvColumnInfo(const std::string& name, const DataType type,
                const std::size_t index)
      : name(name), type(type), index(index) {}

  std::string name;
  DataType type;
  // Index (offset) in CSV.
  std::size_t index;
};

class Table {
 public:
  static std::unique_ptr<Table> FromCsv(
      const std::string& file_path,
      const std::vector<std::string> column_names) {
    csv::CSVReader reader(file_path);

    // Make sure all the requested columns are present and map positions in
    // `column_names` to column positions in the data.
    std::vector<std::string> present_column_names = reader.get_col_names();
    std::vector<CsvColumnInfo> column_infos;
    column_infos.reserve(column_names.size());
    for (const std::string& column_name : column_names) {
      auto pos = std::find(present_column_names.begin(),
                           present_column_names.end(), column_name);

      if (pos == present_column_names.end()) {
        std::cerr << "Unknown column '" << column_name
                  << "'. Available columns: "
                  << absl::StrJoin(present_column_names, ",") << std::endl;
        std::exit(EXIT_FAILURE);
      }

      // Create ColumnInfo`
      column_infos.push_back(
          CsvColumnInfo{.name = column_name,
                        .type = DataType::STRING,
                        .index = static_cast<size_t>(
                            std::distance(present_column_names.begin(), pos))});
    }

    std::vector<std::vector<std::string>> csv_data(column_names.size());
    for (csv::CSVRow& row : reader) {
      for (size_t i = 0; i < column_names.size(); ++i) {
        csv_data[i].push_back(row[column_infos[i].index].get<std::string>());
      }
    }

    for (size_t i = 0; i < csv_data.size(); ++i) {
      bool is_column_int = true;
      for (const std::string& value : csv_data[i]) {
        if (value != Column::kStringNullSentinel &&
            !std::all_of(value.begin(), value.end(),
                         [](unsigned char c) { return std::isdigit(c); })) {
          is_column_int = false;
          break;
        }
      }

      if (is_column_int) {
        // If the column is an integer column, change its type to
        // DataType::INT and convert all sentinel values.
        column_infos[i].type = DataType::INT;
        for (std::string& value : csv_data[i]) {
          if (value == Column::kStringNullSentinel) {
            value = absl::StrCat(Column::kIntNullSentinel);
          }
        }
      }
    }

    // Create columns.
    std::vector<std::unique_ptr<Column>> columns;
    columns.reserve(column_infos.size());
    for (size_t i = 0; i < column_infos.size(); ++i) {
      const CsvColumnInfo& info = column_infos[i];
      columns.push_back(
          absl::make_unique<Column>(info.name, info.type, csv_data[i]));
    }

    return std::unique_ptr<Table>(new Table("test_table", std::move(columns)));
  }

  static std::unique_ptr<Table> Create(
      const std::string& name, std::vector<std::unique_ptr<Column>> columns) {
    size_t num_rows = columns[0]->num_rows();
    for (const std::unique_ptr<Column>& column : columns) {
      if (column->num_rows() != num_rows) {
        std::cerr << "Incorrect number of rows: expected " << num_rows
                  << ", got " << column->num_rows() << std::endl;
        std::exit(EXIT_FAILURE);
      }
    }
    return std::unique_ptr<Table>(new Table(name, std::move(columns)));
  }

  const Column& GetColumn(const std::string& name) {
    for (const ColumnPtr& column : columns_) {
      if (column->name() == name) return *column;
    }
    std::cerr << "Column " << name << " not found." << std::endl;
    exit(EXIT_FAILURE);
  }

  const std::vector<std::unique_ptr<Column>>& GetColumns() { return columns_; }

  void PrintHeader() const {
    for (size_t i = 0; i < columns_.size(); ++i) {
      std::cout << columns_[i]->name();
      if (i < columns_.size() - 1) std::cout << ",";
    }
    std::cout << std::endl;
  }

  void PrintColumns() const {
    for (const std::unique_ptr<Column>& column : columns_) {
      column->PrintStats();
    }
  }

  // Randomly shuffles the table rows for all columns.
  void Shuffle() {
    std::vector<size_t> indexes(columns_[0]->num_rows());
    std::iota(indexes.begin(), indexes.end(), 0);
    absl::BitGen gen;
    std::shuffle(indexes.begin(), indexes.end(), gen);

    for (std::unique_ptr<Column>& column : columns_) {
      column->Reorder(indexes);
    }
  }

  // Sorts the table rows using columns ordered by cardinality as the key.
  void SortWithCardinalityKey() {
    // Sort the columns by cardinality.
    std::vector<std::pair<size_t, size_t>> cardinality_and_index;
    cardinality_and_index.reserve(columns_.size());
    for (size_t i = 0; i < columns_.size(); ++i) {
      cardinality_and_index.emplace_back(columns_[i]->num_distinct_values(), i);
    }
    std::sort(cardinality_and_index.begin(), cardinality_and_index.end());

    // Create a comparator that compares values according to the determined
    // column order.
    std::function<bool(size_t, size_t)> comparator = [&](size_t row_id,
                                                         size_t other_row_id) {
      for (const auto& [_, column_index] : cardinality_and_index) {
        if ((*columns_[column_index])[row_id] <
            (*columns_[column_index])[other_row_id]) {
          return true;
        } else if ((*columns_[column_index])[row_id] >
                   (*columns_[column_index])[other_row_id]) {
          return false;
        }
      }

      return false;
    };

    // Sort rows indexes using the comparator and reorder all columns.
    std::vector<size_t> indexes(columns_[0]->num_rows());
    std::iota(indexes.begin(), indexes.end(), 0);
    std::sort(indexes.begin(), indexes.end(), comparator);
    for (std::unique_ptr<Column>& column : columns_) {
      column->Reorder(indexes);
    }
  }

  std::string ToCsvString() const {
    std::string csv_string;

    if (columns_.empty()) return csv_string;

    const size_t num_rows = columns_[0]->num_rows();
    for (size_t row_id = 0; row_id < num_rows; ++row_id) {
      for (size_t column_id = 0; column_id < columns_.size(); ++column_id) {
        if (column_id != 0) absl::StrAppend(&csv_string, ",");

        absl::StrAppend(&csv_string, columns_[column_id]->ValueAt(row_id));
      }

      absl::StrAppend(&csv_string, "\n");
    }

    return csv_string;
  }

 private:
  Table(const std::string& name, std::vector<std::unique_ptr<Column>> columns)
      : name_(name), columns_(std::move(columns)) {}

  std::string name_;
  std::vector<ColumnPtr> columns_;
};

// Creates a table with a single column with uniformly distributed values.
std::unique_ptr<Table> GenerateUniformData(const size_t generate_num_values,
                                           const size_t num_unique_values);

}  // namespace ci

#endif  // CUCKOO_INDEX_DATA_H_
