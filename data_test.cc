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
// File: data_test.cc
// -----------------------------------------------------------------------------

#include "data.h"

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace ci {
namespace {

using ::testing::ElementsAreArray;

// Checks that `ValueAt()` method of a column created with the given data type
// and values produces expected results.
template <typename T>
void CheckValueAt(DataType data_type, const std::vector<T> expected_values) {}

TEST(ColumnTest, ValueAtReturnsValuesForIntColumns) {
  std::vector<std::string> expected_values = {"42", "13", "1", "42"};
  auto column =
      absl::make_unique<Column>("column_name", DataType::INT, expected_values);

  std::vector<std::string> actual_values;
  actual_values.reserve(column->num_rows());
  for (size_t i = 0; i < column->num_rows(); ++i) {
    actual_values.push_back(column->ValueAt(i));
  }

  EXPECT_THAT(actual_values, ElementsAreArray(expected_values));
}

TEST(ColumnTest, ValueAtReturnsValuesForStringColumns) {
  std::vector<std::string> expected_values = {"US", "CH", "US", "CH"};
  auto column = absl::make_unique<Column>("column_name", DataType::STRING,
                                          expected_values);

  std::vector<std::string> actual_values;
  actual_values.reserve(column->num_rows());
  for (size_t i = 0; i < column->num_rows(); ++i) {
    actual_values.push_back(column->ValueAt(i));
  }

  EXPECT_THAT(actual_values, ElementsAreArray(expected_values));
}

TEST(ColumnTest, CompressInts) {
  auto column =
      absl::make_unique<Column>("column_name", DataType::INT,
                                std::vector<std::string>{"0", "1", "2", "3"});

  const size_t compressed_size_1_stripe =
      column->compressed_size_bytes(/*num_rows_per_stripe=*/4);
  EXPECT_GT(compressed_size_1_stripe, 20);
  EXPECT_LT(compressed_size_1_stripe, 30);

  const size_t compressed_size_4_stripes =
      column->compressed_size_bytes(/*num_rows_per_stripe=*/1);
  EXPECT_GT(compressed_size_4_stripes, 50);
  EXPECT_LT(compressed_size_4_stripes, 60);
}

TEST(ColumnTest, CompressStrings) {
  auto column = absl::make_unique<Column>(
      "column_name", DataType::STRING,
      std::vector<std::string>{"DE", "US", "IT", "FR"});

  const size_t compressed_size_1_stripe =
      column->compressed_size_bytes(/*num_rows_per_stripe=*/4);
  EXPECT_GT(compressed_size_1_stripe, 20);
  EXPECT_LT(compressed_size_1_stripe, 30);

  const size_t compressed_size_4_stripes =
      column->compressed_size_bytes(/*num_rows_per_stripe=*/1);
  EXPECT_GT(compressed_size_4_stripes, 40);
  EXPECT_LT(compressed_size_4_stripes, 50);
}

TEST(ColumnTest, Reorder) {
  auto column =
      absl::make_unique<Column>("column_name", DataType::INT,
                                std::vector<std::string>{"0", "1", "2", "3"});

  const std::vector<size_t> indexes = {2, 1, 3, 0};
  column->Reorder(indexes);
  for (size_t i = 0; i < column->num_rows(); ++i) {
    EXPECT_EQ((*column)[i], indexes[i]);
  }
}

TEST(DataTest, SortWithCardinalityKey) {
  std::vector<std::unique_ptr<Column>> columns;
  columns.push_back(absl::make_unique<Column>(
      "customer_id", DataType::INT,
      std::vector<std::string>({"42", "13", "1", "42"})));
  columns.push_back(absl::make_unique<Column>(
      "country", DataType::STRING,
      std::vector<std::string>({"US", "CH", "US", "CH"})));
  std::unique_ptr<Table> table = Table::Create("test", std::move(columns));

  EXPECT_EQ(table->ToCsvString(),
            "42,US\n"
            "13,CH\n"
            "1,US\n"
            "42,CH\n");

  table->SortWithCardinalityKey();

  // Expect that the table is sorted by "country" first as it has lower
  // cardinality.
  EXPECT_EQ(table->ToCsvString(),
            "13,CH\n"
            "42,CH\n"
            "1,US\n"
            "42,US\n");
}

}  // namespace
}  // namespace ci
