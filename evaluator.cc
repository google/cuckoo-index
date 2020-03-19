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
// File: evaluator.cc
// -----------------------------------------------------------------------------

#include "evaluator.h"

#include <limits>
#include <ostream>

#include "absl/random/distributions.h"
#include "absl/random/random.h"
#include "absl/strings/str_format.h"
#include "data.h"
#include "evaluation.pb.h"
#include "index_structure.h"

namespace ci {

using ci::EvaluationResults;
using TestCase = ci::EvaluationResults::TestCase;

std::vector<EvaluationResults> Evaluator::RunExperiments(
    std::vector<std::unique_ptr<IndexStructureFactory>>
        index_structure_factories,
    const std::unique_ptr<Table>& table,
    const std::vector<size_t>& num_rows_per_stripe_to_test, size_t num_lookups,
    const std::vector<std::string>& test_cases) {
  std::vector<EvaluationResults> results;

  for (const std::unique_ptr<Column>& column : table->GetColumns()) {
    std::cout << "Column: " << column->name() << std::endl;
    EvaluationResults base_result;
    base_result.set_column_name(column->name());
    base_result.set_column_type(DataTypeName(column->type()));
    base_result.set_column_cardinality(column->num_distinct_values());

    for (size_t num_rows_per_stripe : num_rows_per_stripe_to_test) {
      base_result.set_num_rows_per_stripe(num_rows_per_stripe);
      base_result.set_num_stripes(column->num_rows() / num_rows_per_stripe);
      base_result.set_column_compressed_size_bytes(
          column->compressed_size_bytes(num_rows_per_stripe));

      for (const std::unique_ptr<IndexStructureFactory>& factory :
           index_structure_factories) {
        std::unique_ptr<IndexStructure> index =
            factory->Create(*column, num_rows_per_stripe);
        EvaluationResults result = base_result;
        result.set_index_structure(index->name());
        result.set_index_size_bytes(index->byte_size());
        result.set_index_compressed_size_bytes(index->compressed_byte_size());
        *result.mutable_bitmap_stats() = index->bitmap_stats();

        for (const std::string& test_case : test_cases) {
          if (test_case == "positive_uniform") {
            *result.add_test_cases() = DoPositiveUniformLookups(
                *column, *index, num_rows_per_stripe, num_lookups);
          } else if (test_case == "positive_distinct") {
            *result.add_test_cases() = DoPositiveDistinctLookups(
                *column, *index, num_rows_per_stripe, num_lookups);
          } else if (test_case == "positive_zipf") {
            *result.add_test_cases() = DoPositiveZipfLookups(
                *column, *index, num_rows_per_stripe, num_lookups);
          } else if (test_case == "negative") {
            *result.add_test_cases() = DoNegativeLookups(
                *column, *index, num_rows_per_stripe, num_lookups);
          } else if (test_case == "mixed") {
            for (double hit_rate = 0.0; hit_rate <= 1.0; hit_rate += 0.1) {
              *result.add_test_cases() = DoMixedLookups(
                  *column, *index, num_rows_per_stripe, num_lookups, hit_rate);
            }
          } else {
            std::cerr << "Test case " << test_case << " does not exist."
                      << std::endl;
            exit(EXIT_FAILURE);
          }
        }

        results.push_back(std::move(result));
      }
    }
  }

  return results;
}

TestCase Evaluator::DoPositiveUniformLookups(const Column& column,
                                             const IndexStructure& index,
                                             std::size_t num_rows_per_stripe,
                                             std::size_t num_lookups) {
  std::mt19937 gen(42);
  const std::size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  std::size_t num_false_positive_stripes = 0;
  std::size_t num_true_negative_stripes = 0;
  // Remove NULLs from the possible lookup values.
  std::vector<int> column_data = column.data();
  column_data.erase(std::remove(column_data.begin(), column_data.end(),
                                Column::kIntNullSentinel),
                    column_data.end());

  std::uniform_int_distribution<std::size_t> row_offset_d(
      0, column_data.size() - 1);
  for (size_t i = 0; i < num_lookups; ++i) {
    // Draw value from random row offset.
    const int value = column_data[row_offset_d(gen)];
    ProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                    &num_true_negative_stripes, &num_false_positive_stripes);
  }

  TestCase test_case;
  test_case.set_name("positive_uniform");
  test_case.set_num_lookups(num_lookups);
  test_case.set_num_false_positives(num_false_positive_stripes);
  test_case.set_num_true_negatives(num_true_negative_stripes);

  return test_case;
}

TestCase Evaluator::DoPositiveDistinctLookups(const Column& column,
                                              const IndexStructure& index,
                                              std::size_t num_rows_per_stripe,
                                              std::size_t num_lookups) {
  std::mt19937 gen(42);
  const std::size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  std::size_t num_false_positive_stripes = 0;
  std::size_t num_true_negative_stripes = 0;
  std::vector<int> distinct_values = column.distinct_values();
  // Remove NULLs from the lookup.
  distinct_values.erase(
      std::remove(distinct_values.begin(), distinct_values.end(),
                  Column::kIntNullSentinel),
      distinct_values.end());
  std::uniform_int_distribution<std::size_t> distinct_values_offset_d(
      0, distinct_values.size() - 1);
  for (size_t i = 0; i < num_lookups; ++i) {
    // Draw value from random offset.
    const int value = distinct_values[distinct_values_offset_d(gen)];
    ProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                    &num_true_negative_stripes, &num_false_positive_stripes);
  }

  TestCase test_case;
  test_case.set_name("positive_distinct");
  test_case.set_num_lookups(num_lookups);
  test_case.set_num_false_positives(num_false_positive_stripes);
  test_case.set_num_true_negatives(num_true_negative_stripes);

  return test_case;
}

TestCase Evaluator::DoPositiveZipfLookups(const Column& column,
                                          const IndexStructure& index,
                                          std::size_t num_rows_per_stripe,
                                          std::size_t num_lookups) {
  std::mt19937 gen(42);
  const std::size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  std::size_t num_false_positive_stripes = 0;
  std::size_t num_true_negative_stripes = 0;
  std::vector<int> distinct_values = column.distinct_values();
  // Remove NULLs from the lookup.
  distinct_values.erase(
      std::remove(distinct_values.begin(), distinct_values.end(),
                  Column::kIntNullSentinel),
      distinct_values.end());
  for (size_t i = 0; i < num_lookups; ++i) {
    // Draw value from random (Zipf distributed) offset.
    // absl::Zipf is used with its default parameters q = 2.0 and v = 1.0.
    // q (>= 1.0) is what's referred to as the distribution parameter a in other
    // implementations (e.g., numpy.random.zipf).
    const size_t offset =
        absl::Zipf(gen, distinct_values.size() - 1, /*q=*/2.0);
    const int value = distinct_values[offset];
    ProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                    &num_true_negative_stripes, &num_false_positive_stripes);
  }

  TestCase test_case;
  test_case.set_name("positive_zipf");
  test_case.set_num_lookups(num_lookups);
  test_case.set_num_false_positives(num_false_positive_stripes);
  test_case.set_num_true_negatives(num_true_negative_stripes);

  return test_case;
}

TestCase Evaluator::DoNegativeLookups(const Column& column,
                                      const IndexStructure& index,
                                      std::size_t num_rows_per_stripe,
                                      std::size_t num_lookups) {
  std::mt19937 gen(42);
  const std::size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  std::size_t num_false_positive_stripes = 0;
  std::size_t num_true_negative_stripes = 0;
  std::uniform_int_distribution<int> value_d(std::numeric_limits<int>::min(),
                                             std::numeric_limits<int>::max());
  for (size_t i = 0; i < num_lookups; ++i) {
    // Draw random value that is not present in the column.
    int value = value_d(gen);
    while (column.Contains(value)) {
      value = value_d(gen);
    }
    ProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                    &num_true_negative_stripes, &num_false_positive_stripes);
  }

  TestCase test_case;
  test_case.set_name("negative");
  test_case.set_num_lookups(num_lookups);
  test_case.set_num_false_positives(num_false_positive_stripes);
  test_case.set_num_true_negatives(num_true_negative_stripes);

  return test_case;
}

TestCase Evaluator::DoMixedLookups(const Column& column,
                                   const IndexStructure& index,
                                   std::size_t num_rows_per_stripe,
                                   std::size_t num_lookups, double hit_rate) {
  absl::BitGen bitgen;
  std::mt19937 gen(42);
  const std::size_t num_stripes = column.num_rows() / num_rows_per_stripe;
  std::size_t num_false_positive_stripes = 0;
  std::size_t num_true_negative_stripes = 0;
  std::vector<int> distinct_values = column.distinct_values();
  // Remove NULLs from the lookup.
  distinct_values.erase(
      std::remove(distinct_values.begin(), distinct_values.end(),
                  Column::kIntNullSentinel),
      distinct_values.end());
  std::uniform_int_distribution<std::size_t> distinct_values_offset_d(
      0, distinct_values.size() - 1);
  std::uniform_int_distribution<int> value_d(std::numeric_limits<int>::min(),
                                             std::numeric_limits<int>::max());
  for (size_t i = 0; i < num_lookups; ++i) {
    int value;
    // Positive or negative lookup?
    if (absl::Bernoulli(bitgen, hit_rate)) {
      // Positive case.
      // Draw value from random offset.
      value = distinct_values[distinct_values_offset_d(gen)];
    } else {
      // Negative case.
      value = value_d(gen);
      while (column.Contains(value)) {
        value = value_d(gen);
      }
    }
    ProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                    &num_true_negative_stripes, &num_false_positive_stripes);
  }

  TestCase test_case;
  test_case.set_name(absl::StrFormat("mixed/%.1f", hit_rate));
  test_case.set_num_lookups(num_lookups);
  test_case.set_num_false_positives(num_false_positive_stripes);
  test_case.set_num_true_negatives(num_true_negative_stripes);

  return test_case;
}

void Evaluator::ProbeAllStripes(const Column& column,
                                const IndexStructure& index, int value,
                                std::size_t num_rows_per_stripe,
                                std::size_t num_stripes,
                                std::size_t* num_true_negative_stripes,
                                std::size_t* num_false_positive_stripes) {
  for (size_t stripe_id = 0; stripe_id < num_stripes; ++stripe_id) {
    // Get expected result (ground truth).
    const bool expected =
        column.StripeContains(num_rows_per_stripe, stripe_id, value);
    if (!expected) ++(*num_true_negative_stripes);
    if (index.StripeContains(stripe_id, value) != expected) {
      if (expected) {
        std::cerr << index.name() << " returned a false negative." << std::endl;
        exit(EXIT_FAILURE);
      }
      ++(*num_false_positive_stripes);
    }
  }
}

}  // namespace ci
