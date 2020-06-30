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
// File: evaluate.cc
// -----------------------------------------------------------------------------

#include <cstddef>
#include <iostream>
#include <ostream>
#include <random>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "cuckoo_index.h"
#include "cuckoo_utils.h"
#include "data.h"
#include "evaluation.pb.h"
#include "evaluation_utils.h"
#include "evaluator.h"
#include "index_structure.h"
#include "per_stripe_bloom.h"
#include "per_stripe_xor.h"
#include "zone_map.h"

ABSL_FLAG(int, generate_num_values, 100000,
          "Number of values to generate (number of rows).");
ABSL_FLAG(int, num_unique_values, 1000,
          "Number of unique values to generate (cardinality).");
ABSL_FLAG(std::string, input_csv_path, "", "Path to the input CSV file.");
ABSL_FLAG(std::string, output_csv_path, "",
          "Path to write the output CSV file to.");
ABSL_FLAG(std::vector<std::string>, columns_to_test, {"company_name"},
          "Comma-separated list of columns to tests, e.g. "
          "'company_name,country_code'.");
ABSL_FLAG(std::vector<std::string>, num_rows_per_stripe_to_test, {"10000"},
          "Number of rows per stripe. Defaults to 10,000.");
ABSL_FLAG(int, num_lookups, 1000, "Number of lookups. Defaults to 1,000.");
ABSL_FLAG(std::vector<std::string>, test_cases, {"positive_uniform"},
          "Comma-separated list of test cases, e.g. "
          "'positive_uniform,positive_distinct'.");
ABSL_FLAG(std::string, sorting, "NONE",
          "Sorting to apply to the data. Supported values: 'NONE', "
          "'BY_CARDINALITY' (sorts lexicographically, starting with columns "
          "with the lowest cardinality), 'RANDOM'");

namespace {
static constexpr absl::string_view kNoSorting = "NONE";
static constexpr absl::string_view kByCardinalitySorting = "BY_CARDINALITY";
static constexpr absl::string_view kRandomSorting = "RANDOM";
static bool IsValidSorting(absl::string_view sorting) {
  static const auto* values = new absl::flat_hash_set<absl::string_view>(
      {kNoSorting, kByCardinalitySorting, kRandomSorting});

  return values->contains(sorting);
}
}  // namespace

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  const size_t generate_num_values = absl::GetFlag(FLAGS_generate_num_values);
  const size_t num_unique_values = absl::GetFlag(FLAGS_num_unique_values);
  const std::string input_csv_path = absl::GetFlag(FLAGS_input_csv_path);
  const std::string output_csv_path = absl::GetFlag(FLAGS_output_csv_path);
  if (output_csv_path.empty()) {
    std::cerr << "You must specify --output_csv_path" << std::endl;
    std::exit(EXIT_FAILURE);
  }
  const std::vector<std::string>
      columns_to_test = absl::GetFlag(FLAGS_columns_to_test);
  std::vector<size_t> num_rows_per_stripe_to_test;
  for (const std::string num_rows :
      absl::GetFlag(FLAGS_num_rows_per_stripe_to_test)) {
    num_rows_per_stripe_to_test.push_back(std::stoull(num_rows));
  }
  const size_t num_lookups = absl::GetFlag(FLAGS_num_lookups);
  const std::vector<std::string> test_cases = absl::GetFlag(FLAGS_test_cases);
  const std::string sorting = absl::GetFlag(FLAGS_sorting);

  // Define data.
  std::unique_ptr<ci::Table> table;
  if (input_csv_path.empty() || columns_to_test.empty()) {
    std::cerr
        << "[WARNING] --input_csv_path or --columns_to_test not specified, "
           "generating synthetic data." << std::endl;
    std::cout << "Generating " << generate_num_values << " values ("
              << static_cast<double>(num_unique_values) / generate_num_values
                  * 100 << "% unique)..." << std::endl;
    table = ci::GenerateUniformData(generate_num_values, num_unique_values);
  } else {
    std::cout << "Loading data from file " << input_csv_path << "..."
              << std::endl;
    table = ci::Table::FromCsv(input_csv_path, columns_to_test);
  }

  // Potentially sort the data.
  if (!IsValidSorting(sorting)) {
    std::cerr << "Invalid sorting method: " << sorting << std::endl;
    std::exit(EXIT_FAILURE);
  }
  if (sorting == kByCardinalitySorting) {
    std::cerr << "Sorting the table according to column cardinality..."
              << std::endl;
    table->SortWithCardinalityKey();
  } else if (sorting == kRandomSorting) {
    std::cerr << "Randomly shuffling the table..." << std::endl;
    table->Shuffle();
  }

  // Define competitors.
  std::vector<std::unique_ptr<ci::IndexStructureFactory>> index_factories;
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING, ci::kMaxLoadFactor1SlotsPerBucket,
      /*scan_rate=*/0.02, /*slots_per_bucket=*/1,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(
      absl::make_unique<ci::PerStripeBloomFactory>(/*num_bits_per_key=*/10));
  index_factories.push_back(absl::make_unique<ci::PerStripeXorFactory>());
  index_factories.push_back(absl::make_unique<ci::ZoneMapFactory>());

  // Evaluate competitors.
  ci::Evaluator evaluator;
  std::vector<ci::EvaluationResults> results = evaluator.RunExperiments(
      std::move(index_factories), table, num_rows_per_stripe_to_test,
      num_lookups, test_cases);

  ci::WriteToCsv(output_csv_path, results);

  std::cout << std::endl
            << "** Result summary **" << std::endl
            << absl::StrFormat("%-50s %10s %10s %11s %11s",
                               "field & index-type", "column", "index",
                               "relative", "scan-rate")
            << std::endl;
  for (const auto& result : results) {
    const std::string column_compr_size =
        absl::StrCat(result.column_compressed_size_bytes());
    const std::string index_compr_size =
        absl::StrCat(result.index_compressed_size_bytes());
    double scan_rate = -1.0;
    for (const auto& test_case : result.test_cases()) {
      if (test_case.name() == "negative")
        scan_rate = 100.0 * test_case.num_false_positives() /
                    (test_case.num_lookups() * result.num_stripes());
    }
    std::cout << absl::StrFormat("%-50s %10s %10s %10.2f%% %10.2f%%",
                                 absl::StrCat(result.column_name(), ", ",
                                              result.index_structure(), ":"),
                                 column_compr_size, index_compr_size,
                                 100.0 * result.index_compressed_size_bytes() /
                                     result.column_compressed_size_bytes(),
                                 scan_rate)
              << std::endl;
  }

  return 0;
}
