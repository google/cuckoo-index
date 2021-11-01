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
// File: lookup_benchmark.cc
// -----------------------------------------------------------------------------
//
// Benchmarks for various `IndexStructure`s. Add new benchmark configs in the
// `main(..)` function.
//
// To run the benchmark run:
// bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :lookup_benchmark
// -- --input_csv_path='...' --columns_to_test='A,B,C'
//
// Example run:
// Run on (80 X 3900 MHz CPU s)
// CPU Caches:
//  L1 Data 32 KiB (x40)
//  L1 Instruction 32 KiB (x40)
//  L2 Unified 1024 KiB (x40)
//  L3 Unified 28160 KiB (x2)
// Load Average: 0.88, 0.68, 0.71
// -----------------------------------------------------------------------------
// Benchmark                                                           Time
// -----------------------------------------------------------------------------
// PositiveDistinctLookup/Color/16384/PerStripeBloom/10            28543 ns
// NegativeLookup/Color/16384/PerStripeBloom/10                    34615 ns
// PositiveDistinctLookup/Color/16384/CuckooIndex:1:0.49:0.02       2562 ns
// NegativeLookup/Color/16384/CuckooIndex:1:0.49:0.02                891 ns
// PositiveDistinctLookup/Color/16384/CuckooIndex:1:0.84:0.02       5240 ns
// NegativeLookup/Color/16384/CuckooIndex:1:0.84:0.02               5113 ns
// PositiveDistinctLookup/Color/16384/CuckooIndex:1:0.95:0.02       3845 ns
// NegativeLookup/Color/16384/CuckooIndex:1:0.95:0.02               4157 ns
// PositiveDistinctLookup/Color/16384/CuckooIndex:1:0.98:0.02       3396 ns
// NegativeLookup/Color/16384/CuckooIndex:1:0.98:0.02               3992 ns
// PositiveDistinctLookup/Color/16384/PerStripeXor                  4768 ns
// NegativeLookup/Color/16384/PerStripeXor                          3664 ns
// PositiveDistinctLookup/Color/65536/PerStripeBloom/10             7745 ns
// NegativeLookup/Color/65536/PerStripeBloom/10                     8782 ns
// PositiveDistinctLookup/Color/65536/CuckooIndex:1:0.49:0.02       1396 ns
// NegativeLookup/Color/65536/CuckooIndex:1:0.49:0.02                581 ns
// PositiveDistinctLookup/Color/65536/CuckooIndex:1:0.84:0.02       4111 ns
// NegativeLookup/Color/65536/CuckooIndex:1:0.84:0.02               5056 ns
// PositiveDistinctLookup/Color/65536/CuckooIndex:1:0.95:0.02       2821 ns
// NegativeLookup/Color/65536/CuckooIndex:1:0.95:0.02               4281 ns
// PositiveDistinctLookup/Color/65536/CuckooIndex:1:0.98:0.02       2486 ns
// NegativeLookup/Color/65536/CuckooIndex:1:0.98:0.02               4377 ns
// PositiveDistinctLookup/Color/65536/PerStripeXor                  1383 ns
// NegativeLookup/Color/65536/PerStripeXor                           895 ns

#include <cstdlib>
#include <random>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "benchmark/benchmark.h"
#include "cuckoo_index.h"
#include "cuckoo_utils.h"
#include "index_structure.h"
#include "per_stripe_bloom.h"
#include "per_stripe_xor.h"

ABSL_FLAG(int, generate_num_values, 100000,
"Number of values to generate (number of rows).");
ABSL_FLAG(int, num_unique_values, 1000,
"Number of unique values to generate (cardinality).");
ABSL_FLAG(std::string, input_csv_path, "", "Path to the input CSV file.");
ABSL_FLAG(std::vector<std::string>, columns_to_test, {},
          "Comma-separated list of columns to tests, e.g. "
          "'company_name,country_code'.");
ABSL_FLAG(std::string, sorting, "NONE",
          "Sorting to apply to the data. Supported values: 'NONE', "
          "'BY_CARDINALITY' (sorts lexicographically, starting with columns "
          "with the lowest cardinality), 'RANDOM'");

// To avoid drawing a random value for each single lookup, we look values up in
// batches. To avoid caching effects, we use 1M values as the batch size.
constexpr size_t kLookupBatchSize = 1'000'000;

constexpr absl::string_view kNoSorting = "NONE";
constexpr absl::string_view kByCardinalitySorting = "BY_CARDINALITY";
constexpr absl::string_view kRandomSorting = "RANDOM";

bool IsValidSorting(absl::string_view sorting) {
  static const auto* values = new absl::flat_hash_set<absl::string_view>(
      {kNoSorting, kByCardinalitySorting, kRandomSorting});

  return values->contains(sorting);
}

void BM_PositiveDistinctLookup(const ci::Column& column,
                               std::shared_ptr<ci::IndexStructure> index,
                               const int num_stripes, benchmark::State& state) {
  std::mt19937 gen(42);
  std::vector<int> distinct_values = column.distinct_values();
  // Remove NULLs from the lookup.
  distinct_values.erase(
      std::remove(distinct_values.begin(), distinct_values.end(),
                  ci::Column::kIntNullSentinel),
      distinct_values.end());
  std::uniform_int_distribution<std::size_t> distinct_values_offset_d(
      0, distinct_values.size() - 1);

  std::vector<int> values;
  values.reserve(kLookupBatchSize);
  for (size_t i = 0; i < kLookupBatchSize; ++i) {
    values.push_back(distinct_values[distinct_values_offset_d(gen)]);
  }

  while (state.KeepRunningBatch(values.size())) {
    for (size_t i = 0; i < values.size(); ++i) {
      ::benchmark::DoNotOptimize(index->GetQualifyingStripes(values[i],
                                                             num_stripes));
    }
  }
}

void BM_NegativeLookup(const ci::Column& column,
                       std::shared_ptr<ci::IndexStructure> index,
                       const int num_stripes, benchmark::State& state) {
  std::mt19937 gen(42);
  std::uniform_int_distribution<int> value_d(std::numeric_limits<int>::min(),
                                             std::numeric_limits<int>::max());
  std::vector<int> values;
  values.reserve(kLookupBatchSize);
  for (size_t i = 0; i < kLookupBatchSize; ++i) {
    // Draw random value that is not present in the column.
    int value = value_d(gen);
    while (column.Contains(value)) {
      value = value_d(gen);
    }
    values.push_back(value);
  }

  while (state.KeepRunningBatch(values.size())) {
    for (size_t i = 0; i < values.size(); ++i) {
      ::benchmark::DoNotOptimize(index->GetQualifyingStripes(values[i],
                                                             num_stripes));
    }
  }
}

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  const size_t generate_num_values = absl::GetFlag(FLAGS_generate_num_values);
  const size_t num_unique_values = absl::GetFlag(FLAGS_num_unique_values);
  const std::string input_csv_path = absl::GetFlag(FLAGS_input_csv_path);
  const std::vector<std::string>
      columns_to_test = absl::GetFlag(FLAGS_columns_to_test);

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
  const std::string sorting = absl::GetFlag(FLAGS_sorting);
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

  std::vector<std::unique_ptr<ci::IndexStructureFactory>> index_factories;
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING, ci::kMaxLoadFactor1SlotsPerBucket,
      /*scan_rate=*/0.01, /*slots_per_bucket=*/1,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(
      absl::make_unique<ci::PerStripeBloomFactory>(/*num_bits_per_key=*/10));
  index_factories.push_back(absl::make_unique<ci::PerStripeXorFactory>());

  // Set up the benchmarks.
  for (const std::unique_ptr<ci::Column>& column : table->GetColumns()) {
    for (size_t num_rows_per_stripe : {1ULL << 13, 1ULL << 16}) {
      for (const std::unique_ptr<ci::IndexStructureFactory>& factory :
           index_factories) {
        std::shared_ptr<ci::IndexStructure> index = absl::WrapUnique(
            factory->Create(*column, num_rows_per_stripe).release());
        const int num_stripes = column->num_rows() / num_rows_per_stripe;

        const std::string positive_distinct_lookup_benchmark_name =
            absl::StrFormat(/*format=*/"PositiveDistinctLookup/%s/%d/%s",
                            column->name(), num_rows_per_stripe, index->name());
        ::benchmark::RegisterBenchmark(
            positive_distinct_lookup_benchmark_name.c_str(),
            [&column, index, num_stripes](::benchmark::State& st) -> void {
              BM_PositiveDistinctLookup(*column, index, num_stripes, st);
            });

        const std::string negative_lookup_benchmark_name =
            absl::StrFormat(/*format=*/"NegativeLookup/%s/%d/%s",
                            column->name(), num_rows_per_stripe, index->name());
        ::benchmark::RegisterBenchmark(
            negative_lookup_benchmark_name.c_str(),
            [&column, index, num_stripes](::benchmark::State& st) -> void {
              BM_NegativeLookup(*column, index, num_stripes, st);
            });
      }
    }
  }

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return EXIT_SUCCESS;
}
