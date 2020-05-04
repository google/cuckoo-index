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
// -- --input_file_path='...' --columns_to_test='A,B,C'
//
// Example run:
// Run on (12 X 4500 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x6)
//   L1 Instruction 32 KiB (x6)
//   L2 Unified 1024 KiB (x6)
//   L3 Unified 8448 KiB (x1)
// Load Average: 0.70, 0.29, 0.22
// Benchmark                                                           Time(ns)
// -----------------------------------------------------------------------------
// PositiveDistinctLookup/country_code/16384/PerStripeBloom/10            14911
// NegativeLookup/country_code/16384/PerStripeBloom/10                    15312
// PositiveDistinctLookup/country_code/16384/CLTSecondary/10               5752
// NegativeLookup/country_code/16384/CLTSecondary/10                       6007
// PositiveDistinctLookup/country_code/16384/CuckooIndex:1:0.49:0.02      25492
// NegativeLookup/country_code/16384/CuckooIndex:1:0.49:0.02              22125
// PositiveDistinctLookup/country_code/16384/CuckooIndex:1:0.84:0.02     483760
// NegativeLookup/country_code/16384/CuckooIndex:1:0.84:0.02             771757
// PositiveDistinctLookup/country_code/16384/CuckooIndex:1:0.95:0.02     296864
// NegativeLookup/country_code/16384/CuckooIndex:1:0.95:0.02             674640
// PositiveDistinctLookup/country_code/16384/CuckooIndex:1:0.98:0.02     280902
// NegativeLookup/country_code/16384/CuckooIndex:1:0.98:0.02             766191
// PositiveDistinctLookup/country_code/16384/PerStripeXor                  1830
// NegativeLookup/country_code/16384/PerStripeXor                          1833
// PositiveDistinctLookup/country_code/65536/PerStripeBloom/10             3820
// NegativeLookup/country_code/65536/PerStripeBloom/10                     3804
// PositiveDistinctLookup/country_code/65536/CLTSecondary/10               1444
// NegativeLookup/country_code/65536/CLTSecondary/10                       1453
// PositiveDistinctLookup/country_code/65536/CuckooIndex:1:0.49:0.02       6010
// NegativeLookup/country_code/65536/CuckooIndex:1:0.49:0.02               5458
// PositiveDistinctLookup/country_code/65536/CuckooIndex:1:0.84:0.02     143072
// NegativeLookup/country_code/65536/CuckooIndex:1:0.84:0.02             240090
// PositiveDistinctLookup/country_code/65536/CuckooIndex:1:0.95:0.02      79689
// NegativeLookup/country_code/65536/CuckooIndex:1:0.95:0.02             183355
// PositiveDistinctLookup/country_code/65536/CuckooIndex:1:0.98:0.02      72320
// NegativeLookup/country_code/65536/CuckooIndex:1:0.98:0.02             211880
// PositiveDistinctLookup/country_code/65536/PerStripeXor                   433
// NegativeLookup/country_code/65536/PerStripeXor                           433

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

ABSL_FLAG(std::string, input_file_path, "", "Path to the Capacitor file.");
ABSL_FLAG(std::vector<std::string>, columns_to_test, {},
          "Comma-separated list of columns to tests, e.g. "
          "'company_name,country_code'.");
ABSL_FLAG(std::vector<std::string>, num_rows_per_stripe_to_test, {"10000"},
          "Number of rows per stripe. Defaults to 10,000.");
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

// Probes the first `num_stripes` in the given `index` for the `value`.
void ProbeAllStripes(const ci::IndexStructure& index, int value,
                     int num_stripes) {
  for (size_t stripe_id = 0; stripe_id < static_cast<size_t>(num_stripes);
       ++stripe_id) {
    ::benchmark::DoNotOptimize(index.StripeContains(stripe_id, value));
  }
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
      ProbeAllStripes(*index, values[i], num_stripes);
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
      ProbeAllStripes(*index, values[i], num_stripes);
    }
  }
}

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);
  if (absl::GetFlag(FLAGS_input_file_path).empty()) {
    std::cerr << "You must specify --input_file_path" << std::endl;
    std::exit(EXIT_FAILURE);
  }

  // Define data.
  const std::vector<std::string> column_names =
      absl::GetFlag(FLAGS_columns_to_test);
  std::unique_ptr<ci::Table> table =
      ci::Table::FromCsv(absl::GetFlag(FLAGS_input_file_path), column_names);

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
  index_factories.push_back(
      absl::make_unique<ci::PerStripeBloomFactory>(/*num_bits_per_key=*/10));
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING,
      /*max_load_factor=*/ci::kMaxLoadFactor1SlotsPerBucket,
      /*scan_rate=*/0.02, /*slots_per_bucket=*/1,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING,
      /*max_load_factor=*/ci::kMaxLoadFactor2SlotsPerBucket,
      /*scan_rate=*/0.02, /*slots_per_bucket=*/2,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING,
      /*max_load_factor=*/ci::kMaxLoadFactor4SlotsPerBucket,
      /*scan_rate=*/0.02, /*slots_per_bucket=*/4,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(absl::make_unique<ci::CuckooIndexFactory>(
      ci::CuckooAlgorithm::SKEWED_KICKING,
      /*max_load_factor=*/ci::kMaxLoadFactor8SlotsPerBucket,
      /*scan_rate=*/0.02, /*slots_per_bucket=*/8,
      /*prefix_bits_optimization=*/false));
  index_factories.push_back(absl::make_unique<ci::PerStripeXorFactory>());

  // Set up the benchmarks.
  for (const std::unique_ptr<ci::Column>& column : table->GetColumns()) {
    for (size_t num_rows_per_stripe : {1ULL << 14, 1ULL << 16}) {
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
