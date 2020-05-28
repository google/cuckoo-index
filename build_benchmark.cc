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
// File: build_benchmark.cc
// -----------------------------------------------------------------------------
//
// Benchmarks for build times of various `IndexStructure`s. Add new benchmark
// configs in the `main(..)` function.
//
// To run the benchmark run:
// bazel run -c opt --cxxopt='-std=c++17' --dynamic_mode=off :build_benchmark
// -- --input_file_path=Vehicle__Snowmobile__and_Boat_Registrations.csv
// --columns_to_test="City"
//
// Example run:
// Run on (12 X 4500 MHz CPU s)
// CPU Caches:
//   L1 Data 32 KiB (x6)
//   L1 Instruction 32 KiB (x6)
//   L2 Unified 1024 KiB (x6)
//   L3 Unified 8448 KiB (x1)
// Load Average: 1.61, 1.03, 0.77
// -------------------------------------------------------------------------
// Benchmark                                                            Time
// -------------------------------------------------------------------------
// BuildTime/City/16384/PerStripeBloom/10                            83.5 ns
// BuildTime/City/16384/CuckooIndex:1:0.49:0.02                       133 ns
// BuildTime/City/16384/CuckooIndex:1:0.84:0.02                      65.2 ns
// BuildTime/City/16384/CuckooIndex:1:0.95:0.02                      35.4 ns
// BuildTime/City/16384/CuckooIndex:1:0.98:0.02                      31.3 ns
// BuildTime/City/16384/PerStripeXor                                 9.50 ns
// BuildTime/City/65536/PerStripeBloom/10                            79.4 ns
// BuildTime/City/65536/CuckooIndex:1:0.49:0.02                       131 ns
// BuildTime/City/65536/CuckooIndex:1:0.84:0.02                      55.0 ns
// BuildTime/City/65536/CuckooIndex:1:0.95:0.02                      27.3 ns
// BuildTime/City/65536/CuckooIndex:1:0.98:0.02                      22.9 ns
// BuildTime/City/65536/PerStripeXor                                 6.42 ns
// BuildTime/Synthethic_1000000/16384/PerStripeBloom/10              64.3 ns
// BuildTime/Synthethic_1000000/16384/CuckooIndex:1:0.49:0.02        17.1 ns
// BuildTime/Synthethic_1000000/16384/CuckooIndex:1:0.84:0.02        13.7 ns
// BuildTime/Synthethic_1000000/16384/CuckooIndex:1:0.95:0.02        13.6 ns
// BuildTime/Synthethic_1000000/16384/CuckooIndex:1:0.98:0.02        14.1 ns
// BuildTime/Synthethic_1000000/16384/PerStripeXor                   4.15 ns
// BuildTime/Synthethic_1000000/65536/PerStripeBloom/10              64.2 ns
// BuildTime/Synthethic_1000000/65536/CuckooIndex:1:0.49:0.02        17.5 ns
// BuildTime/Synthethic_1000000/65536/CuckooIndex:1:0.84:0.02        12.8 ns
// BuildTime/Synthethic_1000000/65536/CuckooIndex:1:0.95:0.02        12.2 ns
// BuildTime/Synthethic_1000000/65536/CuckooIndex:1:0.98:0.02        19.3 ns
// BuildTime/Synthethic_1000000/65536/PerStripeXor                   3.76 ns
// BuildTime/Synthethic_10000000/16384/PerStripeBloom/10             64.4 ns
// BuildTime/Synthethic_10000000/16384/CuckooIndex:1:0.49:0.02       15.3 ns
// BuildTime/Synthethic_10000000/16384/CuckooIndex:1:0.84:0.02       13.5 ns
// BuildTime/Synthethic_10000000/16384/CuckooIndex:1:0.95:0.02       13.1 ns
// BuildTime/Synthethic_10000000/16384/CuckooIndex:1:0.98:0.02       13.3 ns
// BuildTime/Synthethic_10000000/16384/PerStripeXor                  4.17 ns
// BuildTime/Synthethic_10000000/65536/PerStripeBloom/10             65.1 ns
// BuildTime/Synthethic_10000000/65536/CuckooIndex:1:0.49:0.02       14.4 ns
// BuildTime/Synthethic_10000000/65536/CuckooIndex:1:0.84:0.02       12.3 ns
// BuildTime/Synthethic_10000000/65536/CuckooIndex:1:0.95:0.02       11.7 ns
// BuildTime/Synthethic_10000000/65536/CuckooIndex:1:0.98:0.02       11.8 ns
// BuildTime/Synthethic_10000000/65536/PerStripeXor                  3.83 ns

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
ABSL_FLAG(std::vector<std::string>, num_rows_per_stripe,
          std::vector<std::string>({absl::StrCat(1ULL << 14),
                                    absl::StrCat(1ULL << 16)}),
          "Number of rows per stripe. Defaults to 10,000.");
ABSL_FLAG(std::vector<std::string>, synthetic_dataset_sizes,
          std::vector<std::string>({"1000000", "10000000"}),
          "Sizes of synthetic datasets to test.");
ABSL_FLAG(std::string, sorting, "NONE",
          "Sorting to apply to the data. Supported values: 'NONE', "
          "'BY_CARDINALITY' (sorts lexicographically, starting with columns "
          "with the lowest cardinality), 'RANDOM'");

constexpr absl::string_view kNoSorting = "NONE";
constexpr absl::string_view kByCardinalitySorting = "BY_CARDINALITY";
constexpr absl::string_view kRandomSorting = "RANDOM";

bool IsValidSorting(absl::string_view sorting) {
  static const auto* values = new absl::flat_hash_set<absl::string_view>(
      {kNoSorting, kByCardinalitySorting, kRandomSorting});

  return values->contains(sorting);
}

ci::ColumnPtr CreateSyntheticColumn(size_t size) {
  std::mt19937 gen(42);

  std::vector<int> values;
  values.reserve(size);
  for (size_t i = 0; i < size; ++i) {
    // Draw value from random (Zipf distributed) offset.
    // absl::Zipf is used with its default parameters q = 2.0 and v = 1.0.
    // q (>= 1.0) is what's referred to as the distribution parameter a in other
    // implementations (e.g., numpy.random.zipf).
    values.push_back(absl::Zipf(gen, size, /*q=*/2.0));
  }

  return ci::Column::IntColumn(absl::StrCat("Synthethic_", size), values);
}

void BM_BuildTime(const ci::Column& column,
                  const ci::IndexStructureFactory& factory,
                  size_t num_rows_per_stripe, benchmark::State& state) {
  while (state.KeepRunningBatch(column.num_rows())) {
    benchmark::DoNotOptimize(factory.Create(column, num_rows_per_stripe));
  }
}

int main(int argc, char* argv[]) {
  absl::ParseCommandLine(argc, argv);

  std::vector<std::unique_ptr<ci::Table>> tables;

  // Potentially add real data.
  bool runOnRealData = !absl::GetFlag(FLAGS_input_file_path).empty() &&
                       !absl::GetFlag(FLAGS_columns_to_test).empty();
  if (runOnRealData) {
    tables.push_back(ci::Table::FromCsv(absl::GetFlag(FLAGS_input_file_path),
                                        absl::GetFlag(FLAGS_columns_to_test)));
  } else {
    std::cerr
        << "[WARNING] --input_file_path or --columns_to_test not specified, "
           "running synthetic benchmarks only."
        << std::endl;
  }

  // Add synthetic data.
  for (const std::string& dataset_size :
       absl::GetFlag(FLAGS_synthetic_dataset_sizes)) {
    std::vector<std::unique_ptr<ci::Column>> columns;
    columns.push_back(CreateSyntheticColumn(std::stoull(dataset_size)));
    tables.push_back(ci::Table::Create(
        /*name=*/"", std::move(columns)));
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
    for (std::unique_ptr<ci::Table>& table : tables) {
      table->SortWithCardinalityKey();
    }
  } else if (sorting == kRandomSorting) {
    std::cerr << "Randomly shuffling the table..." << std::endl;
    for (std::unique_ptr<ci::Table>& table : tables) {
      table->Shuffle();
    }
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
  for (const std::unique_ptr<ci::Table>& table : tables) {
    for (const std::unique_ptr<ci::Column>& column : table->GetColumns()) {
      for (const std::string& num_rows_per_stripe_string :
           absl::GetFlag(FLAGS_num_rows_per_stripe)) {
        size_t num_rows_per_stripe = std::stoull(num_rows_per_stripe_string);
        for (const std::unique_ptr<ci::IndexStructureFactory>& factory :
             index_factories) {
          const std::string benchmark_name =
              absl::StrFormat(/*format=*/"BuildTime/%s/%d/%s", column->name(),
                              num_rows_per_stripe, factory->index_name());
          ::benchmark::RegisterBenchmark(
              benchmark_name.c_str(),
              [&column, &factory,
               num_rows_per_stripe](::benchmark::State& st) -> void {
                BM_BuildTime(*column, *factory, num_rows_per_stripe, st);
              });
        }
      }
    }
  }

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return EXIT_SUCCESS;
}
