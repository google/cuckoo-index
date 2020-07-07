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
// -- --input_csv_path=Vehicle__Snowmobile__and_Boat_Registrations.csv
// --columns_to_test="City"
//
// add --benchmark_format=csv --undefok=benchmark_format to output in the CSV
// format.
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
#include "common/profiling.h"
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

constexpr absl::string_view kNoSorting = "NONE";
constexpr absl::string_view kByCardinalitySorting = "BY_CARDINALITY";
constexpr absl::string_view kRandomSorting = "RANDOM";

bool IsValidSorting(absl::string_view sorting) {
  static const auto* values = new absl::flat_hash_set<absl::string_view>(
      {kNoSorting, kByCardinalitySorting, kRandomSorting});

  return values->contains(sorting);
}

void BM_BuildTime(const ci::Column& column,
                  const ci::IndexStructureFactory& factory,
                  size_t num_rows_per_stripe, benchmark::State& state) {
  ci::Profiler::GetThreadInstance().Reset();

  for (auto _ : state) {
    benchmark::DoNotOptimize(factory.Create(column, num_rows_per_stripe));
  }

  state.counters["1-ValueToStripeBitmaps"] = benchmark::Counter(
      ci::Profiler::GetThreadInstance().GetValue(ci::Counter::ValueToStripeBitmaps),
      benchmark::Counter::kAvgIterations);
  state.counters["2-DistributeValues"] = benchmark::Counter(
      ci::Profiler::GetThreadInstance().GetValue(ci::Counter::DistributeValues),
      benchmark::Counter::kAvgIterations);
  state.counters["3-CreateSlots"] = benchmark::Counter(
      ci::Profiler::GetThreadInstance().GetValue(ci::Counter::CreateSlots),
      benchmark::Counter::kAvgIterations);
  state.counters["4-CreateFingerprintStore"] = benchmark::Counter(
      ci::Profiler::GetThreadInstance().GetValue(ci::Counter::CreateFingerprintStore),
      benchmark::Counter::kAvgIterations);
  state.counters["5-GetGlobalBitmap"] = benchmark::Counter(
      ci::Profiler::GetThreadInstance().GetValue(ci::Counter::GetGlobalBitmap),
      benchmark::Counter::kAvgIterations);
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

  ::benchmark::Initialize(&argc, argv);
  ::benchmark::RunSpecifiedBenchmarks();

  return EXIT_SUCCESS;
}
