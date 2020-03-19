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
// File: evaluator.h
// -----------------------------------------------------------------------------

#ifndef CUCKOO_INDEX_EVALUATOR_H
#define CUCKOO_INDEX_EVALUATOR_H

#include <random>

#include "data.h"
#include "evaluation.pb.h"
#include "index_structure.h"

namespace ci {

class Evaluator {
 public:
  // Runs experiments for the given parameters and returns their results.
  //
  // Selected experiments `test_cases` (e.g. positive uniform look-ups) are run
  // for a pair of (column, num_rows_per_stripe). `index_structure_factories`
  // are used to create an index structure per experiment.
  std::vector<ci::EvaluationResults> RunExperiments(
      std::vector<std::unique_ptr<IndexStructureFactory>>
          index_structure_factories,
      const std::unique_ptr<Table>& table,
      const std::vector<size_t>& num_rows_per_stripe_to_test,
      size_t num_lookups, const std::vector<std::string>& test_cases);

 private:
  // Performs positive lookups with values drawn from random row offsets. This
  // assumes that positive lookup values follow the same distribution than
  // stored values, i.e., frequent values are queried frequently.
  ci::EvaluationResults::TestCase DoPositiveUniformLookups(
      const Column& column, const IndexStructure& index_structure,
      std::size_t num_rows_per_stripe, std::size_t num_lookups);

  // Performs positive lookups with a subset of all distinct values (chosen
  // uniformly at random). This means, e.g., that lookup values that only occur
  // in a single stripe can cause up to N-1 false positive stripes. Values that
  // occur in all stripes, on the other hand, cannot cause any false positive
  // stripes.
  ci::EvaluationResults::TestCase DoPositiveDistinctLookups(
      const Column& column, const IndexStructure& index,
      std::size_t num_rows_per_stripe, std::size_t num_lookups);

  // Performs positive lookups with a subset of all distinct values (chosen
  // based on a Zipf distribution).
  ci::EvaluationResults::TestCase DoPositiveZipfLookups(
      const Column& column, const IndexStructure& index,
      std::size_t num_rows_per_stripe, std::size_t num_lookups);

  // Performs negative lookups with random values not present the `column`.
  // Note that in this test case, ZoneMaps will be 100% effective for
  // dict-encoded string columns as lookup keys will have values >= number of
  // distinct strings (i.e., they'll be outside of the domain of the
  // dict-encoded column). Alternatively, we could exclude one stripe and use
  // distinct values that only occur in that stripe as lookup values. However,
  // this wouldn't necessarily produce the same results as if we wouldn't
  // dict-encode string columns in the first place (particularly it depends on
  // which stripe we exclude). Also, this strategy wouldn't work for low
  // cardinality columns. Therefore, we've decided to not evaluate ZoneMaps on
  // string columns with negative lookup keys and use a rather simple lookup key
  // generation here that only ensures that a lookup key doesn't occur in any
  // stripe.
  ci::EvaluationResults::TestCase DoNegativeLookups(
      const Column& column, const IndexStructure& index,
      std::size_t num_rows_per_stripe, std::size_t num_lookups);

  // Performs lookups with a mix between positive (chosen from distinct
  // values like in DoPositiveDistinctLookups) and negative lookup keys.
  // `hit_rate` determines the share of positive lookups (e.g., 0.1 means that
  // 10% of the lookup keys are positive, i.e., are at least present in one
  // stripe).
  ci::EvaluationResults::TestCase DoMixedLookups(
      const Column& column, const IndexStructure& index,
      std::size_t num_rows_per_stripe, std::size_t num_lookups,
      double hit_rate);

  // Probes all stripes for the given `column` and `index`, and updates
  // `num_true_negative_stripes` (ground truth true negatives) and
  // `num_false_positive_stripes` (number of times the `index` did not prune
  // a stripe even though it could have).
  void ProbeAllStripes(const Column& column, const IndexStructure& index,
                       int value, std::size_t num_rows_per_stripe,
                       std::size_t num_stripes,
                       std::size_t* num_true_negative_stripes,
                       std::size_t* num_false_positive_stripes);
};

}  // namespace ci

#endif  // CUCKOO_INDEX_EVALUATOR_H
