//
// Created by daniel on 11/07/2021.
//


#include "cuckoo_index.h"
#include "data.h"
#include "index_structure.h"
#include "absl/memory/memory.h"



using namespace ci;

struct LookupResultSet {
 size_t num_true_negative_stripes = 0;
 size_t num_false_positive_stripes = 0;
};


bool VectorStripeContains(const std::vector<int>& data_, std::size_t num_rows_per_stripe, std::size_t stripe_id,
                    int value) {
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

void SimpleProbeAllStripes(const Column& column,
                                const IndexStructure& index, int value,
                                std::size_t num_rows_per_stripe,
                                std::size_t num_stripes,
                                std::size_t* num_true_negative_stripes,
                                std::size_t* num_false_positive_stripes) {
    for (size_t stripe_id = 0; stripe_id < num_stripes; ++stripe_id) {
        // Get expected result (ground truth).
//        const bool expected =
//                VectorStripeContains(column_data, num_rows_per_stripe, stripe_id, value);

        const bool expected = column.StripeContains(num_rows_per_stripe, stripe_id, value);

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

LookupResultSet DoPositiveUniformLookups(const Column& column,
                         const IndexStructure& index,
                         std::size_t num_rows_per_stripe,
                         std::size_t num_lookups) {
    std::mt19937 gen(42);
    auto column_data = column.data();
    const std::size_t num_stripes = column_data.size() / num_rows_per_stripe;
    std::size_t num_false_positive_stripes = 0;
    std::size_t num_true_negative_stripes = 0;
// Remove NULLs from the possible lookup values.
    column_data.erase(std::remove(column_data.begin(), column_data.end(),
                                  0),
                      column_data.end());

    std::uniform_int_distribution <std::size_t> row_offset_d(
            0, column_data.size() - 1);
    for (size_t i = 0; i < num_lookups; ++i) {
// Draw value from random row offset.
        const int value = column_data[row_offset_d(gen)];
        SimpleProbeAllStripes(column, index, value, num_rows_per_stripe, num_stripes,
                        &num_true_negative_stripes, &num_false_positive_stripes);
    }

    return LookupResultSet{num_true_negative_stripes, num_false_positive_stripes};
}

int main(int argc, char* argv[]) {

    const size_t generate_num_values = 100000;
    const size_t num_unique_values = 1000;
    const size_t num_rows_per_stripe = 10000;
    //const size_t num_rows_per_stripe = 1;
    const size_t num_lookups = 1000;

    std::unique_ptr<ci::CuckooIndexFactory> index_factory =
    absl::make_unique<ci::CuckooIndexFactory>(
            ci::CuckooAlgorithm::SKEWED_KICKING, ci::kMaxLoadFactor1SlotsPerBucket,
            /*scan_rate=*/0.001, /*slots_per_bucket=*/1,
            /*prefix_bits_optimization=*/false);

//    std::vector<int> column_data = {1,2,3,5,7,9,12};
//
//    auto column  = Column::IntColumn("test", column_data);
//
//    std::unique_ptr<IndexStructure> index =
//            index_factory->Create(*column, num_rows_per_stripe);
//
//    auto result_set = DoPositiveUniformLookups(
//            column_data, *index, num_rows_per_stripe, num_lookups
//    );

    const std::unique_ptr<Table> table = ci::GenerateUniformData(generate_num_values, num_unique_values);

    std::cout << "running tests for table with " << table->GetColumns().size() << "columns"  << std::endl;
    table->PrintHeader();
    for (const std::unique_ptr<Column>& column : table->GetColumns()) {
        std::cout << "run test for column: " << column->name() << std::endl;
        std::unique_ptr<IndexStructure> index =
                index_factory->Create(*column, num_rows_per_stripe);

//        auto data_copy = column->data();
//        auto result_set = DoPositiveUniformLookups(
//                data_copy, *index, num_rows_per_stripe, num_lookups
//                );

        auto result_set = DoPositiveUniformLookups(
                *column, *index, num_rows_per_stripe, num_lookups
        );

        std::cout << "ResultSet: num_false_positive_stripes: " << result_set.num_false_positive_stripes
        <<  ", num_false_positive_stripes: " << result_set.num_true_negative_stripes << std::endl;

    }
    return 0;
}