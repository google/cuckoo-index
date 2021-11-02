include(googletest)

add_executable(data_test "${PROJECT_SOURCE_DIR}/data_test.cc")
target_link_libraries(data_test 
  data
  gtest_main
)

add_executable(per_stripe_bloom_test "${PROJECT_SOURCE_DIR}/per_stripe_bloom_test.cc")
target_link_libraries(per_stripe_bloom_test 
  per_stripe_bloom
  gtest_main
)

add_executable(xor_filter_test "${PROJECT_SOURCE_DIR}/xor_filter_test.cc")
target_link_libraries(xor_filter_test 
  xor_filter
  gtest_main
)

add_executable(per_stripe_xor_test "${PROJECT_SOURCE_DIR}/per_stripe_xor_test.cc")
target_link_libraries(per_stripe_xor_test 
  per_stripe_xor
  gtest_main
)

add_executable(cuckoo_index_test "${PROJECT_SOURCE_DIR}/cuckoo_index_test.cc")
target_link_libraries(cuckoo_index_test 
  cuckoo_index
  gtest_main
)

add_executable(cuckoo_kicker_test "${PROJECT_SOURCE_DIR}/cuckoo_kicker_test.cc")
target_link_libraries(cuckoo_kicker_test 
  cuckoo_kicker
  gtest_main
)

add_executable(evaluation_utils_test "${PROJECT_SOURCE_DIR}/evaluation_utils_test.cc")
target_link_libraries(evaluation_utils_test 
  evaluation_utils
  gtest_main
)

add_executable(cuckoo_utils_test "${PROJECT_SOURCE_DIR}/cuckoo_utils_test.cc")
target_link_libraries(cuckoo_utils_test 
  cuckoo_utils
  evaluation_utils
  gtest_main
)

add_executable(fingerprint_store_test "${PROJECT_SOURCE_DIR}/fingerprint_store_test.cc")
target_link_libraries(fingerprint_store_test 
  fingerprint_store
  gtest_main
)

add_executable(zone_map_test "${PROJECT_SOURCE_DIR}/zone_map_test.cc")
target_link_libraries(zone_map_test 
  zone_map
  gtest_main
)

add_executable(bitmap_benchmark_test "${PROJECT_SOURCE_DIR}/bitmap_benchmark_test.cc")
target_link_libraries(bitmap_benchmark_test 
  evaluation_utils
  common_bitmap
  common_rle_bitmap
  croaring
  absl::flags
  absl::memory
  absl::strings
  benchmark
  gtest_main
)