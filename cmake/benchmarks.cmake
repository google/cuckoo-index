include(benchmark)

add_executable(build_benchmark "${PROJECT_SOURCE_DIR}/build_benchmark.cc")
target_link_libraries(build_benchmark 
  cuckoo_index
  cuckoo_utils
  index_structure
  per_stripe_bloom
  per_stripe_xor
  common_profiling
  absl::flags
  absl::flags_parse
  benchmark
  gtest
)

add_executable(lookup_benchmark "${PROJECT_SOURCE_DIR}/lookup_benchmark.cc")
target_link_libraries(lookup_benchmark 
  cuckoo_index
  cuckoo_utils
  index_structure
  per_stripe_bloom
  per_stripe_xor
  absl::flags
  absl::flags_parse
  benchmark
  gtest
)