add_library(common_byte_coding "${PROJECT_SOURCE_DIR}/common/byte_coding.h")
target_link_libraries(common_byte_coding
  absl::strings
  absl::span
  libprotobuf-lite
)

add_library(common_bit_packing "${PROJECT_SOURCE_DIR}/common/bit_packing.h")
target_link_libraries(common_bit_packing
  common_byte_coding
  absl::core_headers
  absl::endian
  absl::span
)

add_library(common_bitmap "${PROJECT_SOURCE_DIR}/common/bitmap.h")
target_link_libraries(common_bitmap
  absl::strings
  Boost::dynamic_bitset
)

add_library(common_profiling "${PROJECT_SOURCE_DIR}/common/profiling.cc" "${PROJECT_SOURCE_DIR}/common/profiling.h")
target_link_libraries(common_profiling
  absl::flat_hash_map
  absl::time
)

add_library(common_rle_bitmap "${PROJECT_SOURCE_DIR}/common/rle_bitmap.cc" "${PROJECT_SOURCE_DIR}/common/rle_bitmap.h")
target_link_libraries(common_rle_bitmap
  common_bit_packing
  common_bitmap
  absl::strings
)
