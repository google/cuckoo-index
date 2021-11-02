# Dependencies
include(absl)
include(boost)
include(croaring)
include(csvparser)
include(leveldb)
include(protobuf)
include(xor_singleheader)

include_directories(${PROJECT_SOURCE_DIR})
include(common)

# Get Protobuf include dirs
get_target_property(protobuf_dirs libprotobuf INTERFACE_INCLUDE_DIRECTORIES)
foreach(dir IN LISTS protobuf_dirs)
  if ("${dir}" MATCHES "BUILD_INTERFACE")
    list(APPEND PROTO_DIRS "--proto_path=${dir}")
  endif()
endforeach()

# Generate Protobuf cpp sources
set(PROTO_HDRS)
set(PROTO_SRCS)
file(GLOB PROTO_FILES "${PROJECT_SOURCE_DIR}/*.proto")

foreach(PROTO_FILE IN LISTS PROTO_FILES)
  get_filename_component(PROTO_NAME ${PROTO_FILE} NAME_WE)
  set(PROTO_HDR ${PROJECT_SOURCE_DIR}/${PROTO_NAME}.pb.h)
  set(PROTO_SRC ${PROJECT_SOURCE_DIR}/${PROTO_NAME}.pb.cc)

  add_custom_command(
    OUTPUT ${PROTO_SRC} ${PROTO_HDR}
    COMMAND protoc
    "--proto_path=${PROJECT_SOURCE_DIR}"
    ${PROTO_DIRS}
    "--cpp_out=${PROJECT_SOURCE_DIR}"
    ${PROTO_FILE}
    DEPENDS ${PROTO_FILE} protoc
    COMMENT "Generate C++ protocol buffer for ${PROTO_FILE}"
    VERBATIM)
  list(APPEND PROTO_HDRS ${PROTO_HDR})
  list(APPEND PROTO_SRCS ${PROTO_SRC})
endforeach()

add_library(evaluation_cc_proto ${PROTO_SRCS} ${PROTO_HDRS})
target_link_libraries(evaluation_cc_proto libprotobuf)

add_library(data "${PROJECT_SOURCE_DIR}/data.cc" "${PROJECT_SOURCE_DIR}/data.h")
target_link_libraries(data
  evaluation_utils
  common_byte_coding
  Boost::math
  Boost::multiprecision
  absl::flat_hash_set
  absl::memory
  absl::random_random
  absl::strings
  csv-parser
)

add_library(per_stripe_bloom "${PROJECT_SOURCE_DIR}/per_stripe_bloom.h")
target_link_libraries(per_stripe_bloom
  data
  evaluation_utils
  index_structure
  absl::strings
  leveldb
)

add_library(xor_filter "${PROJECT_SOURCE_DIR}/xor_filter.h")
target_link_libraries(xor_filter
  absl::strings
  xor_singleheader
)

add_library(per_stripe_xor "${PROJECT_SOURCE_DIR}/per_stripe_xor.h")
target_link_libraries(per_stripe_xor
  data
  evaluation_utils
  index_structure
  absl::strings
  xor_singleheader
)

add_library(cuckoo_index "${PROJECT_SOURCE_DIR}/cuckoo_index.cc" "${PROJECT_SOURCE_DIR}/cuckoo_index.h")
target_link_libraries(cuckoo_index
  cuckoo_kicker
  cuckoo_utils
  evaluation_utils
  fingerprint_store
  index_structure
  common_byte_coding
  common_profiling
  common_rle_bitmap
  absl::flat_hash_map
  absl::memory
  absl::strings
)

add_library(cuckoo_kicker "${PROJECT_SOURCE_DIR}/cuckoo_kicker.cc" "${PROJECT_SOURCE_DIR}/cuckoo_kicker.h")
target_link_libraries(cuckoo_kicker
  cuckoo_utils
  absl::flat_hash_map
  absl::random_random
)

add_library(evaluation_utils "${PROJECT_SOURCE_DIR}/evaluation_utils.cc" "${PROJECT_SOURCE_DIR}/evaluation_utils.h")
target_link_libraries(evaluation_utils
  evaluation_cc_proto
  common_bitmap
  common_rle_bitmap
  croaring
  absl::memory
  absl::strings
  Boost::iostreams
)

add_library(cuckoo_utils "${PROJECT_SOURCE_DIR}/cuckoo_utils.cc" "${PROJECT_SOURCE_DIR}/cuckoo_utils.h")
target_link_libraries(cuckoo_utils
  common_bit_packing
  common_bitmap
  common_byte_coding
  croaring
  absl::flat_hash_set
  absl::city
  absl::memory
  absl::strings
  absl::str_format
)

add_library(fingerprint_store "${PROJECT_SOURCE_DIR}/fingerprint_store.cc" "${PROJECT_SOURCE_DIR}/fingerprint_store.h")
target_link_libraries(fingerprint_store
  cuckoo_utils
  evaluation_utils
  common_bitmap
  common_rle_bitmap
  absl::flat_hash_map
  absl::strings
)

add_library(index_structure "${PROJECT_SOURCE_DIR}/index_structure.h")
target_link_libraries(index_structure
  data
  evaluation_cc_proto
)

add_library(zone_map "${PROJECT_SOURCE_DIR}/zone_map.h")
target_link_libraries(zone_map
  data
  evaluation_utils
  index_structure
  absl::memory
  absl::strings
)

add_library(evaluator "${PROJECT_SOURCE_DIR}/evaluator.cc" "${PROJECT_SOURCE_DIR}/evaluator.h")
target_link_libraries(evaluator
  data
  evaluation_cc_proto
  index_structure
  absl::random_random
  absl::str_format
)

add_executable(evaluate "${PROJECT_SOURCE_DIR}/evaluate.cc")
target_link_libraries(evaluate 
  cuckoo_index
  cuckoo_utils
  data
  evaluation_cc_proto
  evaluation_utils
  evaluator
  index_structure
  per_stripe_bloom
  per_stripe_xor
  zone_map
  absl::flags
  absl::flags_parse
  absl::memory
  absl::strings
  absl::str_format
)
