cmake_minimum_required(VERSION 3.16)

project(CuckooIndex)

include (cmake/find/ccache.cmake)

message (STATUS
        "Building for: ${CMAKE_SYSTEM} ${CMAKE_SYSTEM_PROCESSOR} ${CMAKE_LIBRARY_ARCHITECTURE} ;
        CCACHE=${CCACHE_FOUND} ${CCACHE_VERSION}")

#set(CMAKE_VERBOSE_MAKEFILE ON)

add_subdirectory(csv-parser)
add_subdirectory(abseil-cpp)
add_subdirectory(CRoaring)

set(GOOGLETEST_VERSION 1.11.0)

add_subdirectory(googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)

target_compile_definitions(gtest INTERFACE GTEST_HAS_POSIX_RE=0)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")

message("finding pagedckages of boost")

set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
set(Boost_USE_DEBUG_LIBS         OFF) # ignore debug libs and 
set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs 
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF) 

#find_package(Boost 1.71 REQUIRED COMPONENTS math)
#find_package(Boost 1.71 REQUIRED COMPONENTS multiprecision)

message("finish finding boost packages")

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

#include(FetchContent)
#FetchContent_Declare(
#  absl
#  GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
#  #GIT_TAG xxxxx
#)

#
## CMake 3.14+
#FetchContent_MakeAvailable(absl)


#target_include_directories(data SYSTEM PUBLIC ${Boost_INCLUDE_DIRS})

add_library(cuckoo_index
        cuckoo_index.cc
        cuckoo_kicker.cc
        cuckoo_utils.cc
        common/byte_coding
        common/rle_bitmap.cc
        common/profiling.cc
        data.cc
        evaluation_utils.cc
        fingerprint_store.cc
        evaluation.pb.cc
        evaluation.proto
        )

target_include_directories(cuckoo_index PUBLIC
        .
        abseil-cpp
        common
        single
        CRoaring/include
        CRoaring/cpp
        csv-parser
        single
        )

#        /usr/include/
#        protobuf/src

#PUBLIC
target_link_libraries(
        cuckoo_index
        PUBLIC
        absl::flat_hash_map
        absl::memory
        absl::random_random
        absl::strings
        absl::strings_internal
        absl::str_format_internal
        absl_strings
        absl_strings_internal
        protobuf
        boost_iostreams
        csv
        roaring
)


add_library(cuckoo_index_test
        cuckoo_index_test.cc
        )

target_include_directories(cuckoo_index_test PUBLIC
        googletest/googletest
        googletest/googletest/include
        googletest/googlemock/include/)
target_link_libraries(
        cuckoo_index_test PUBLIC
        cuckoo_index
        gtest
        gmock)

# Configure playground
add_executable(
        simple_test
        simple_test.cpp
)

target_link_libraries(
        simple_test
        cuckoo_index
)







