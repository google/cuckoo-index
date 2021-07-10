cmake_minimum_required(VERSION 3.16)

project(CuckooIndex)

add_subdirectory(csv-parser)
add_subdirectory(abseil-cpp)
add_subdirectory(CRoaring)

set(GOOGLETEST_VERSION 1.11.0)
#add_subdirectory(googletest/googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)

add_subdirectory(googletest ${CMAKE_CURRENT_BINARY_DIR}/googletest)

#add_subdirectory(googletest/googlemock)
target_compile_definitions(gtest INTERFACE GTEST_HAS_POSIX_RE=0)

add_library(data data.cc cuckoo_index.cc cuckoo_kicker.cc cuckoo_utils.cc common/byte_coding)

#target_include_directories(data PUBLIC common)
target_include_directories(data PUBLIC .)
target_include_directories(data PUBLIC abseil-cpp)
target_include_directories(data PUBLIC CRoaring/include ./CRoaring/cpp csv-parser)

target_link_libraries(data absl::flat_hash_map absl::memory absl::random_random absl::strings csv)

#add_compile_options(-std=c++17)
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

add_library(data_test data_test.cc)
target_include_directories(data_test PUBLIC googletest/googletest googletest/googletest/include googletest/googlemock/include/)
target_link_libraries(data_test PUBLIC data gtest gmock)








