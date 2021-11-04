# Note: CMake support is community-based. The maintainers do not use CMake internally.

cmake_minimum_required(VERSION 3.18) # For SOURCE_SUBDIR in FetchContent_Declare 

project(cuckooindex CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
add_compile_options(-Wall -Wextra)

enable_testing()

option(CUCKOOINDEX_BUILD_TESTS "Builds the cuckoo index tests." ON)
option(CUCKOOINDEX_BUILD_BENCHMARKS "Builds the cuckoo index benchmarks if the tests are built as well." ON)

if(CUCKOOINDEX_BUILD_TESTS)
  include(tests)

  if(CUCKOOINDEX_BUILD_BENCHMARKS)
    include(benchmarks)
  endif()
endif()

include(cuckooindex)
