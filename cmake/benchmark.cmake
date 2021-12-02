include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        benchmark
        GIT_REPOSITORY "https://github.com/google/benchmark.git"
        GIT_TAG 090faecb454fbd6e6e17a75ef8146acb037118d4
        PATCH_COMMAND ""
)
FetchContent_MakeAvailable(benchmark)
FetchContent_GetProperties(benchmark SOURCE_DIR BENCHMARK_INCLUDE_DIR)
include_directories(${BENCHMARK_INCLUDE_DIR})