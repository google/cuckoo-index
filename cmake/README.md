# CMake support

**NOTE** CMake support is community-based. The maintainers do not use CMake internally.

## Including Cuckoo Index in your CMake based project

You can include Cuckoo Index in your own CMake based project like this:
``` cmake
include(FetchContent)
set(CUCKOOINDEX_BUILD_TESTS OFF)
set(CUCKOOINDEX_BUILD_BENCHMARKS OFF)
FetchContent_Declare(
    cuckooindex
    GIT_REPOSITORY "https://github.com/google/cuckoo-index.git"
)
FetchContent_MakeAvailable(cuckooindex)
FetchContent_GetProperties(cuckooindex SOURCE_DIR CUCKOOINDEX_INCLUDE_DIR)
include_directories(${CUCKOOINDEX_INCLUDE_DIR})

target_link_libraries(your_target cuckoo_index)
```