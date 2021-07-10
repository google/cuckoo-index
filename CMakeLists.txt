project(CuckooIndex)

add_library(data data.cc common/byte_coding)

add_subdirectory(csv-parser)

find_package(Boost 1.71 REQUIRED COMPONENTS math)
find_package(Boost 1.71 REQUIRED COMPONENTS multiprecision)

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})

include(FetchContent)
FetchContent_Declare(
  absl
  GIT_REPOSITORY https://github.com/abseil/abseil-cpp.git
  #GIT_TAG xxxxx
)

# CMake 3.14+
FetchContent_MakeAvailable(absl)


#target_include_directories(data SYSTEM PUBLIC ${Boost_INCLUDE_DIRS})

target_link_libraries(data PUBLIC boost::math boost::multiprecision)

