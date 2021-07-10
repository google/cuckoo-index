project(CuckooIndex)

cmake_minimum_required(VERSION 3.16)

add_library(data data.cc common/byte_coding)

add_subdirectory(csv-parser)

message("finding packages of boost")

set(Boost_USE_STATIC_LIBS        ON)  # only find static libs
set(Boost_USE_DEBUG_LIBS         OFF) # ignore debug libs and 
set(Boost_USE_RELEASE_LIBS       ON)  # only find release libs 
set(Boost_USE_MULTITHREADED      ON)
set(Boost_USE_STATIC_RUNTIME    OFF) 

#find_package(Boost 1.71 REQUIRED COMPONENTS math)
#find_package(Boost 1.71 REQUIRED COMPONENTS multiprecision)

message("finish finding boost packages")

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

#target_link_libraries(data PUBLIC boost::math boost::multiprecision)

add_executable(data_test data_test.cc)
#target_link_libraries(data_test PUBLIC data gtest_main)
target_link_libraries(data_test PUBLIC data)


