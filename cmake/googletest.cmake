include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
set(BUILD_GTEST OFF CACHE BOOL "" FORCE)
FetchContent_Declare(
        gtest
        GIT_REPOSITORY "https://github.com/google/googletest.git"
        GIT_TAG 10b1902d893ea8cc43c69541d70868f91af3646b
        PATCH_COMMAND ""
)
FetchContent_MakeAvailable(gtest)
FetchContent_GetProperties(gtest SOURCE_DIR GTEST_INCLUDE_DIR)
include_directories(${GTEST_INCLUDE_DIR}/googlemock/include)
include_directories(${GTEST_INCLUDE_DIR}/googletest/include)