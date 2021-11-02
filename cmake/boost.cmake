include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

# This is a different version than the one used in the bazel BUILD as it is the first one to support FetchContent_Declare
# The version in the bazel BUILD can't be updated to this version, though, as boost math is currently in a broken state there
FetchContent_Declare(
        boost
        GIT_REPOSITORY "https://github.com/boostorg/boost.git"
        GIT_TAG boost-1.77.0
        PATCH_COMMAND cd <SOURCE_DIR>/libs/math && git checkout v1.77-standalone # Boost math is broken in 1.77.0, apply fixed patch
)
FetchContent_MakeAvailable(boost)
FetchContent_GetProperties(boost SOURCE_DIR BOOST_INCLUDE_DIR)
include_directories(${BOOST_INCLUDE_DIR})
