include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

set(BUILD_TESTING OFF)
set(ABSL_ENABLE_INSTALL ON)
set(ABSL_USE_EXTERNAL_GOOGLETEST ON)
FetchContent_Declare(
        absl
        GIT_REPOSITORY "https://github.com/abseil/abseil-cpp.git"
        GIT_TAG df3ea785d8c30a9503321a3d35ee7d35808f190d
        PATCH_COMMAND ""
)
FetchContent_MakeAvailable(absl)
FetchContent_GetProperties(absl SOURCE_DIR ABSL_INCLUDE_DIR)
include_directories(${ABSL_INCLUDE_DIR})
