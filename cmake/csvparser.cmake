include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

FetchContent_Declare(
    csv-parser
    GIT_REPOSITORY "https://github.com/vincentlaucsb/csv-parser.git"
    GIT_TAG 6fb1f43ad43fc7962baa3b0fe524b282a56ae4b0
    PATCH_COMMAND ""
)
FetchContent_MakeAvailable(csv-parser)
FetchContent_GetProperties(csv-parser SOURCE_DIR CSV_PARSER_INCLUDE_DIR)
add_library(csv-parser INTERFACE)
target_include_directories(csv-parser INTERFACE ${CSV_PARSER_INCLUDE_DIR})
