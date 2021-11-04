include(FetchContent)
set(FETCHCONTENT_QUIET ON)
set(FETCHCONTENT_UPDATES_DISCONNECTED ON)
set(BUILD_SHARED_LIBS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
find_package(Git REQUIRED)

FetchContent_Declare(
    xor_singleheader
    GIT_REPOSITORY "https://github.com/FastFilter/xor_singleheader.git"
    GIT_TAG 6cea6a4dcf2f18a0e3b9b9e0b94d6012b804ffa1
    PATCH_COMMAND ""
)
FetchContent_MakeAvailable(xor_singleheader)
FetchContent_GetProperties(xor_singleheader SOURCE_DIR XOR_SINGLEHEADER_INCLUDE_DIR)
add_library(xor_singleheader INTERFACE)
target_include_directories(xor_singleheader INTERFACE ${XOR_SINGLEHEADER_INCLUDE_DIR})
