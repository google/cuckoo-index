include(ExternalProject)
find_package(Git REQUIRED)

# Need to use ExternalProject_Add to use custom BUILD_COMMAND
ExternalProject_Add(
        croaring_src
        PREFIX "_deps/croaring"
        GIT_REPOSITORY "https://github.com/lemire/CroaringUnityBuild.git"
        GIT_TAG  c1d1a754faa6451436efaffa3fe449edc7710b65
        TIMEOUT 10
        CONFIGURE_COMMAND ""
        UPDATE_COMMAND    ""
        INSTALL_COMMAND   ""
        BUILD_ALWAYS      OFF
        BUILD_COMMAND     ${CMAKE_CXX_COMPILER} -c <SOURCE_DIR>/roaring.c -o libcroaring.o
        COMMAND           ${CMAKE_COMMAND} -E copy <BINARY_DIR>/libcroaring.o <INSTALL_DIR>/lib/libcroaring.o
)

ExternalProject_Get_Property(croaring_src install_dir)
set(CROARING_INCLUDE_DIR ${install_dir}/src/croaring_src)
set(CROARING_LIBRARY_PATH ${install_dir}/lib/libcroaring.o)
file(MAKE_DIRECTORY ${CROARING_INCLUDE_DIR})
add_library(croaring STATIC IMPORTED)
set_property(TARGET croaring PROPERTY IMPORTED_LOCATION ${CROARING_LIBRARY_PATH})
set_property(TARGET croaring APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${CROARING_INCLUDE_DIR})

add_dependencies(croaring croaring_src)