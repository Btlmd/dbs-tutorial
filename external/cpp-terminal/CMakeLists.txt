project(cpp-terminal
        VERSION 1.0.0
        DESCRIPTION "Small header only C++ library for writing multiplatform terminal applications"
        HOMEPAGE_URL "https://github.com/jupyter-xeus/cpp-terminal"
        LANGUAGES CXX)

# configure version information
configure_file(version.hpp.in version.hpp)

# create and configure library target
add_library(cpp-terminal base.cpp prompt.cpp window.cpp input.cpp private/platform.cpp)

target_include_directories(cpp-terminal PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/../)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../)
