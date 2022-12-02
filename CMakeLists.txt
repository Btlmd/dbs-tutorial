cmake_minimum_required(VERSION 3.22)
project(dbs_tutorial)

set(CMAKE_CXX_STANDARD 20)

if (NOT UNIX OR APPLE)
    message(FATAL_ERROR "Only support Linux")
endif()

add_compile_definitions(BOOST_LOG_DYN_LINK)

add_compile_definitions(DEBUG)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
    message("Building a release version")
endif()

#set(CMAKE_CXX_FLAGS "-Wall -Wextra")
#set(CMAKE_CXX_FLAGS_DEBUG "-g")
#set(CMAKE_CXX_FLAGS_DEBUG "-O0")
#set(CMAKE_CXX_FLAGS_RELEASE "-O3")

# Libraries
    # From conan
    include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
    conan_basic_setup()
        # Boost
        FIND_PACKAGE(Boost 1.80 REQUIRED COMPONENTS log_setup log)
        include_directories(${Boost_INCLUDE_DIR})

    # From ./external
        # cpp-terminal
        set(CPPTERMINAL_ENABLE_TESING OFF CACHE BOOL "Disable test to avoid conflict" FORCE)
        set(CPPTERMINAL_BUILD_EXAMPLES OFF CACHE BOOL "Disable examples" FORCE)
        set(CPPTERMINAL_ENABLE_INSTALL OFF CACHE BOOL "Disable install" FORCE)
        add_subdirectory(external/cpp-terminal)

# All libraries
list(APPEND LIBS cpp-terminal antlr4-runtime fmt Boost::log_setup Boost::log)

# Generate grammar CPP files
execute_process(COMMAND ${PROJECT_SOURCE_DIR}/utils/generate_grammar.sh)

# project source
file(GLOB_RECURSE dbms_source src/**/*.cpp)
add_executable(main src/main.cpp ${dbms_source})
target_link_libraries(main ${LIBS})
target_include_directories(main PUBLIC src)

enable_testing()
find_package(GTest REQUIRED COMPONENTS gtest_main)
function(GTEST TEST_NAME)
    add_executable(${TEST_NAME} tests/${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} ${LIBS} GTest::gtest_main)
    gtest_discover_tests(${TEST_NAME})
endfunction()

# test cases
GTEST(basic)