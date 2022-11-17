cmake_minimum_required(VERSION 3.22)
project(dbs_tutorial)

set(CMAKE_CXX_STANDARD 20)

if (NOT UNIX OR APPLE)
    message(FATAL_ERROR "Only support Linux")
endif()

# libraries
include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()
add_subdirectory(external/cpp-terminal)
list(APPEND LIBS cpp-terminal antlr4-runtime fmt)
FIND_PACKAGE( Boost 1.80 REQUIRED )
include_directories(${Boost_INCLUDE_DIR})

# Generate grammar CPP files
execute_process(COMMAND ${PROJECT_SOURCE_DIR}/utils/generate_grammar.sh)

# project source
file(GLOB_RECURSE dbms_source src/**/*.cpp)
add_executable(main src/main.cpp ${dbms_source})
target_link_libraries(main ${LIBS})
target_include_directories(main PUBLIC src)

enable_testing()
find_package(GTest)
function(GTEST TEST_NAME)
    add_executable(${TEST_NAME} tests/${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} ${LIBS} GTest::gtest_main)
    gtest_discover_tests(${TEST_NAME})
endfunction()

# test cases
GTEST(basic)