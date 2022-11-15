cmake_minimum_required(VERSION 3.22)
project(dbs_tutorial)

set(CMAKE_CXX_STANDARD 20)

# libraries
add_subdirectory(external/antlr4)
add_subdirectory(external/cpp-terminal)
list(APPEND LIBS cpp-terminal antlr4)

# project source
file(GLOB_RECURSE dbms_source src/**/*.cpp)
add_executable(main src/main.cpp ${dbms_source})
target_link_libraries(main ${LIBS})
target_include_directories(main PUBLIC src)

# google test
include(FetchContent)
FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/0e6aac2571eb1753b8855d8d1f592df64d1a4828.zip
)
FetchContent_MakeAvailable(googletest)
include(GoogleTest)
enable_testing()
function(GTEST TEST_NAME)
    add_executable(${TEST_NAME} tests/${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} ${LIBS} GTest::gtest_main)
    gtest_discover_tests(${TEST_NAME})
endfunction()

# test cases
GTEST(basic)