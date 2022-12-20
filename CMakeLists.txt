cmake_minimum_required(VERSION 3.22)
project(dbs_tutorial)

set(CMAKE_CXX_STANDARD 20)

if (NOT UNIX OR APPLE)
    message(FATAL_ERROR "Only support Linux")
endif()

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
        FIND_PACKAGE(Boost 1.80 REQUIRED COMPONENTS log_setup log ${BOOST_PYTHONLIB})
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
#execute_process(COMMAND ${PROJECT_SOURCE_DIR}/utils/generate_grammar.sh)

# Project source
file(GLOB_RECURSE dbms_source src/**/*.cpp)

## `main` static library
#add_library(main STATIC ${dbms_source})
#target_link_libraries(main ${LIBS})
#target_include_directories(main PUBLIC src)

# `main` shared library
add_library(main_shared SHARED ${dbms_source})
target_link_libraries(main_shared ${LIBS})
target_include_directories(main_shared PUBLIC src)

# `front` target
add_executable(front src/main.cpp)
target_link_libraries(front main_shared)

# `debug` target
add_executable(debug src/debug.cpp)
target_link_libraries(debug main_shared)

add_executable(index_debug src/index_debug.cpp)
target_link_libraries(index_debug main_shared)

FIND_PACKAGE(PythonInterp)
FIND_PACKAGE(Boost COMPONENTS python${PYTHON_VERSION_SUFFIX})
FIND_PACKAGE(PythonInterp 3)
FIND_PACKAGE(PythonLibs 3 REQUIRED)

message(STATUS "PYTHON_LIBRARIES = ${PYTHON_LIBRARIES}")
message(STATUS "PYTHON_EXECUTABLE = ${PYTHON_EXECUTABLE}")
message(STATUS "PYTHON_INCLUDE_DIRS = ${PYTHON_INCLUDE_DIRS}")
message(STATUS "Boost_LIBRARIES = ${Boost_LIBRARIES}")

# `connect_db` target
python_add_module(connect_db tests/python_utils/connect.cpp)
target_include_directories(connect_db PUBLIC ${Boost_INCLUDE_DIRS} ${PYTHON_INCLUDE_DIRS})
target_link_libraries(connect_db main_shared ${Boost_LIBRARIES} ${PYTHON_LIBRARIES})

# tests
enable_testing()

# generate sql
execute_process(COMMAND ${PROJECT_SOURCE_DIR}/utils/generate_sql_tests.sh )

file(COPY tests/python/compare.py DESTINATION ${CMAKE_LIBRARY_OUTPUT_DIRECTORY})

file(GLOB_RECURSE files ${PROJECT_SOURCE_DIR}/tests/sql/*.sql)
add_test(NAME python-version COMMAND python3 --version)
foreach(file ${files})
    get_filename_component(file_name ${file} NAME)
    message(" > ADD TEST ${file_name}")
    add_test(NAME ${file_name} COMMAND python3 ${CMAKE_LIBRARY_OUTPUT_DIRECTORY}/compare.py --host localhost --user root --password root --sql ${file})
endforeach()

# unit tests
find_package(GTest REQUIRED COMPONENTS gtest_main)
function(GTEST TEST_NAME)
    add_executable(${TEST_NAME} tests/test_${TEST_NAME}.cpp)
    target_link_libraries(${TEST_NAME} main_shared GTest::gtest_main)
    gtest_discover_tests(${TEST_NAME})
    message(" > ADD GTEST ${TEST_NAME}")
endfunction()

GTEST(data_page)
