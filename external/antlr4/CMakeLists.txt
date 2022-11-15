file(GLOB_RECURSE antlr4_runtime_source ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(antlr4 STATIC ${antlr4_runtime_source})
target_include_directories(antlr4 PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
