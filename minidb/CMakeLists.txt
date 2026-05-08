cmake_minimum_required(VERSION 3.20)

project(
  MiniDB
  VERSION 0.1.0
  LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

add_library(minidb_core
  src/minidb/ddl_parser.cpp
  src/minidb/greeting.cpp
)
target_include_directories(minidb_core PUBLIC include)

add_executable(minidb src/main.cpp)
target_link_libraries(minidb PRIVATE minidb_core)

include(CTest)

if(BUILD_TESTING)
  find_package(Catch2 3 REQUIRED)

  add_executable(minidb_tests
    tests/ddl_ast_test.cpp
    tests/ddl_parser_test.cpp
    tests/greeting_test.cpp
  )
  target_link_libraries(minidb_tests PRIVATE minidb_core Catch2::Catch2WithMain)

  include(Catch)
  catch_discover_tests(minidb_tests)
endif()
