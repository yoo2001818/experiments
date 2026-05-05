#include <catch2/catch_test_macros.hpp>

#include "minidb/greeting.hpp"

TEST_CASE("greeting returns hello world") {
  REQUIRE(minidb::greeting() == "Hello, world!");
}
