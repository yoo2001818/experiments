#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <variant>

#include "minidb/evaluator.hpp"
#include "minidb/parser.hpp"

namespace {

minidb::ExprPtr parse_select_expression(const std::string &sql) {
  const auto statement = minidb::parse_dml_statement(sql);
  const auto &select = std::get<minidb::SelectStmt>(statement);
  return std::get<minidb::ExprSelectItem>(select.select_list.at(0)).expr;
}

minidb::Value evaluate(const std::string &expression) {
  return minidb::ast_evaluate(
      parse_select_expression("SELECT " + expression + ";"));
}

} // namespace

TEST_CASE("evaluate literal arithmetic recursively") {
  REQUIRE(std::get<std::int64_t>(evaluate("1 + 2 * 3")) == 7);
  REQUIRE(std::get<std::int64_t>(evaluate("-(10 % 4)")) == -2);
  REQUIRE(std::get<std::string>(evaluate("'mini' || 'db'")) == "minidb");
}

TEST_CASE("evaluate comparisons and three-valued boolean expressions") {
  REQUIRE(std::get<bool>(evaluate("1 + 1 = 2")));
  REQUIRE(std::get<bool>(evaluate("TRUE AND NOT FALSE")));
  REQUIRE_FALSE(std::get<bool>(evaluate("FALSE AND NULL")));
  REQUIRE(std::get<bool>(evaluate("TRUE OR NULL")));
  REQUIRE(std::holds_alternative<std::nullptr_t>(evaluate("TRUE AND NULL")));
  REQUIRE(std::holds_alternative<std::nullptr_t>(evaluate("FALSE OR NULL")));
}

TEST_CASE("evaluate nulls and unresolved identifiers as null") {
  REQUIRE(std::holds_alternative<std::nullptr_t>(evaluate("NULL")));
  REQUIRE(std::holds_alternative<std::nullptr_t>(evaluate("missing_column")));
  REQUIRE(
      std::holds_alternative<std::nullptr_t>(evaluate("missing_column + 1")));
}

TEST_CASE("reject invalid or unsupported evaluations") {
  REQUIRE_THROWS_AS(evaluate("1 / 0"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("1 + 'one'"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("FALSE AND 1"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("1.5 + 1"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("ABS(1)"), minidb::EvaluationError);
}
