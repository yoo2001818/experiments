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

minidb::ExprPtr bound_ref(std::uint32_t index) {
  return std::make_shared<minidb::Expr>(minidb::Expr{
      .node = minidb::BoundRefExpr{.ref = minidb::RefSlot{.index = index}},
  });
}

} // namespace

TEST_CASE("evaluate literal arithmetic recursively") {
  REQUIRE(std::get<minidb::IntegerValue>(evaluate("1 + 2 * 3")).value == 7);
  REQUIRE(std::get<minidb::IntegerValue>(evaluate("-(10 % 4)")).value == -2);
  REQUIRE(std::get<minidb::StringValue>(evaluate("'mini' || 'db'")).value ==
          "minidb");
}

TEST_CASE("evaluate comparisons and three-valued boolean expressions") {
  REQUIRE(std::get<minidb::BooleanValue>(evaluate("1 + 1 = 2")).value);
  REQUIRE(std::get<minidb::BooleanValue>(evaluate("TRUE AND NOT FALSE")).value);
  REQUIRE_FALSE(
      std::get<minidb::BooleanValue>(evaluate("FALSE AND NULL")).value);
  REQUIRE(std::get<minidb::BooleanValue>(evaluate("TRUE OR NULL")).value);
  REQUIRE(std::holds_alternative<minidb::NullValue>(evaluate("TRUE AND NULL")));
  REQUIRE(std::holds_alternative<minidb::NullValue>(evaluate("FALSE OR NULL")));
}

TEST_CASE("evaluate nulls and unresolved identifiers as null") {
  REQUIRE(std::holds_alternative<minidb::NullValue>(evaluate("NULL")));
  REQUIRE(
      std::holds_alternative<minidb::NullValue>(evaluate("missing_column")));
  REQUIRE(std::holds_alternative<minidb::NullValue>(
      evaluate("missing_column + 1")));
}

TEST_CASE("evaluate bound references from an input row") {
  const minidb::Expr expression{
      .node =
          minidb::BinaryExpr{
              .left = bound_ref(0),
              .op = minidb::BinaryOperator::Add,
              .right = bound_ref(1),
          },
  };
  const minidb::DataRow input{
      minidb::IntegerValue{.value = 20},
      minidb::IntegerValue{.value = 22},
  };

  const auto result = minidb::ast_evaluate(expression, input);
  REQUIRE(std::get<minidb::IntegerValue>(result).value == 42);
}

TEST_CASE("bound references preserve nulls and validate their slots") {
  const minidb::DataRow input{minidb::NullValue{}};
  REQUIRE(std::holds_alternative<minidb::NullValue>(
      minidb::ast_evaluate(bound_ref(0), input)));
  REQUIRE_THROWS_AS(minidb::ast_evaluate(bound_ref(1), input),
                    minidb::EvaluationError);
  REQUIRE_THROWS_AS(minidb::ast_evaluate(bound_ref(0)),
                    minidb::EvaluationError);
}

TEST_CASE("reject invalid or unsupported evaluations") {
  REQUIRE_THROWS_AS(evaluate("1 / 0"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("1 + 'one'"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("FALSE AND 1"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("1.5 + 1"), minidb::EvaluationError);
  REQUIRE_THROWS_AS(evaluate("ABS(1)"), minidb::EvaluationError);
}
