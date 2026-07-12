#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>

#include "minidb/ast_rewrite.hpp"
#include "minidb/evaluator.hpp"
#include "minidb/parser.hpp"

namespace {

minidb::ExprPtr parse_select_expression(const std::string &expression) {
  const auto statement =
      minidb::parse_dml_statement("SELECT " + expression + ";");
  const auto &select = std::get<minidb::SelectStmt>(statement);
  return std::get<minidb::ExprSelectItem>(select.select_list.at(0)).expr;
}

} // namespace

TEST_CASE("rewrite identifiers into bound references") {
  const auto original = parse_select_expression("price * quantity + tax");
  std::uint32_t next_slot = 0;

  const auto rewritten = minidb::ast_rewrite(
      original, [&next_slot](minidb::ExprPtr expr) -> minidb::ExprPtr {
        if (std::holds_alternative<minidb::IdentifierExpr>(expr->node)) {
          return std::make_shared<minidb::Expr>(minidb::Expr{
              .node =
                  minidb::BoundRefExpr{
                      .ref = minidb::RefSlot{.index = next_slot++}},
          });
        }
        return expr;
      });

  const minidb::DataRow input{
      minidb::IntegerValue{.value = 10},
      minidb::IntegerValue{.value = 3},
      minidb::IntegerValue{.value = 2},
  };
  const auto result = minidb::ast_evaluate(rewritten, input);

  REQUIRE(next_slot == 3);
  REQUIRE(std::get<minidb::IntegerValue>(result).value == 32);
  REQUIRE(std::holds_alternative<minidb::IdentifierExpr>(
      std::get<minidb::BinaryExpr>(original->node).right->node));
}

TEST_CASE("rewrite visits expression nodes in post-order") {
  const auto original = parse_select_expression("source + 1");

  const auto rewritten = minidb::ast_rewrite(
      original, [](minidb::ExprPtr expr) -> minidb::ExprPtr {
        if (std::holds_alternative<minidb::IdentifierExpr>(expr->node)) {
          return parse_select_expression("41");
        }
        const auto *binary = std::get_if<minidb::BinaryExpr>(&expr->node);
        if (binary != nullptr &&
            std::holds_alternative<minidb::LiteralExpr>(binary->left->node) &&
            std::holds_alternative<minidb::LiteralExpr>(binary->right->node)) {
          return parse_select_expression("42");
        }
        return expr;
      });

  REQUIRE(
      std::get<minidb::IntegerValue>(minidb::ast_evaluate(rewritten)).value ==
      42);
}

TEST_CASE("rewrite traverses every child-bearing expression shape") {
  const auto original = parse_select_expression(R"sql(
    CASE subject
      WHEN probe(subject) THEN subject IS NOT NULL
      WHEN subject BETWEEN lower_bound AND upper_bound THEN
        subject IN (first_value, second_value)
      ELSE NOT subject LIKE pattern
    END
  )sql");
  std::size_t identifier_count = 0;

  (void)minidb::ast_rewrite(
      original, [&identifier_count](minidb::ExprPtr expr) {
        if (std::holds_alternative<minidb::IdentifierExpr>(expr->node)) {
          identifier_count += 1;
        }
        return expr;
      });

  REQUIRE(identifier_count == 11);
}

TEST_CASE("rewrite rejects invalid inputs and callback results") {
  const auto expression = parse_select_expression("1");
  REQUIRE_THROWS_AS(
      minidb::ast_rewrite(nullptr, [](minidb::ExprPtr expr) { return expr; }),
      std::invalid_argument);
  REQUIRE_THROWS_AS(
      minidb::ast_rewrite(expression, minidb::AstRewriteCallback{}),
      std::invalid_argument);
  REQUIRE_THROWS_AS(
      minidb::ast_rewrite(expression,
                          [](minidb::ExprPtr) { return minidb::ExprPtr{}; }),
      std::invalid_argument);
}
