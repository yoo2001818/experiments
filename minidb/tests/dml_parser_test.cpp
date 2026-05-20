#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

#include "minidb/parser.hpp"

namespace {

template <typename T>
const T& as(const minidb::DmlStatement& statement) {
  return std::get<T>(statement);
}

template <typename T>
const T& node(const minidb::ExprPtr& expr) {
  return std::get<T>(expr->node);
}

std::vector<std::string> parts(const minidb::Identifier& identifier) {
  return identifier.parts;
}

} // namespace

TEST_CASE("parse SELECT wildcard projection aliases clauses and limits") {
  const auto statement = minidb::parse_dml_statement(R"sql(
    SELECT DISTINCT *, users.*, id + 1 AS next_id, name nickname
    FROM users AS u, accounts a
    WHERE NOT active OR score BETWEEN 1 AND 10
    ORDER BY 1 DESC, name
    LIMIT 5 OFFSET 2;
  )sql");

  const auto& select = as<minidb::SelectStmt>(statement);
  REQUIRE(select.distinct);
  REQUIRE(select.select_list.size() == 4);

  REQUIRE(std::holds_alternative<minidb::WildcardSelectItem>(
    select.select_list.at(0)));

  const auto& table_wildcard =
    std::get<minidb::WildcardSelectItem>(select.select_list.at(1));
  REQUIRE(table_wildcard.qualifier.has_value());
  REQUIRE(parts(*table_wildcard.qualifier) == std::vector<std::string>{"users"});

  const auto& next_id =
    std::get<minidb::ExprSelectItem>(select.select_list.at(2));
  REQUIRE(next_id.alias == "next_id");
  REQUIRE(node<minidb::BinaryExpr>(next_id.expr).op ==
    minidb::BinaryOperator::Add);

  const auto& nickname =
    std::get<minidb::ExprSelectItem>(select.select_list.at(3));
  REQUIRE(nickname.alias == "nickname");

  REQUIRE(select.from.size() == 2);
  REQUIRE(parts(select.from.at(0).table_name) ==
    std::vector<std::string>{"users"});
  REQUIRE(select.from.at(0).alias == "u");
  REQUIRE(select.from.at(1).alias == "a");

  REQUIRE(select.where.has_value());
  const auto& where = node<minidb::BinaryExpr>(*select.where);
  REQUIRE(where.op == minidb::BinaryOperator::Or);
  REQUIRE(std::holds_alternative<minidb::UnaryExpr>(where.left->node));
  REQUIRE(std::holds_alternative<minidb::BetweenExpr>(where.right->node));

  REQUIRE(select.order_by.size() == 2);
  REQUIRE(std::get<std::uint32_t>(select.order_by.at(0).key) == 1);
  REQUIRE(select.order_by.at(0).direction == minidb::SortDirection::Desc);
  REQUIRE(std::holds_alternative<minidb::ExprPtr>(select.order_by.at(1).key));

  REQUIRE(select.limit.has_value());
  REQUIRE(node<minidb::LiteralExpr>(select.limit->row_count).value.index() ==
    1);
  REQUIRE(select.limit->offset.has_value());
}

TEST_CASE("parse expression predicates and case expressions") {
  const auto statement = minidb::parse_dml_statement(R"sql(
    SELECT
      CASE status WHEN 1 THEN 'one' ELSE 'other' END AS label,
      CASE WHEN name NOT LIKE 'A%' THEN TRUE ELSE FALSE END AS flag
    FROM users
    WHERE amount IS NOT NULL AND id NOT IN (1, 2, 3);
  )sql");

  const auto& select = as<minidb::SelectStmt>(statement);
  REQUIRE(select.select_list.size() == 2);

  const auto& label =
    std::get<minidb::ExprSelectItem>(select.select_list.at(0));
  const auto& simple_case = node<minidb::CaseExpr>(label.expr);
  REQUIRE(simple_case.operand.has_value());
  REQUIRE(simple_case.when_clauses.size() == 1);
  REQUIRE(simple_case.else_expr.has_value());

  const auto& flag =
    std::get<minidb::ExprSelectItem>(select.select_list.at(1));
  const auto& searched_case = node<minidb::CaseExpr>(flag.expr);
  REQUIRE_FALSE(searched_case.operand.has_value());
  REQUIRE(std::holds_alternative<minidb::LikeExpr>(
    searched_case.when_clauses.at(0).condition->node));

  REQUIRE(select.where.has_value());
  const auto& where = node<minidb::BinaryExpr>(*select.where);
  REQUIRE(where.op == minidb::BinaryOperator::And);
  REQUIRE(std::holds_alternative<minidb::IsExpr>(where.left->node));
  REQUIRE(std::holds_alternative<minidb::InExpr>(where.right->node));
}

TEST_CASE("parse numeric string quoted identifiers and precedence") {
  const auto statement = minidb::parse_dml_statement(
    "SELECT .5e+2 + 3 * -`from` || 'can''t' FROM `select`;");

  const auto& select = as<minidb::SelectStmt>(statement);
  const auto& item = std::get<minidb::ExprSelectItem>(select.select_list.at(0));
  const auto& add = node<minidb::BinaryExpr>(item.expr);
  REQUIRE(add.op == minidb::BinaryOperator::Add);
  REQUIRE(node<minidb::LiteralExpr>(add.left).value.index() == 1);

  const auto& multiply = node<minidb::BinaryExpr>(add.right);
  REQUIRE(multiply.op == minidb::BinaryOperator::Multiply);
  const auto& concat = node<minidb::BinaryExpr>(multiply.right);
  REQUIRE(concat.op == minidb::BinaryOperator::Concat);
  REQUIRE(node<minidb::LiteralExpr>(concat.right).value.index() == 2);

  REQUIRE(parts(select.from.at(0).table_name) ==
    std::vector<std::string>{"select"});
}

TEST_CASE("parse INSERT forms") {
  const auto values_statement = minidb::parse_dml_statement(
    "INSERT INTO users (id, name) VALUES (1, 'Ada'), ROW(DEFAULT, 'Grace');");
  const auto& values_insert = as<minidb::InsertStmt>(values_statement);
  REQUIRE(values_insert.column_names.size() == 2);
  const auto& values =
    std::get<minidb::InsertValuesSource>(values_insert.source);
  REQUIRE(values.rows.size() == 2);
  REQUIRE(std::holds_alternative<minidb::DefaultValue>(
    values.rows.at(1).at(0)));

  const auto set_statement = minidb::parse_dml_statement(
    "INSERT users SET id = 1, name = DEFAULT;");
  const auto& set_insert = as<minidb::InsertStmt>(set_statement);
  REQUIRE(std::get<minidb::InsertSetSource>(
    set_insert.source).assignments.size() == 2);

  const auto select_statement = minidb::parse_dml_statement(
    "INSERT INTO archived SELECT * FROM users;");
  REQUIRE(std::holds_alternative<minidb::InsertSelectSource>(
    as<minidb::InsertStmt>(select_statement).source));

  const auto table_statement = minidb::parse_dml_statement(
    "INSERT INTO archived TABLE users;");
  REQUIRE(std::holds_alternative<minidb::InsertTableSource>(
    as<minidb::InsertStmt>(table_statement).source));
}

TEST_CASE("parse UPDATE and DELETE statements") {
  const auto update_statement = minidb::parse_dml_statement(R"sql(
    UPDATE users u
    SET name = DEFAULT, visits = visits + 1
    WHERE id = 1
    ORDER BY visits DESC
    LIMIT 10;
  )sql");
  const auto& update = as<minidb::UpdateStmt>(update_statement);
  REQUIRE(update.table.alias == "u");
  REQUIRE(update.assignments.size() == 2);
  REQUIRE(std::holds_alternative<minidb::DefaultValue>(
    update.assignments.at(0).value));
  REQUIRE(update.where.has_value());
  REQUIRE(update.order_by.size() == 1);
  REQUIRE(update.limit.has_value());

  const auto delete_statement = minidb::parse_dml_statement(
    "DELETE FROM users WHERE id != 1 ORDER BY id LIMIT 5, 10;");
  const auto& delete_ = as<minidb::DeleteStmt>(delete_statement);
  REQUIRE(parts(delete_.table_name) == std::vector<std::string>{"users"});
  REQUIRE(delete_.where.has_value());
  REQUIRE(delete_.order_by.size() == 1);
  REQUIRE(delete_.limit.has_value());
  REQUIRE(delete_.limit->offset.has_value());
}

TEST_CASE("DML parser reports errors") {
  REQUIRE_THROWS_AS(minidb::parse_dml_statement("SELECT FROM users;"),
    minidb::ParseError);
  REQUIRE_THROWS_AS(minidb::parse_dml_statement("SELECT 'unterminated"),
    minidb::ParseError);
  REQUIRE_THROWS_AS(minidb::parse_dml_statement("SELECT 1 2 3;"),
    minidb::ParseError);
  REQUIRE_THROWS_AS(minidb::parse_dml_statement("CREATE TABLE t (id INTEGER);"),
    minidb::ParseError);
}
