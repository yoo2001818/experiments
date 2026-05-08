#include <catch2/catch_test_macros.hpp>

#include <variant>

#include "minidb/ddl_ast.hpp"
#include "minidb/ddl_parser.hpp"

namespace {

template <typename T>
const T& as(const minidb::DdlStatement& statement) {
  return std::get<T>(statement);
}

template <typename T>
const T& as_definition(const minidb::CreateDefinition& definition) {
  return std::get<T>(definition);
}

} // namespace

TEST_CASE("parse CREATE TABLE with columns and inline indexes") {
  const auto statement = minidb::parse_ddl_statement(R"sql(
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER NOT NULL PRIMARY KEY COMMENT 'row id',
      active BOOLEAN NULL,
      name CHAR(32) UNIQUE KEY,
      payload BINARY(16),
      UNIQUE INDEX users_name_idx USING btree (name ASC) COMMENT 'name lookup',
      PRIMARY KEY pk_users (id DESC)
    );
  )sql");

  const auto& table = as<minidb::CreateTableStmt>(statement);
  REQUIRE(table.if_not_exists);
  REQUIRE(table.table_name == "users");
  REQUIRE(table.definitions.size() == 6);

  const auto& id = as_definition<minidb::ColumnDefinition>(
    table.definitions.at(0));
  REQUIRE(id.column_name == "id");
  REQUIRE(std::holds_alternative<minidb::IntegerType>(id.data_type));
  REQUIRE(id.nullability == minidb::Nullability::NotNull);
  REQUIRE(id.primary_key);
  REQUIRE(id.comment == "row id");

  const auto& active = as_definition<minidb::ColumnDefinition>(
    table.definitions.at(1));
  REQUIRE(std::holds_alternative<minidb::BooleanType>(active.data_type));
  REQUIRE(active.nullability == minidb::Nullability::Null);

  const auto& name = as_definition<minidb::ColumnDefinition>(
    table.definitions.at(2));
  REQUIRE(std::get<minidb::CharType>(name.data_type).length == 32);
  REQUIRE(name.unique);

  const auto& payload = as_definition<minidb::ColumnDefinition>(
    table.definitions.at(3));
  REQUIRE(std::get<minidb::BinaryType>(payload.data_type).length == 16);

  const auto& name_index = as_definition<minidb::IndexDefinition>(
    table.definitions.at(4));
  REQUIRE(name_index.constraint == minidb::IndexConstraint::Unique);
  REQUIRE(name_index.index_name == "users_name_idx");
  REQUIRE(name_index.index_type == "btree");
  REQUIRE(name_index.comment == "name lookup");
  REQUIRE(name_index.key_parts.at(0).column_name == "name");
  REQUIRE(name_index.key_parts.at(0).direction == minidb::SortDirection::Asc);

  const auto& primary_key = as_definition<minidb::IndexDefinition>(
    table.definitions.at(5));
  REQUIRE(primary_key.constraint == minidb::IndexConstraint::PrimaryKey);
  REQUIRE(primary_key.index_name == "pk_users");
  REQUIRE(primary_key.key_parts.at(0).direction == minidb::SortDirection::Desc);
}

TEST_CASE("parse CREATE INDEX") {
  const auto statement = minidb::parse_ddl_statement(
    "CREATE UNIQUE INDEX users_active_idx ON users (active DESC, id) "
    "COMMENT 'active lookup'");

  const auto& index = as<minidb::CreateIndexStmt>(statement);
  REQUIRE(index.unique);
  REQUIRE(index.index_name == "users_active_idx");
  REQUIRE(index.table_name == "users");
  REQUIRE(index.comment == "active lookup");
  REQUIRE(index.key_parts.size() == 2);
  REQUIRE(index.key_parts.at(0).column_name == "active");
  REQUIRE(index.key_parts.at(0).direction == minidb::SortDirection::Desc);
  REQUIRE(index.key_parts.at(1).column_name == "id");
  REQUIRE_FALSE(index.key_parts.at(1).direction.has_value());
}

TEST_CASE("parse DROP statements") {
  const auto drop_table = minidb::parse_ddl_statement(
    "DROP TABLE IF EXISTS users, accounts;");
  const auto& table = as<minidb::DropTableStmt>(drop_table);
  REQUIRE(table.if_exists);
  REQUIRE(table.table_names == std::vector<std::string>{"users", "accounts"});

  const auto drop_index = minidb::parse_ddl_statement(
    "DROP INDEX users_active_idx ON users");
  const auto& index = as<minidb::DropIndexStmt>(drop_index);
  REQUIRE(index.index_name == "users_active_idx");
  REQUIRE(index.table_name == "users");
}

TEST_CASE("parser treats keywords case-insensitively and preserves identifiers") {
  const auto statement = minidb::parse_ddl_statement(
    "create table `select` (`from` integer)");
  const auto& table = as<minidb::CreateTableStmt>(statement);
  REQUIRE(table.table_name == "select");

  const auto& column = as_definition<minidb::ColumnDefinition>(
    table.definitions.at(0));
  REQUIRE(column.column_name == "from");
}

TEST_CASE("parser reports errors") {
  REQUIRE_THROWS_AS(
    minidb::parse_ddl_statement("CREATE TABLE users (id TEXT)"),
    minidb::ParseError);

  try {
    (void)minidb::parse_ddl_statement("CREATE TABLE users (name CHAR(0))");
    FAIL("expected parse error");
  } catch (const minidb::ParseError& error) {
    REQUIRE(error.location().line == 1);
    REQUIRE(error.location().column > 0);
  }
}
