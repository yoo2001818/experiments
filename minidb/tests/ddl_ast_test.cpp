#include <catch2/catch_test_macros.hpp>

#include <variant>

#include "minidb/ddl_ast.hpp"

TEST_CASE("DDL AST represents CREATE TABLE definitions") {
  minidb::CreateTableStmt stmt{
    .if_not_exists = true,
    .table_name = "users",
    .definitions = {
      minidb::ColumnDefinition{
        .column_name = "id",
        .data_type = minidb::IntegerType{},
        .nullability = minidb::Nullability::NotNull,
        .primary_key = true,
      },
      minidb::ColumnDefinition{
        .column_name = "name",
        .data_type = minidb::CharType{.length = 32},
      },
      minidb::IndexDefinition{
        .constraint = minidb::IndexConstraint::Unique,
        .index_name = "users_name_idx",
        .key_parts = {
          minidb::KeyPart{
            .column_name = "name",
            .direction = minidb::SortDirection::Asc,
          },
        },
      },
    },
  };

  REQUIRE(stmt.if_not_exists);
  REQUIRE(stmt.table_name == "users");
  REQUIRE(stmt.definitions.size() == 3);

  const auto& id_column =
    std::get<minidb::ColumnDefinition>(stmt.definitions.at(0));
  REQUIRE(id_column.column_name == "id");
  REQUIRE(std::holds_alternative<minidb::IntegerType>(id_column.data_type));
  REQUIRE(id_column.primary_key);

  const auto& name_column =
    std::get<minidb::ColumnDefinition>(stmt.definitions.at(1));
  const auto& name_type = std::get<minidb::CharType>(name_column.data_type);
  REQUIRE(name_type.length == 32);

  const auto& index =
    std::get<minidb::IndexDefinition>(stmt.definitions.at(2));
  REQUIRE(index.constraint == minidb::IndexConstraint::Unique);
  REQUIRE(index.index_name == "users_name_idx");
  REQUIRE(index.key_parts.at(0).direction == minidb::SortDirection::Asc);
}

TEST_CASE("DDL statement variant represents standalone statements") {
  minidb::DdlStatement create_index = minidb::CreateIndexStmt{
    .unique = true,
    .index_name = "users_id_idx",
    .table_name = "users",
    .key_parts = {
      minidb::KeyPart{.column_name = "id"},
    },
  };

  minidb::DdlStatement drop_table = minidb::DropTableStmt{
    .if_exists = true,
    .table_names = {"users", "accounts"},
  };

  REQUIRE(std::holds_alternative<minidb::CreateIndexStmt>(create_index));
  REQUIRE(std::holds_alternative<minidb::DropTableStmt>(drop_table));
}
