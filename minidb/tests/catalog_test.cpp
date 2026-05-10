#include <catch2/catch_test_macros.hpp>

#include "minidb/catalog.hpp"

TEST_CASE("catalog JSON round trips empty catalog") {
  const minidb::Catalog catalog{
    .version = 1,
    .tables = {},
    .next_table_storage_id = 1,
  };

  const auto restored = minidb::deserialize_catalog(
    minidb::serialize_catalog(catalog));

  REQUIRE(restored.version == 1);
  REQUIRE(restored.next_table_storage_id == 1);
  REQUIRE(restored.tables.empty());
}

TEST_CASE("catalog JSON round trips table schema") {
  const minidb::Catalog catalog{
    .version = 1,
    .tables = {
      minidb::TableSchema{
        .name = "users",
        .storage_name = "t_00000001",
        .next_index_storage_id = 2,
        .comment = "user table",
        .columns = {
          minidb::ColumnSchema{
            .name = "id",
            .type = minidb::ColumnType::Integer,
            .type_size = std::nullopt,
            .is_nullable = false,
            .is_unique = false,
            .is_primary_key = true,
            .comment = std::nullopt,
          },
          minidb::ColumnSchema{
            .name = "name",
            .type = minidb::ColumnType::Char,
            .type_size = 16,
            .is_nullable = true,
            .is_unique = true,
            .is_primary_key = false,
            .comment = "display name",
          },
        },
        .indexes = {
          minidb::IndexSchema{
            .name = "users_name_idx",
            .storage_name = "i_00000001",
            .is_unique = true,
            .is_primary_key = false,
            .type = "BTREE",
            .keys = {
              minidb::IndexSchemaKeyPart{
                .column_name = "name",
                .direction = minidb::SortDirection::Desc,
              },
            },
            .comment = "name lookup",
          },
        },
      },
    },
    .next_table_storage_id = 2,
  };

  const auto restored = minidb::deserialize_catalog(
    minidb::serialize_catalog(catalog));

  REQUIRE(restored.next_table_storage_id == 2);
  REQUIRE(restored.tables.size() == 1);
  REQUIRE(restored.tables.at(0).name == "users");
  REQUIRE(restored.tables.at(0).storage_name == "t_00000001");
  REQUIRE(restored.tables.at(0).columns.at(1).type == minidb::ColumnType::Char);
  REQUIRE(restored.tables.at(0).columns.at(1).type_size == 16);
  REQUIRE(restored.tables.at(0).indexes.at(0).keys.at(0).direction ==
    minidb::SortDirection::Desc);
}

TEST_CASE("catalog JSON rejects unsupported version") {
  REQUIRE_THROWS_AS(
    minidb::deserialize_catalog(
      R"json({"version":2,"next_table_storage_id":1,"tables":[]})json"),
    std::runtime_error);
}
