#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "minidb/database.hpp"
#include "minidb/ddl_parser.hpp"

namespace {

struct TempDir {
  std::filesystem::path path;

  TempDir() {
    const auto stamp = std::chrono::steady_clock::now()
      .time_since_epoch()
      .count();
    path = std::filesystem::temp_directory_path() /
      ("minidb_test_" + std::to_string(stamp));
    std::filesystem::remove_all(path);
  }

  ~TempDir() {
    std::error_code error;
    std::filesystem::remove_all(path, error);
  }
};

minidb::DdlStatement parse(std::string_view sql) {
  return minidb::parse_ddl_statement(sql);
}

minidb::TableSchema users_schema() {
  return minidb::TableSchema{
    .name = "users",
    .storage_name = "t_00000001",
    .next_index_storage_id = 1,
    .comment = std::nullopt,
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
        .is_unique = false,
        .is_primary_key = false,
        .comment = std::nullopt,
      },
    },
    .indexes = {},
  };
}

} // namespace

TEST_CASE("database creates table storage and persists catalog") {
  TempDir dir;
  auto database = minidb::Database::create(dir.path);

  REQUIRE(std::filesystem::exists(dir.path / "catalog.json"));
  REQUIRE(std::filesystem::exists(dir.path / "tables"));

  REQUIRE(database.execute(parse(
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name CHAR(16));")) ==
    "table created: users");

  REQUIRE(database.catalog().tables.size() == 1);
  REQUIRE(database.catalog().tables.at(0).storage_name == "t_00000001");
  REQUIRE(std::filesystem::exists(
    dir.path / "tables" / "t_00000001" / "rows.bin"));

  auto reopened = minidb::Database::open(dir.path);
  REQUIRE(reopened.catalog().tables.size() == 1);
  REQUIRE(reopened.catalog().tables.at(0).name == "users");
  REQUIRE(reopened.catalog().tables.at(0).columns.at(1).type_size == 16);
}

TEST_CASE("database handles duplicate and idempotent CREATE TABLE") {
  TempDir dir;
  auto database = minidb::Database::create(dir.path);

  (void)database.execute(parse("CREATE TABLE users (id INTEGER);"));
  REQUIRE(database.execute(parse(
    "CREATE TABLE IF NOT EXISTS users (id INTEGER);")) ==
    "table exists: users");
  REQUIRE_THROWS_AS(
    database.execute(parse("CREATE TABLE users (id INTEGER);")),
    std::runtime_error);
}

TEST_CASE("database drops table storage") {
  TempDir dir;
  auto database = minidb::Database::create(dir.path);
  (void)database.execute(parse("CREATE TABLE users (id INTEGER);"));

  const auto table_path = dir.path / "tables" /
    database.catalog().tables.at(0).storage_name;
  REQUIRE(std::filesystem::exists(table_path));

  REQUIRE(database.execute(parse("DROP TABLE users;")) == "table dropped");
  REQUIRE_FALSE(std::filesystem::exists(table_path));
  REQUIRE(database.catalog().tables.empty());

  REQUIRE(database.execute(parse("DROP TABLE IF EXISTS users;")) ==
    "table not found");
}

TEST_CASE("database validates multi-table DROP before deleting files") {
  TempDir dir;
  auto database = minidb::Database::create(dir.path);
  (void)database.execute(parse("CREATE TABLE users (id INTEGER);"));

  const auto table_path = dir.path / "tables" /
    database.catalog().tables.at(0).storage_name;

  REQUIRE_THROWS_AS(
    database.execute(parse("DROP TABLE users, missing;")),
    std::runtime_error);
  REQUIRE(std::filesystem::exists(table_path));
  REQUIRE(database.catalog().tables.size() == 1);
}

TEST_CASE("database stores index metadata without index file") {
  TempDir dir;
  auto database = minidb::Database::create(dir.path);
  (void)database.execute(parse("CREATE TABLE users (id INTEGER, name CHAR(16));"));

  REQUIRE(database.execute(parse(
    "CREATE UNIQUE INDEX users_name_idx ON users (name DESC);")) ==
    "index created: users_name_idx");

  const auto& table = database.catalog().tables.at(0);
  REQUIRE(table.indexes.size() == 1);
  REQUIRE(table.indexes.at(0).storage_name == "i_00000001");
  REQUIRE(table.indexes.at(0).keys.at(0).direction == minidb::SortDirection::Desc);
  REQUIRE_FALSE(std::filesystem::exists(
    dir.path / "tables" / table.storage_name / "indexes" / "i_00000001.bin"));

  REQUIRE(database.execute(parse("DROP INDEX users_name_idx ON users;")) ==
    "index dropped: users_name_idx");
  REQUIRE(database.catalog().tables.at(0).indexes.empty());
}

TEST_CASE("table row file validates header and stores fixed-width rows") {
  TempDir dir;
  const auto schema = users_schema();

  minidb::Table table(schema, dir.path);
  table.create();
  table.insert(minidb::Row{
    .values = {std::int64_t{7}, std::string{"Ada"}},
  });
  table.flush();

  minidb::Table reopened(schema, dir.path);
  reopened.open();
  REQUIRE(reopened.size() == 1);
  const auto row = reopened.read(0);
  REQUIRE(row.has_value());
  REQUIRE(std::get<std::int64_t>(row->values.at(0)) == 7);
  REQUIRE(std::get<std::string>(row->values.at(1)) == "Ada");

  reopened.delete_row(0);
  REQUIRE_FALSE(reopened.read(0).has_value());
}

TEST_CASE("table scan iterates live rows and skips tombstones") {
  TempDir dir;
  const auto schema = users_schema();

  minidb::Table table(schema, dir.path);
  table.create();

  auto empty_scan = table.scan().begin();
  REQUIRE_FALSE(empty_scan.next().has_value());

  table.insert(minidb::Row{
    .values = {std::int64_t{1}, std::string{"Ada"}},
  });
  table.insert(minidb::Row{
    .values = {std::int64_t{2}, nullptr},
  });
  table.insert(minidb::Row{
    .values = {std::int64_t{3}, std::string{"Grace"}},
  });
  table.delete_row(1);

  std::vector<minidb::RowEntry> entries;
  for (const auto &entry : table.scan()) {
    entries.push_back(entry);
  }

  REQUIRE(entries.size() == 2);

  const auto& first = entries.at(0);
  REQUIRE(first.row_offset == 0);
  REQUIRE(std::get<std::int64_t>(first.row.values.at(0)) == 1);
  REQUIRE(std::get<std::string>(first.row.values.at(1)) == "Ada");

  const auto& second = entries.at(1);
  REQUIRE(second.row_offset == 2);
  REQUIRE(std::get<std::int64_t>(second.row.values.at(0)) == 3);
  REQUIRE(std::get<std::string>(second.row.values.at(1)) == "Grace");

  auto scan = table.scan().begin();
  const auto next_first = scan.next();
  REQUIRE(next_first.has_value());
  REQUIRE(next_first->row_offset == 0);
  const auto next_second = scan.next();
  REQUIRE(next_second.has_value());
  REQUIRE(next_second->row_offset == 2);
  REQUIRE_FALSE(scan.next().has_value());
  REQUIRE_FALSE(scan.next().has_value());
}

TEST_CASE("table scan iterator supports manual increment") {
  TempDir dir;
  const auto schema = users_schema();

  minidb::Table table(schema, dir.path);
  table.create();
  table.insert(minidb::Row{
    .values = {std::int64_t{1}, std::string{"Ada"}},
  });
  table.insert(minidb::Row{
    .values = {std::int64_t{2}, std::string{"Grace"}},
  });

  auto scan = table.scan();
  auto it = scan.begin();
  REQUIRE(it != scan.end());
  REQUIRE(it->row_offset == 0);

  ++it;
  REQUIRE(it != scan.end());
  REQUIRE((*it).row_offset == 1);

  it++;
  REQUIRE(it == scan.end());
}

TEST_CASE("table scan next helper remains available") {
  TempDir dir;
  const auto schema = users_schema();

  minidb::Table table(schema, dir.path);
  table.create();
  table.insert(minidb::Row{
    .values = {std::int64_t{1}, std::string{"Ada"}},
  });

  auto scan = table.scan().begin();
  const auto first = scan.next();
  REQUIRE(first.has_value());
  REQUIRE(first->row_offset == 0);
  REQUIRE(std::get<std::int64_t>(first->row.values.at(0)) == 1);
  REQUIRE(std::get<std::string>(first->row.values.at(1)) == "Ada");
  REQUIRE_FALSE(scan.next().has_value());
}

TEST_CASE("table row file rejects invalid header") {
  TempDir dir;
  const auto schema = users_schema();
  const auto table_dir = dir.path / "tables" / schema.storage_name;
  std::filesystem::create_directories(table_dir);
  {
    std::ofstream file(table_dir / "rows.bin", std::ios::binary);
    file << "nope";
    std::uint32_t version = 1;
    file.write(reinterpret_cast<const char*>(&version), sizeof(version));
  }

  minidb::Table table(schema, dir.path);
  REQUIRE_THROWS_AS(table.open(), std::runtime_error);
}
