#pragma once

#include "minidb/catalog.hpp"
#include "minidb/ddl_ast.hpp"
#include "minidb/storage.hpp"
#include <cstddef>
#include <filesystem>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <unordered_map>
#include <vector>

namespace minidb {

using BinaryValue = std::vector<std::uint8_t>;

using Value =
    std::variant<std::nullptr_t, std::int64_t, bool, std::string, BinaryValue>;

struct Row {
  std::vector<Value> values;
};

class Table {
public:
  explicit Table(TableSchema schema, std::filesystem::path working_dir);
  ~Table();

  Table(Table &&) noexcept;
  Table &operator=(Table &&) noexcept = delete;
  Table(const Table &) = delete;
  Table &operator=(const Table &) = delete;

  void create();
  void open();
  void drop();
  void flush();

  void insert(Row const &row);
  std::optional<Row> read(std::int64_t row_offset);
  // iterator is intentionally omitted for now
  void delete_row(std::int64_t row_offset);
  std::int64_t size();

  const TableSchema &schema() const;
  const std::vector<ColumnSchema> &columns() const;

private:
  // This must remain constant as this is a copy of the root database's schema
  const TableSchema schema_;
  std::filesystem::path working_dir_;
  std::filesystem::path table_path_;

  int row_size_;
  std::vector<int> column_offsets_;

  BinaryFile file_;

  Row decode_row_(std::span<std::byte const> in);
  void encode_row_(Row &row, std::span<std::byte> out);
};

class Database {
public:
  ~Database();

  Database(Database &&) noexcept;
  Database &operator=(Database &&) noexcept = delete;
  Database(const Database &) = delete;
  Database &operator=(const Database &) = delete;

  static Database create(std::filesystem::path path);
  static Database open(std::filesystem::path path);

  std::string execute(DdlStatement const &stmt);

  std::string create_table(CreateTableStmt const &stmt);
  std::string create_index(CreateIndexStmt const &stmt);
  std::string drop_table(DropTableStmt const &stmt);
  std::string drop_index(DropIndexStmt const &stmt);

  void flush();
  const Catalog &catalog() const;

private:
  Catalog catalog_;
  std::filesystem::path path_;
  std::unordered_map<std::string, Table> tables_;

  explicit Database();

  void flush_catalog_();
};

} // namespace minidb
