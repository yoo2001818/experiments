#pragma once

#include "minidb/catalog.hpp"
#include "minidb/ddl_ast.hpp"
#include "minidb/table.hpp"
#include <filesystem>
#include <string>
#include <unordered_map>

namespace minidb {

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
