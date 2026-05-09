#pragma once

#include "minidb/enum.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace minidb {

enum class ColumnType {
  Integer,
  Boolean,
  Char,
  Binary,
};

class Column {
public:
  std::string name;
  ColumnType type;
  std::optional<std::uint32_t> type_size;
  bool is_nullable;
  bool is_unique;
  bool is_primary_key;
  std::optional<std::string> comment;
};

class IndexKeyPart {
public:
  std::string column_name;
  SortDirection direction;
};

class Index {
public:
  std::string name;
  std::string storage_name;
  bool is_unique;
  bool is_primary_key;
  std::string type; // Not meaningfully used yet
  std::vector<IndexKeyPart> keys;
  std::optional<std::string> comment;
};

class Table {
public:
  std::string name;
  std::string storage_name;
  std::uint32_t next_index_storage_id;
  std::vector<Column> columns;
  std::optional<std::string> comment;
};

class Catalog {
public:
  std::vector<Table> tables;
  std::uint32_t next_table_storage_id;
};

} // namespace minidb
