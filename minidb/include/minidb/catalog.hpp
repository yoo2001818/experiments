#pragma once

#include "minidb/enum.hpp"
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace minidb {

enum class ColumnType {
  Integer,
  Boolean,
  Char,
  Binary,
};

struct ColumnSchema {
  std::string name;
  ColumnType type;
  std::optional<std::uint32_t> type_size;
  bool is_nullable;
  bool is_unique;
  bool is_primary_key;
  std::optional<std::string> comment;
};

struct IndexSchemaKeyPart {
  std::string column_name;
  SortDirection direction;
};

struct IndexSchema {
  std::string name;
  std::string storage_name;
  bool is_unique;
  bool is_primary_key;
  std::string type; // Not meaningfully used yet
  std::vector<IndexSchemaKeyPart> keys;
  std::optional<std::string> comment;
};

struct TableSchema {
  std::string name;
  std::string storage_name;
  std::uint32_t next_index_storage_id;
  std::optional<std::string> comment;
  std::vector<ColumnSchema> columns;
  std::vector<IndexSchema> indexes;
};

struct Catalog {
  std::uint32_t version;
  std::vector<TableSchema> tables;
  std::uint32_t next_table_storage_id;
};

Catalog deserialize_catalog(std::string_view json);
std::string serialize_catalog(Catalog catalog);

} // namespace minidb
