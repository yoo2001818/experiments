#include "minidb/catalog.hpp"

#include <nlohmann/json.hpp>

#include <stdexcept>
#include <string>

namespace minidb {
namespace {

using Json = nlohmann::json;

constexpr std::uint32_t kCatalogVersion = 1;

std::string column_type_to_string(ColumnType type) {
  switch (type) {
    case ColumnType::Integer:
      return "INTEGER";
    case ColumnType::Boolean:
      return "BOOLEAN";
    case ColumnType::Char:
      return "CHAR";
    case ColumnType::Binary:
      return "BINARY";
  }
  throw std::runtime_error("unknown column type");
}

ColumnType column_type_from_string(const std::string &type) {
  if (type == "INTEGER") return ColumnType::Integer;
  if (type == "BOOLEAN") return ColumnType::Boolean;
  if (type == "CHAR") return ColumnType::Char;
  if (type == "BINARY") return ColumnType::Binary;
  throw std::runtime_error("unknown column type: " + type);
}

std::string sort_direction_to_string(SortDirection direction) {
  switch (direction) {
    case SortDirection::Asc:
      return "ASC";
    case SortDirection::Desc:
      return "DESC";
  }
  throw std::runtime_error("unknown sort direction");
}

SortDirection sort_direction_from_string(const std::string &direction) {
  if (direction == "ASC") return SortDirection::Asc;
  if (direction == "DESC") return SortDirection::Desc;
  throw std::runtime_error("unknown sort direction: " + direction);
}

Json nullable_string(const std::optional<std::string> &value) {
  if (value.has_value()) return *value;
  return nullptr;
}

Json nullable_uint32(const std::optional<std::uint32_t> &value) {
  if (value.has_value()) return *value;
  return nullptr;
}

std::optional<std::string> optional_string(const Json &json) {
  if (json.is_null()) return std::nullopt;
  return json.get<std::string>();
}

std::optional<std::uint32_t> optional_uint32(const Json &json) {
  if (json.is_null()) return std::nullopt;
  return json.get<std::uint32_t>();
}

Json serialize_column(const ColumnSchema &column) {
  return Json{
    {"name", column.name},
    {"type", column_type_to_string(column.type)},
    {"type_size", nullable_uint32(column.type_size)},
    {"is_nullable", column.is_nullable},
    {"is_unique", column.is_unique},
    {"is_primary_key", column.is_primary_key},
    {"comment", nullable_string(column.comment)},
  };
}

ColumnSchema deserialize_column(const Json &json) {
  return ColumnSchema{
    .name = json.at("name").get<std::string>(),
    .type = column_type_from_string(json.at("type").get<std::string>()),
    .type_size = optional_uint32(json.at("type_size")),
    .is_nullable = json.at("is_nullable").get<bool>(),
    .is_unique = json.at("is_unique").get<bool>(),
    .is_primary_key = json.at("is_primary_key").get<bool>(),
    .comment = optional_string(json.at("comment")),
  };
}

Json serialize_index_key(const IndexSchemaKeyPart &key) {
  return Json{
    {"column", key.column_name},
    {"direction", sort_direction_to_string(key.direction)},
  };
}

IndexSchemaKeyPart deserialize_index_key(const Json &json) {
  return IndexSchemaKeyPart{
    .column_name = json.at("column").get<std::string>(),
    .direction = sort_direction_from_string(
      json.at("direction").get<std::string>()),
  };
}

Json serialize_index(const IndexSchema &index) {
  Json keys = Json::array();
  for (const auto &key : index.keys) {
    keys.push_back(serialize_index_key(key));
  }

  return Json{
    {"name", index.name},
    {"storage_name", index.storage_name},
    {"is_unique", index.is_unique},
    {"is_primary_key", index.is_primary_key},
    {"type", index.type},
    {"comment", nullable_string(index.comment)},
    {"keys", std::move(keys)},
  };
}

IndexSchema deserialize_index(const Json &json) {
  IndexSchema index{
    .name = json.at("name").get<std::string>(),
    .storage_name = json.at("storage_name").get<std::string>(),
    .is_unique = json.at("is_unique").get<bool>(),
    .is_primary_key = json.at("is_primary_key").get<bool>(),
    .type = json.at("type").get<std::string>(),
    .keys = {},
    .comment = optional_string(json.at("comment")),
  };

  for (const auto &key : json.at("keys")) {
    index.keys.push_back(deserialize_index_key(key));
  }
  return index;
}

Json serialize_table(const TableSchema &table) {
  Json columns = Json::array();
  for (const auto &column : table.columns) {
    columns.push_back(serialize_column(column));
  }

  Json indexes = Json::array();
  for (const auto &index : table.indexes) {
    indexes.push_back(serialize_index(index));
  }

  return Json{
    {"name", table.name},
    {"storage_name", table.storage_name},
    {"next_index_storage_id", table.next_index_storage_id},
    {"comment", nullable_string(table.comment)},
    {"columns", std::move(columns)},
    {"indexes", std::move(indexes)},
  };
}

TableSchema deserialize_table(const Json &json) {
  TableSchema table{
    .name = json.at("name").get<std::string>(),
    .storage_name = json.at("storage_name").get<std::string>(),
    .next_index_storage_id = json.at("next_index_storage_id")
      .get<std::uint32_t>(),
    .comment = optional_string(json.at("comment")),
    .columns = {},
    .indexes = {},
  };

  for (const auto &column : json.at("columns")) {
    table.columns.push_back(deserialize_column(column));
  }
  for (const auto &index : json.at("indexes")) {
    table.indexes.push_back(deserialize_index(index));
  }
  return table;
}

} // namespace

Catalog deserialize_catalog(std::string_view text) {
  try {
    const Json json = Json::parse(text.begin(), text.end());
    const auto version = json.at("version").get<std::uint32_t>();
    if (version != kCatalogVersion) {
      throw std::runtime_error("unsupported catalog version");
    }

    Catalog catalog{
      .version = version,
      .tables = {},
      .next_table_storage_id = json.at("next_table_storage_id")
        .get<std::uint32_t>(),
    };

    for (const auto &table : json.at("tables")) {
      catalog.tables.push_back(deserialize_table(table));
    }
    return catalog;
  } catch (const nlohmann::json::exception &error) {
    throw std::runtime_error(std::string("invalid catalog JSON: ") +
      error.what());
  }
}

std::string serialize_catalog(const Catalog &catalog) {
  Json tables = Json::array();
  for (const auto &table : catalog.tables) {
    tables.push_back(serialize_table(table));
  }

  const Json json{
    {"version", catalog.version},
    {"next_table_storage_id", catalog.next_table_storage_id},
    {"tables", std::move(tables)},
  };
  return json.dump(2) + "\n";
}

} // namespace minidb
