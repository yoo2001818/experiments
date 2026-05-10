#include "minidb/database.hpp"
#include "minidb/ddl_ast.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iterator>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace minidb {
namespace {

constexpr std::uint32_t kCatalogVersion = 1;

std::string format_storage_name(char prefix, std::uint32_t id) {
  std::ostringstream out;
  out << prefix << '_' << std::setw(8) << std::setfill('0') << id;
  return out.str();
}

std::uint32_t checked_u32_add(std::uint32_t value) {
  if (value == std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("storage id overflow");
  }
  return value + 1;
}

ColumnType ddl_column_type(const DataType &data_type) {
  if (std::holds_alternative<IntegerType>(data_type))
    return ColumnType::Integer;
  if (std::holds_alternative<BooleanType>(data_type))
    return ColumnType::Boolean;
  if (std::holds_alternative<CharType>(data_type))
    return ColumnType::Char;
  if (std::holds_alternative<BinaryType>(data_type))
    return ColumnType::Binary;
  throw std::runtime_error("unknown DDL data type");
}

std::optional<std::uint32_t> ddl_type_size(const DataType &data_type) {
  if (const auto *type = std::get_if<CharType>(&data_type)) {
    return type->length;
  }
  if (const auto *type = std::get_if<BinaryType>(&data_type)) {
    return type->length;
  }
  return std::nullopt;
}

bool ddl_is_nullable(const ColumnDefinition &column) {
  if (column.primary_key)
    return false;
  return column.nullability != Nullability::NotNull;
}

SortDirection schema_direction(const std::optional<SortDirection> &direction) {
  return direction.value_or(SortDirection::Asc);
}

std::filesystem::path tables_dir(const std::filesystem::path &database_path) {
  return database_path / "tables";
}

std::filesystem::path catalog_path(const std::filesystem::path &database_path) {
  return database_path / "catalog.json";
}

std::string read_text_file(const std::filesystem::path &path) {
  std::ifstream file(path);
  if (!file) {
    throw std::runtime_error("failed to read file: " + path.string());
  }
  return std::string(std::istreambuf_iterator<char>(file),
                     std::istreambuf_iterator<char>());
}

void write_text_file_atomic(const std::filesystem::path &path,
                            const std::string &text) {
  const auto tmp_path = path.string() + ".tmp";
  {
    std::ofstream file(tmp_path, std::ios::trunc);
    if (!file) {
      throw std::runtime_error("failed to write file: " + tmp_path);
    }
    file << text;
    if (!file) {
      throw std::runtime_error("failed to write file: " + tmp_path);
    }
  }

  std::error_code error;
  std::filesystem::rename(tmp_path, path, error);
  if (error) {
    std::filesystem::remove(path, error);
    error.clear();
    std::filesystem::rename(tmp_path, path, error);
    if (error) {
      throw std::runtime_error("failed to replace file: " + path.string());
    }
  }
}

auto find_table(std::vector<TableSchema> &tables, const std::string &name) {
  return std::find_if(
      tables.begin(), tables.end(),
      [&](const TableSchema &table) { return table.name == name; });
}

auto find_table(const std::vector<TableSchema> &tables,
                const std::string &name) {
  return std::find_if(
      tables.begin(), tables.end(),
      [&](const TableSchema &table) { return table.name == name; });
}

bool has_column(const TableSchema &table, const std::string &name) {
  return std::any_of(
      table.columns.begin(), table.columns.end(),
      [&](const ColumnSchema &column) { return column.name == name; });
}

void validate_column_name_available(const TableSchema &table,
                                    const std::string &name) {
  if (has_column(table, name)) {
    throw std::runtime_error("duplicate column: " + name);
  }
}

void validate_index_key_columns(const TableSchema &table,
                                const std::vector<IndexSchemaKeyPart> &keys) {
  if (keys.empty()) {
    throw std::runtime_error("index must have at least one key");
  }
  for (const auto &key : keys) {
    if (!has_column(table, key.column_name)) {
      throw std::runtime_error("unknown index column: " + key.column_name);
    }
  }
}

std::string index_storage_name(const TableSchema &table) {
  const std::uint32_t id = table.next_index_storage_id;
  return format_storage_name('i', id);
}

IndexSchema make_index_schema(const TableSchema &table,
                              const IndexDefinition &definition) {
  const std::string storage_name = index_storage_name(table);
  IndexSchema index{
      .name = definition.index_name.value_or(storage_name),
      .storage_name = storage_name,
      .is_unique = definition.constraint == IndexConstraint::Unique,
      .is_primary_key = definition.constraint == IndexConstraint::PrimaryKey,
      .type = definition.index_type.value_or("BTREE"),
      .keys = {},
      .comment = definition.comment,
  };

  for (const auto &key_part : definition.key_parts) {
    index.keys.push_back(IndexSchemaKeyPart{
        .column_name = key_part.column_name,
        .direction = schema_direction(key_part.direction),
    });
  }
  return index;
}

IndexSchema make_index_schema(const TableSchema &table,
                              const CreateIndexStmt &stmt) {
  const std::string storage_name = index_storage_name(table);
  IndexSchema index{
      .name = stmt.index_name,
      .storage_name = storage_name,
      .is_unique = stmt.unique,
      .is_primary_key = false,
      .type = "BTREE",
      .keys = {},
      .comment = stmt.comment,
  };

  for (const auto &key_part : stmt.key_parts) {
    index.keys.push_back(IndexSchemaKeyPart{
        .column_name = key_part.column_name,
        .direction = schema_direction(key_part.direction),
    });
  }
  return index;
}

void ensure_unique_index_name(const TableSchema &table,
                              const std::string &name) {
  const auto exists =
      std::any_of(table.indexes.begin(), table.indexes.end(),
                  [&](const IndexSchema &index) { return index.name == name; });
  if (exists) {
    throw std::runtime_error("duplicate index: " + name);
  }
}

} // namespace

Database::Database() = default;
Database::~Database() = default;
Database::Database(Database &&) noexcept = default;

Database Database::create(std::filesystem::path path) {
  std::filesystem::create_directories(tables_dir(path));
  if (std::filesystem::exists(catalog_path(path))) {
    return open(std::move(path));
  }

  Database database;
  database.path_ = std::move(path);
  database.catalog_ = Catalog{
      .version = kCatalogVersion,
      .tables = {},
      .next_table_storage_id = 1,
  };
  database.flush_catalog_();
  return database;
}

Database Database::open(std::filesystem::path path) {
  Database database;
  database.path_ = std::move(path);
  database.catalog_ =
      deserialize_catalog(read_text_file(catalog_path(database.path_)));

  for (const auto &table_schema : database.catalog_.tables) {
    Table table(table_schema, database.path_);
    table.open();
    database.tables_.emplace(table_schema.name, std::move(table));
  }
  return database;
}

std::string Database::execute(const DdlStatement &stmt) {
  return std::visit(
      [&](const auto &statement) -> std::string {
        using Statement = std::decay_t<decltype(statement)>;
        if constexpr (std::is_same_v<Statement, CreateTableStmt>) {
          return create_table(statement);
        } else if constexpr (std::is_same_v<Statement, CreateIndexStmt>) {
          return create_index(statement);
        } else if constexpr (std::is_same_v<Statement, DropTableStmt>) {
          return drop_table(statement);
        } else if constexpr (std::is_same_v<Statement, DropIndexStmt>) {
          return drop_index(statement);
        }
      },
      stmt);
}

std::string Database::create_table(const CreateTableStmt &stmt) {
  if (find_table(catalog_.tables, stmt.table_name) != catalog_.tables.end()) {
    if (stmt.if_not_exists)
      return "table exists: " + stmt.table_name;
    throw std::runtime_error("table already exists: " + stmt.table_name);
  }

  TableSchema table{
      .name = stmt.table_name,
      .storage_name = format_storage_name('t', catalog_.next_table_storage_id),
      .next_index_storage_id = 1,
      .comment = std::nullopt,
      .columns = {},
      .indexes = {},
  };
  catalog_.next_table_storage_id =
      checked_u32_add(catalog_.next_table_storage_id);

  for (const auto &definition : stmt.definitions) {
    if (const auto *column = std::get_if<ColumnDefinition>(&definition)) {
      validate_column_name_available(table, column->column_name);
      table.columns.push_back(ColumnSchema{
          .name = column->column_name,
          .type = ddl_column_type(column->data_type),
          .type_size = ddl_type_size(column->data_type),
          .is_nullable = ddl_is_nullable(*column),
          .is_unique = column->unique,
          .is_primary_key = column->primary_key,
          .comment = column->comment,
      });
    }
  }

  if (table.columns.empty()) {
    throw std::runtime_error("table must have at least one column");
  }

  for (const auto &definition : stmt.definitions) {
    if (const auto *index = std::get_if<IndexDefinition>(&definition)) {
      IndexSchema index_schema = make_index_schema(table, *index);
      ensure_unique_index_name(table, index_schema.name);
      validate_index_key_columns(table, index_schema.keys);
      table.indexes.push_back(std::move(index_schema));
      table.next_index_storage_id =
          checked_u32_add(table.next_index_storage_id);
    }
  }

  Table runtime_table(table, path_);
  runtime_table.create();
  catalog_.tables.push_back(table);
  catalog_.next_table_storage_id =
      checked_u32_add(catalog_.next_table_storage_id);
  tables_.emplace(table.name, std::move(runtime_table));
  flush_catalog_();
  return "table created: " + stmt.table_name;
}

std::string Database::create_index(const CreateIndexStmt &stmt) {
  auto table_it = find_table(catalog_.tables, stmt.table_name);
  if (table_it == catalog_.tables.end()) {
    throw std::runtime_error("unknown table: " + stmt.table_name);
  }

  IndexSchema index = make_index_schema(*table_it, stmt);
  ensure_unique_index_name(*table_it, index.name);
  validate_index_key_columns(*table_it, index.keys);
  table_it->indexes.push_back(std::move(index));
  table_it->next_index_storage_id =
      checked_u32_add(table_it->next_index_storage_id);

  tables_.erase(table_it->name);
  Table table(*table_it, path_);
  table.open();
  tables_.emplace(table_it->name, std::move(table));
  flush_catalog_();
  return "index created: " + stmt.index_name;
}

std::string Database::drop_table(const DropTableStmt &stmt) {
  std::vector<std::string> table_names;
  for (const auto &table_name : stmt.table_names) {
    const auto table_it = find_table(catalog_.tables, table_name);
    if (table_it == catalog_.tables.end()) {
      if (stmt.if_exists)
        continue;
      throw std::runtime_error("unknown table: " + table_name);
    }
    table_names.push_back(table_name);
  }

  for (const auto &table_name : table_names) {
    auto table_it = find_table(catalog_.tables, table_name);
    const auto storage_path = tables_dir(path_) / table_it->storage_name;
    tables_.erase(table_it->name);
    std::filesystem::remove_all(storage_path);
    catalog_.tables.erase(table_it);
  }

  if (!table_names.empty()) {
    flush_catalog_();
  }
  return !table_names.empty() ? "table dropped" : "table not found";
}

std::string Database::drop_index(const DropIndexStmt &stmt) {
  auto table_it = find_table(catalog_.tables, stmt.table_name);
  if (table_it == catalog_.tables.end()) {
    throw std::runtime_error("unknown table: " + stmt.table_name);
  }

  auto index_it = std::find_if(
      table_it->indexes.begin(), table_it->indexes.end(),
      [&](const IndexSchema &index) { return index.name == stmt.index_name; });
  if (index_it == table_it->indexes.end()) {
    throw std::runtime_error("unknown index: " + stmt.index_name);
  }

  table_it->indexes.erase(index_it);
  tables_.erase(table_it->name);
  Table table(*table_it, path_);
  table.open();
  tables_.emplace(table_it->name, std::move(table));
  flush_catalog_();
  return "index dropped: " + stmt.index_name;
}

void Database::flush() {
  for (auto &[_, table] : tables_) {
    table.flush();
  }
  flush_catalog_();
}

const Catalog &Database::catalog() const { return catalog_; }

void Database::flush_catalog_() {
  std::filesystem::create_directories(path_);
  std::filesystem::create_directories(tables_dir(path_));
  write_text_file_atomic(catalog_path(path_), serialize_catalog(catalog_));
}

} // namespace minidb
