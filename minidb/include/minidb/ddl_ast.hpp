#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace minidb {

struct IntegerType {};
struct BooleanType {};

struct CharType {
  std::uint32_t length;
};

struct BinaryType {
  std::uint32_t length;
};

using DataType = std::variant<IntegerType, BooleanType, CharType, BinaryType>;

enum class Nullability {
  Unspecified,
  Null,
  NotNull,
};

enum class SortDirection {
  Asc,
  Desc,
};

struct KeyPart {
  std::string column_name;
  std::optional<SortDirection> direction;
};

struct ColumnDefinition {
  std::string column_name;
  DataType data_type;
  Nullability nullability = Nullability::Unspecified;
  bool unique = false;
  bool primary_key = false;
  std::optional<std::string> comment;
};

enum class IndexConstraint {
  Regular,
  Unique,
  PrimaryKey,
};

struct IndexDefinition {
  IndexConstraint constraint = IndexConstraint::Regular;
  std::optional<std::string> index_name;
  std::optional<std::string> index_type;
  std::vector<KeyPart> key_parts;
  std::optional<std::string> comment;
};

using CreateDefinition = std::variant<ColumnDefinition, IndexDefinition>;

struct CreateTableStmt {
  bool if_not_exists = false;
  std::string table_name;
  std::vector<CreateDefinition> definitions;
};

struct CreateIndexStmt {
  bool unique = false;
  std::string index_name;
  std::string table_name;
  std::vector<KeyPart> key_parts;
  std::optional<std::string> comment;
};

struct DropTableStmt {
  bool if_exists = false;
  std::vector<std::string> table_names;
};

struct DropIndexStmt {
  std::string index_name;
  std::string table_name;
};

using DdlStatement = std::variant<
  CreateTableStmt,
  CreateIndexStmt,
  DropTableStmt,
  DropIndexStmt
>;

} // namespace minidb
