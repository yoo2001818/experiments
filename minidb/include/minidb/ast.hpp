#pragma once

#include "minidb/enum.hpp"
#include <cstdint>
#include <memory>
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

using DdlStatement = std::variant<CreateTableStmt, CreateIndexStmt,
                                  DropTableStmt, DropIndexStmt>;

struct Identifier {
  std::vector<std::string> parts;
};

struct NullLiteral {};

struct NumericLiteral {
  std::string text;
};

struct StringLiteral {
  std::string value;
};

struct BooleanLiteral {
  bool value;
};

using LiteralValue =
    std::variant<NullLiteral, NumericLiteral, StringLiteral, BooleanLiteral>;

struct Expr;
using ExprPtr = std::shared_ptr<Expr>;

enum class UnaryOperator {
  Plus,
  Minus,
  BitwiseNot,
  Not,
};

enum class BinaryOperator {
  Concat,
  Multiply,
  Divide,
  Modulo,
  Add,
  Subtract,
  BitwiseAnd,
  BitwiseOr,
  ShiftLeft,
  ShiftRight,
  Less,
  Greater,
  LessEqual,
  GreaterEqual,
  Equal,
  NotEqual,
  And,
  Or,
};

struct LiteralExpr {
  LiteralValue value;
};

struct IdentifierExpr {
  Identifier name;
};

struct RefSlot {
  std::uint32_t index;
};

struct BoundRefExpr {
  RefSlot ref;
};

struct UnaryExpr {
  UnaryOperator op;
  ExprPtr operand;
};

struct BinaryExpr {
  ExprPtr left;
  BinaryOperator op;
  ExprPtr right;
};

struct FunctionCallExpr {
  Identifier function_name;
  std::vector<ExprPtr> arguments;
};

struct IsExpr {
  ExprPtr value;
  bool negated = false;
  ExprPtr test;
};

struct BetweenExpr {
  ExprPtr value;
  bool negated = false;
  ExprPtr lower;
  ExprPtr upper;
};

struct InExpr {
  ExprPtr value;
  bool negated = false;
  std::vector<ExprPtr> values;
};

struct LikeExpr {
  ExprPtr value;
  bool negated = false;
  ExprPtr pattern;
};

struct CaseWhen {
  ExprPtr condition;
  ExprPtr result;
};

struct CaseExpr {
  std::optional<ExprPtr> operand;
  std::vector<CaseWhen> when_clauses;
  std::optional<ExprPtr> else_expr;
};

struct Expr {
  using Node = std::variant<LiteralExpr, IdentifierExpr, BoundRefExpr,
                            UnaryExpr, BinaryExpr, FunctionCallExpr, IsExpr,
                            BetweenExpr, InExpr, LikeExpr, CaseExpr>;

  Node node;
};

struct WildcardSelectItem {
  std::optional<Identifier> qualifier;
};

struct ExprSelectItem {
  ExprPtr expr;
  std::optional<std::string> alias;
};

using SelectItem = std::variant<WildcardSelectItem, ExprSelectItem>;

struct TableReference {
  Identifier table_name;
  std::optional<std::string> alias;
};

struct OrderByTerm {
  using Key = std::variant<ExprPtr, std::uint32_t>;

  Key key;
  std::optional<SortDirection> direction;
};

struct LimitClause {
  ExprPtr row_count;
  std::optional<ExprPtr> offset;
};

struct SelectStmt {
  bool distinct = false;
  std::vector<SelectItem> select_list;
  std::vector<TableReference> from;
  std::optional<ExprPtr> where;
  std::vector<OrderByTerm> order_by;
  std::optional<LimitClause> limit;
};

struct DefaultValue {};

using InsertValue = std::variant<ExprPtr, DefaultValue>;
using AssignmentValue = std::variant<ExprPtr, DefaultValue>;

struct InsertValuesSource {
  std::vector<std::vector<InsertValue>> rows;
};

struct InsertAssignment {
  Identifier column_name;
  InsertValue value;
};

struct InsertSetSource {
  std::vector<InsertAssignment> assignments;
};

struct InsertSelectSource {
  std::shared_ptr<SelectStmt> select;
};

struct InsertTableSource {
  Identifier table_name;
};

using InsertSource = std::variant<InsertValuesSource, InsertSetSource,
                                  InsertSelectSource, InsertTableSource>;

struct InsertStmt {
  Identifier table_name;
  std::vector<Identifier> column_names;
  InsertSource source;
};

struct UpdateAssignment {
  Identifier column_name;
  AssignmentValue value;
};

struct UpdateStmt {
  TableReference table;
  std::vector<UpdateAssignment> assignments;
  std::optional<ExprPtr> where;
  std::vector<OrderByTerm> order_by;
  std::optional<LimitClause> limit;
};

struct DeleteStmt {
  Identifier table_name;
  std::optional<ExprPtr> where;
  std::vector<OrderByTerm> order_by;
  std::optional<LimitClause> limit;
};

using DmlStatement =
    std::variant<SelectStmt, InsertStmt, UpdateStmt, DeleteStmt>;

using Statement =
    std::variant<CreateTableStmt, CreateIndexStmt, DropTableStmt, DropIndexStmt,
                 SelectStmt, InsertStmt, UpdateStmt, DeleteStmt>;

} // namespace minidb
