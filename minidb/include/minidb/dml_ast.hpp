#pragma once

#include "minidb/enum.hpp"
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace minidb {

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
  using Node =
      std::variant<LiteralExpr, IdentifierExpr, UnaryExpr, BinaryExpr,
                   FunctionCallExpr, IsExpr, BetweenExpr, InExpr, LikeExpr,
                   CaseExpr>;

  Node node;
};

struct TableReference {
  Identifier table_name;
};

struct OrderByTerm {
  using Key = std::variant<Identifier, ExprPtr, std::uint32_t>;

  Key key;
  std::optional<SortDirection> direction;
};

struct LimitClause {
  ExprPtr row_count;
  std::optional<ExprPtr> offset;
};

struct SelectStmt {
  bool distinct = false;
  std::vector<ExprPtr> select_list;
  std::vector<TableReference> from;
  std::optional<ExprPtr> where;
  std::vector<OrderByTerm> order_by;
  std::optional<LimitClause> limit;
};

struct DefaultValue {};

using InsertValue = std::variant<ExprPtr, DefaultValue>;

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
  ExprPtr value;
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

} // namespace minidb
