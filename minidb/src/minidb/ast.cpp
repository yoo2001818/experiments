#include "minidb/ast.hpp"

#include <sstream>
#include <string_view>
#include <type_traits>
#include <variant>

namespace minidb {
namespace {

bool expr_equal(const ExprPtr &left, const ExprPtr &right) {
  return left == right ||
         (left != nullptr && right != nullptr && *left == *right);
}

bool expr_list_equal(const std::vector<ExprPtr> &left,
                     const std::vector<ExprPtr> &right) {
  if (left.size() != right.size()) {
    return false;
  }
  for (std::size_t index = 0; index < left.size(); ++index) {
    if (!expr_equal(left[index], right[index])) {
      return false;
    }
  }
  return true;
}

bool optional_expr_equal(const std::optional<ExprPtr> &left,
                         const std::optional<ExprPtr> &right) {
  return left.has_value() == right.has_value() &&
         (!left.has_value() || expr_equal(*left, *right));
}

bool literal_equal(const LiteralValue &left, const LiteralValue &right) {
  if (left.index() != right.index()) {
    return false;
  }
  return std::visit(
      [](const auto &left_value, const auto &right_value) {
        using Left = std::decay_t<decltype(left_value)>;
        using Right = std::decay_t<decltype(right_value)>;
        if constexpr (!std::is_same_v<Left, Right>) {
          return false;
        } else if constexpr (std::is_same_v<Left, NullLiteral>) {
          return true;
        } else if constexpr (std::is_same_v<Left, NumericLiteral>) {
          return left_value.text == right_value.text;
        } else if constexpr (std::is_same_v<Left, StringLiteral>) {
          return left_value.value == right_value.value;
        } else {
          return left_value.value == right_value.value;
        }
      },
      left, right);
}

std::string identifier_text(const Identifier &identifier) {
  std::ostringstream out;
  for (std::size_t index = 0; index < identifier.parts.size(); ++index) {
    if (index != 0) {
      out << '.';
    }
    out << identifier.parts[index];
  }
  return out.str();
}

std::string unary_operator_text(UnaryOperator op) {
  switch (op) {
  case UnaryOperator::Plus:
    return "+";
  case UnaryOperator::Minus:
    return "-";
  case UnaryOperator::BitwiseNot:
    return "~";
  case UnaryOperator::Not:
    return "NOT ";
  }
  return "?";
}

std::string binary_operator_text(BinaryOperator op) {
  switch (op) {
  case BinaryOperator::Concat:
    return "||";
  case BinaryOperator::Multiply:
    return "*";
  case BinaryOperator::Divide:
    return "/";
  case BinaryOperator::Modulo:
    return "%";
  case BinaryOperator::Add:
    return "+";
  case BinaryOperator::Subtract:
    return "-";
  case BinaryOperator::BitwiseAnd:
    return "&";
  case BinaryOperator::BitwiseOr:
    return "|";
  case BinaryOperator::ShiftLeft:
    return "<<";
  case BinaryOperator::ShiftRight:
    return ">>";
  case BinaryOperator::Less:
    return "<";
  case BinaryOperator::Greater:
    return ">";
  case BinaryOperator::LessEqual:
    return "<=";
  case BinaryOperator::GreaterEqual:
    return ">=";
  case BinaryOperator::Equal:
    return "=";
  case BinaryOperator::NotEqual:
    return "<>";
  case BinaryOperator::And:
    return "AND";
  case BinaryOperator::Or:
    return "OR";
  }
  return "?";
}

std::string quote_string(std::string_view value) {
  std::string result{"'"};
  for (const char ch : value) {
    result += ch;
    if (ch == '\'') {
      result += '\'';
    }
  }
  result += '\'';
  return result;
}

} // namespace

bool Expr::operator==(const Expr &other) const {
  if (node.index() != other.node.index()) {
    return false;
  }

  return std::visit(
      [](const auto &left, const auto &right) {
        using Left = std::decay_t<decltype(left)>;
        using Right = std::decay_t<decltype(right)>;
        if constexpr (!std::is_same_v<Left, Right>) {
          return false;
        } else if constexpr (std::is_same_v<Left, LiteralExpr>) {
          return literal_equal(left.value, right.value);
        } else if constexpr (std::is_same_v<Left, IdentifierExpr>) {
          return left.name.parts == right.name.parts;
        } else if constexpr (std::is_same_v<Left, BoundRefExpr>) {
          return left.ref.index == right.ref.index;
        } else if constexpr (std::is_same_v<Left, UnaryExpr>) {
          return left.op == right.op && expr_equal(left.operand, right.operand);
        } else if constexpr (std::is_same_v<Left, BinaryExpr>) {
          return left.op == right.op && expr_equal(left.left, right.left) &&
                 expr_equal(left.right, right.right);
        } else if constexpr (std::is_same_v<Left, FunctionCallExpr>) {
          return left.function_name.parts == right.function_name.parts &&
                 expr_list_equal(left.arguments, right.arguments);
        } else if constexpr (std::is_same_v<Left, IsExpr>) {
          return left.negated == right.negated &&
                 expr_equal(left.value, right.value) &&
                 expr_equal(left.test, right.test);
        } else if constexpr (std::is_same_v<Left, BetweenExpr>) {
          return left.negated == right.negated &&
                 expr_equal(left.value, right.value) &&
                 expr_equal(left.lower, right.lower) &&
                 expr_equal(left.upper, right.upper);
        } else if constexpr (std::is_same_v<Left, InExpr>) {
          return left.negated == right.negated &&
                 expr_equal(left.value, right.value) &&
                 expr_list_equal(left.values, right.values);
        } else if constexpr (std::is_same_v<Left, LikeExpr>) {
          return left.negated == right.negated &&
                 expr_equal(left.value, right.value) &&
                 expr_equal(left.pattern, right.pattern);
        } else {
          if (!optional_expr_equal(left.operand, right.operand) ||
              left.when_clauses.size() != right.when_clauses.size() ||
              !optional_expr_equal(left.else_expr, right.else_expr)) {
            return false;
          }
          for (std::size_t index = 0; index < left.when_clauses.size();
               ++index) {
            if (!expr_equal(left.when_clauses[index].condition,
                            right.when_clauses[index].condition) ||
                !expr_equal(left.when_clauses[index].result,
                            right.when_clauses[index].result)) {
              return false;
            }
          }
          return true;
        }
      },
      node, other.node);
}

std::string format_expression(const ExprPtr &expr) {
  if (expr == nullptr) {
    return "<null-expr>";
  }

  return std::visit(
      [](const auto &node) -> std::string {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, LiteralExpr>) {
          return std::visit(
              [](const auto &literal) -> std::string {
                using Literal = std::decay_t<decltype(literal)>;
                if constexpr (std::is_same_v<Literal, NullLiteral>) {
                  return "NULL";
                } else if constexpr (std::is_same_v<Literal, NumericLiteral>) {
                  return literal.text;
                } else if constexpr (std::is_same_v<Literal, StringLiteral>) {
                  return quote_string(literal.value);
                } else {
                  return literal.value ? "TRUE" : "FALSE";
                }
              },
              node.value);
        } else if constexpr (std::is_same_v<T, IdentifierExpr>) {
          return identifier_text(node.name);
        } else if constexpr (std::is_same_v<T, BoundRefExpr>) {
          return "#" + std::to_string(node.ref.index);
        } else if constexpr (std::is_same_v<T, UnaryExpr>) {
          return "(" + unary_operator_text(node.op) +
                 format_expression(node.operand) + ")";
        } else if constexpr (std::is_same_v<T, BinaryExpr>) {
          return "(" + format_expression(node.left) + " " +
                 binary_operator_text(node.op) + " " +
                 format_expression(node.right) + ")";
        } else if constexpr (std::is_same_v<T, FunctionCallExpr>) {
          std::ostringstream out;
          out << identifier_text(node.function_name) << '(';
          for (std::size_t index = 0; index < node.arguments.size(); ++index) {
            if (index != 0) {
              out << ", ";
            }
            out << format_expression(node.arguments[index]);
          }
          out << ')';
          return out.str();
        } else if constexpr (std::is_same_v<T, IsExpr>) {
          return "(" + format_expression(node.value) + " IS " +
                 (node.negated ? "NOT " : "") + format_expression(node.test) +
                 ")";
        } else if constexpr (std::is_same_v<T, BetweenExpr>) {
          return "(" + format_expression(node.value) +
                 (node.negated ? " NOT BETWEEN " : " BETWEEN ") +
                 format_expression(node.lower) + " AND " +
                 format_expression(node.upper) + ")";
        } else if constexpr (std::is_same_v<T, InExpr>) {
          std::ostringstream out;
          out << '(' << format_expression(node.value)
              << (node.negated ? " NOT IN (" : " IN (");
          for (std::size_t index = 0; index < node.values.size(); ++index) {
            if (index != 0) {
              out << ", ";
            }
            out << format_expression(node.values[index]);
          }
          out << "))";
          return out.str();
        } else if constexpr (std::is_same_v<T, LikeExpr>) {
          return "(" + format_expression(node.value) +
                 (node.negated ? " NOT LIKE " : " LIKE ") +
                 format_expression(node.pattern) + ")";
        } else {
          std::ostringstream out;
          out << "CASE";
          if (node.operand.has_value()) {
            out << ' ' << format_expression(*node.operand);
          }
          for (const auto &clause : node.when_clauses) {
            out << " WHEN " << format_expression(clause.condition) << " THEN "
                << format_expression(clause.result);
          }
          if (node.else_expr.has_value()) {
            out << " ELSE " << format_expression(*node.else_expr);
          }
          out << " END";
          return out.str();
        }
      },
      expr->node);
}

} // namespace minidb
