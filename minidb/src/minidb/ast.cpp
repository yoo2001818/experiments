#include "minidb/ast.hpp"

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

} // namespace minidb
