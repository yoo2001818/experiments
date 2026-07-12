#include "minidb/evaluator.hpp"

#include <charconv>
#include <cstdint>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>

namespace minidb {
namespace {

bool is_null(const Value &value) {
  return std::holds_alternative<NullValue>(value);
}

std::int64_t require_integer(const Value &value, const char *operation) {
  const auto *integer = std::get_if<IntegerValue>(&value);
  if (integer == nullptr) {
    throw EvaluationError(std::string(operation) +
                          " requires INTEGER operands");
  }
  return integer->value;
}

bool require_boolean(const Value &value, const char *operation) {
  const auto *boolean = std::get_if<BooleanValue>(&value);
  if (boolean == nullptr) {
    throw EvaluationError(std::string(operation) +
                          " requires BOOLEAN operands");
  }
  return boolean->value;
}

std::string require_string(const Value &value, const char *operation) {
  const auto *string = std::get_if<StringValue>(&value);
  if (string == nullptr) {
    throw EvaluationError(std::string(operation) + " requires string operands");
  }
  return string->value;
}

std::int64_t parse_integer(const NumericLiteral &literal) {
  std::int64_t value = 0;
  const char *begin = literal.text.data();
  const char *end = begin + literal.text.size();
  const auto [parsed_end, error] = std::from_chars(begin, end, value);
  if (error != std::errc{} || parsed_end != end) {
    throw EvaluationError("unsupported numeric literal: " + literal.text);
  }
  return value;
}

Value evaluate_literal(const LiteralExpr &expr) {
  return std::visit(
      [](const auto &literal) -> Value {
        using T = std::decay_t<decltype(literal)>;
        if constexpr (std::is_same_v<T, NullLiteral>) {
          return NullValue{};
        } else if constexpr (std::is_same_v<T, NumericLiteral>) {
          return IntegerValue{.value = parse_integer(literal)};
        } else if constexpr (std::is_same_v<T, StringLiteral>) {
          return StringValue{.value = literal.value};
        } else {
          return BooleanValue{.value = literal.value};
        }
      },
      expr.value);
}

Value evaluate_unary(const UnaryExpr &expr) {
  const Value operand = ast_evaluate(expr.operand);
  if (is_null(operand)) {
    return NullValue{};
  }

  switch (expr.op) {
  case UnaryOperator::Plus:
    return IntegerValue{.value = require_integer(operand, "unary +")};
  case UnaryOperator::Minus: {
    const auto integer = require_integer(operand, "unary -");
    if (integer == std::numeric_limits<std::int64_t>::min()) {
      throw EvaluationError("INTEGER overflow in unary -");
    }
    return IntegerValue{.value = -integer};
  }
  case UnaryOperator::BitwiseNot:
    return IntegerValue{.value = ~require_integer(operand, "~")};
  case UnaryOperator::Not:
    return BooleanValue{.value = !require_boolean(operand, "NOT")};
  }
  throw EvaluationError("unknown unary operator");
}

Value evaluate_and(const Value &left, const Value &right) {
  const bool left_is_null = is_null(left);
  const bool right_is_null = is_null(right);
  const bool left_value = left_is_null ? false : require_boolean(left, "AND");
  const bool right_value =
      right_is_null ? false : require_boolean(right, "AND");
  if ((!left_is_null && !left_value) || (!right_is_null && !right_value)) {
    return BooleanValue{.value = false};
  }
  if (left_is_null || right_is_null) {
    return NullValue{};
  }
  return BooleanValue{.value = true};
}

Value evaluate_or(const Value &left, const Value &right) {
  const bool left_is_null = is_null(left);
  const bool right_is_null = is_null(right);
  const bool left_value = left_is_null ? false : require_boolean(left, "OR");
  const bool right_value = right_is_null ? false : require_boolean(right, "OR");
  if ((!left_is_null && left_value) || (!right_is_null && right_value)) {
    return BooleanValue{.value = true};
  }
  if (left_is_null || right_is_null) {
    return NullValue{};
  }
  return BooleanValue{.value = false};
}

template <typename Compare>
Value compare_values(const Value &left, const Value &right, Compare compare) {
  if (left.index() != right.index()) {
    throw EvaluationError("comparison requires operands of the same type");
  }
  if (const auto *value = std::get_if<IntegerValue>(&left)) {
    return BooleanValue{
        .value = compare(value->value, std::get<IntegerValue>(right).value)};
  }
  if (const auto *value = std::get_if<BooleanValue>(&left)) {
    return BooleanValue{
        .value = compare(value->value, std::get<BooleanValue>(right).value)};
  }
  if (const auto *value = std::get_if<StringValue>(&left)) {
    return BooleanValue{
        .value = compare(value->value, std::get<StringValue>(right).value)};
  }
  throw EvaluationError("unsupported comparison operand type");
}

std::int64_t checked_add(std::int64_t left, std::int64_t right) {
  if ((right > 0 && left > std::numeric_limits<std::int64_t>::max() - right) ||
      (right < 0 && left < std::numeric_limits<std::int64_t>::min() - right)) {
    throw EvaluationError("INTEGER overflow in +");
  }
  return left + right;
}

std::int64_t checked_subtract(std::int64_t left, std::int64_t right) {
  if ((right < 0 && left > std::numeric_limits<std::int64_t>::max() + right) ||
      (right > 0 && left < std::numeric_limits<std::int64_t>::min() + right)) {
    throw EvaluationError("INTEGER overflow in -");
  }
  return left - right;
}

std::int64_t checked_multiply(std::int64_t left, std::int64_t right) {
  if (left == 0 || right == 0) {
    return 0;
  }
  if ((left == -1 && right == std::numeric_limits<std::int64_t>::min()) ||
      (right == -1 && left == std::numeric_limits<std::int64_t>::min())) {
    throw EvaluationError("INTEGER overflow in *");
  }
  if ((left > 0 && right > 0 &&
       left > std::numeric_limits<std::int64_t>::max() / right) ||
      (left > 0 && right < 0 &&
       right < std::numeric_limits<std::int64_t>::min() / left) ||
      (left < 0 && right > 0 &&
       left < std::numeric_limits<std::int64_t>::min() / right) ||
      (left < 0 && right < 0 &&
       left < std::numeric_limits<std::int64_t>::max() / right)) {
    throw EvaluationError("INTEGER overflow in *");
  }
  return left * right;
}

Value evaluate_binary(const BinaryExpr &expr) {
  const Value left = ast_evaluate(expr.left);
  const Value right = ast_evaluate(expr.right);

  if (expr.op == BinaryOperator::And) {
    return evaluate_and(left, right);
  }
  if (expr.op == BinaryOperator::Or) {
    return evaluate_or(left, right);
  }
  if (is_null(left) || is_null(right)) {
    return NullValue{};
  }

  switch (expr.op) {
  case BinaryOperator::Concat:
    return StringValue{.value = require_string(left, "||") +
                                require_string(right, "||")};
  case BinaryOperator::Multiply:
    return IntegerValue{.value = checked_multiply(require_integer(left, "*"),
                                                  require_integer(right, "*"))};
  case BinaryOperator::Divide: {
    const auto lhs = require_integer(left, "/");
    const auto rhs = require_integer(right, "/");
    if (rhs == 0) {
      throw EvaluationError("division by zero");
    }
    if (lhs == std::numeric_limits<std::int64_t>::min() && rhs == -1) {
      throw EvaluationError("INTEGER overflow in /");
    }
    return IntegerValue{.value = lhs / rhs};
  }
  case BinaryOperator::Modulo: {
    const auto lhs = require_integer(left, "%");
    const auto rhs = require_integer(right, "%");
    if (rhs == 0) {
      throw EvaluationError("division by zero");
    }
    if (lhs == std::numeric_limits<std::int64_t>::min() && rhs == -1) {
      return IntegerValue{.value = 0};
    }
    return IntegerValue{.value = lhs % rhs};
  }
  case BinaryOperator::Add:
    return IntegerValue{.value = checked_add(require_integer(left, "+"),
                                             require_integer(right, "+"))};
  case BinaryOperator::Subtract:
    return IntegerValue{.value = checked_subtract(require_integer(left, "-"),
                                                  require_integer(right, "-"))};
  case BinaryOperator::BitwiseAnd:
    return IntegerValue{.value = require_integer(left, "&") &
                                 require_integer(right, "&")};
  case BinaryOperator::BitwiseOr:
    return IntegerValue{.value = require_integer(left, "|") |
                                 require_integer(right, "|")};
  case BinaryOperator::ShiftLeft:
  case BinaryOperator::ShiftRight: {
    const auto lhs = require_integer(left, "shift");
    const auto rhs = require_integer(right, "shift");
    if (rhs < 0 || rhs >= std::numeric_limits<std::uint64_t>::digits) {
      throw EvaluationError("shift count is out of range");
    }
    if (expr.op == BinaryOperator::ShiftLeft) {
      return IntegerValue{.value = static_cast<std::int64_t>(
                              static_cast<std::uint64_t>(lhs) << rhs)};
    }
    return IntegerValue{.value = lhs >> rhs};
  }
  case BinaryOperator::Less:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a < b; });
  case BinaryOperator::Greater:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a > b; });
  case BinaryOperator::LessEqual:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a <= b; });
  case BinaryOperator::GreaterEqual:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a >= b; });
  case BinaryOperator::Equal:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a == b; });
  case BinaryOperator::NotEqual:
    return compare_values(left, right,
                          [](const auto &a, const auto &b) { return a != b; });
  case BinaryOperator::And:
  case BinaryOperator::Or:
    break;
  }
  throw EvaluationError("unknown binary operator");
}

} // namespace

EvaluationError::EvaluationError(std::string message)
    : std::runtime_error(std::move(message)) {}

Value ast_evaluate(const Expr &expr) {
  return std::visit(
      [](const auto &node) -> Value {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, LiteralExpr>) {
          return evaluate_literal(node);
        } else if constexpr (std::is_same_v<T, IdentifierExpr>) {
          return NullValue{};
        } else if constexpr (std::is_same_v<T, UnaryExpr>) {
          return evaluate_unary(node);
        } else if constexpr (std::is_same_v<T, BinaryExpr>) {
          return evaluate_binary(node);
        } else {
          throw EvaluationError(
              "expression node is not supported by the evaluator");
        }
      },
      expr.node);
}

Value ast_evaluate(const ExprPtr &expr) {
  if (expr == nullptr) {
    throw EvaluationError("cannot evaluate a null expression pointer");
  }
  return ast_evaluate(*expr);
}

} // namespace minidb
