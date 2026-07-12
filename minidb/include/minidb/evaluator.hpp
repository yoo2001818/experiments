#pragma once

#include "minidb/ast.hpp"
#include "minidb/value.hpp"

#include <stdexcept>
#include <string>

namespace minidb {

class EvaluationError : public std::runtime_error {
public:
  explicit EvaluationError(std::string message);
};

Value ast_evaluate(const Expr &expr);
Value ast_evaluate(const ExprPtr &expr);

} // namespace minidb
