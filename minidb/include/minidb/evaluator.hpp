#pragma once

#include "minidb/ast.hpp"
#include "minidb/value.hpp"

#include <stdexcept>
#include <string>
#include <vector>

namespace minidb {

class EvaluationError : public std::runtime_error {
public:
  explicit EvaluationError(std::string message);
};

using DataRow = std::vector<Value>;

Value ast_evaluate(const Expr &expr);
Value ast_evaluate(const ExprPtr &expr);
Value ast_evaluate(const Expr &expr, const DataRow &input);
Value ast_evaluate(const ExprPtr &expr, const DataRow &input);

} // namespace minidb
