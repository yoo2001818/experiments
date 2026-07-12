#pragma once

#include "minidb/ast.hpp"

#include <functional>

namespace minidb {

using AstRewriteCallback = std::function<ExprPtr(ExprPtr)>;

// Returns a rewritten copy of expr. The callback runs once per expression after
// its children have been rewritten and must return a non-null expression.
ExprPtr ast_rewrite(const ExprPtr &expr, const AstRewriteCallback &callback);

} // namespace minidb
