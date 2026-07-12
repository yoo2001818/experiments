#include "minidb/ast_rewrite.hpp"

#include <stdexcept>
#include <type_traits>
#include <utility>

namespace minidb {
namespace {

void rewrite_child(ExprPtr &child, const AstRewriteCallback &callback) {
  child = ast_rewrite(child, callback);
}

void rewrite_optional_child(std::optional<ExprPtr> &child,
                            const AstRewriteCallback &callback) {
  if (child.has_value()) {
    rewrite_child(*child, callback);
  }
}

void rewrite_children(Expr &expr, const AstRewriteCallback &callback) {
  std::visit(
      [&callback](auto &node) {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, UnaryExpr>) {
          rewrite_child(node.operand, callback);
        } else if constexpr (std::is_same_v<T, BinaryExpr>) {
          rewrite_child(node.left, callback);
          rewrite_child(node.right, callback);
        } else if constexpr (std::is_same_v<T, FunctionCallExpr>) {
          for (auto &argument : node.arguments) {
            rewrite_child(argument, callback);
          }
        } else if constexpr (std::is_same_v<T, IsExpr>) {
          rewrite_child(node.value, callback);
          rewrite_child(node.test, callback);
        } else if constexpr (std::is_same_v<T, BetweenExpr>) {
          rewrite_child(node.value, callback);
          rewrite_child(node.lower, callback);
          rewrite_child(node.upper, callback);
        } else if constexpr (std::is_same_v<T, InExpr>) {
          rewrite_child(node.value, callback);
          for (auto &value : node.values) {
            rewrite_child(value, callback);
          }
        } else if constexpr (std::is_same_v<T, LikeExpr>) {
          rewrite_child(node.value, callback);
          rewrite_child(node.pattern, callback);
        } else if constexpr (std::is_same_v<T, CaseExpr>) {
          rewrite_optional_child(node.operand, callback);
          for (auto &when : node.when_clauses) {
            rewrite_child(when.condition, callback);
            rewrite_child(when.result, callback);
          }
          rewrite_optional_child(node.else_expr, callback);
        }
      },
      expr.node);
}

} // namespace

ExprPtr ast_rewrite(const ExprPtr &expr, const AstRewriteCallback &callback) {
  if (expr == nullptr) {
    throw std::invalid_argument("cannot rewrite a null expression pointer");
  }
  if (!callback) {
    throw std::invalid_argument("AST rewrite callback is empty");
  }

  auto rewritten = std::make_shared<Expr>(*expr);
  rewrite_children(*rewritten, callback);
  rewritten = callback(std::move(rewritten));
  if (rewritten == nullptr) {
    throw std::invalid_argument("AST rewrite callback returned null");
  }
  return rewritten;
}

} // namespace minidb
