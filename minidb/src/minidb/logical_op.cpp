#include "minidb/logical_op.hpp"
#include "minidb/ast.hpp"
#include "minidb/ast_rewrite.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <variant>

namespace minidb {
namespace {

ExprPtr bound_ref(std::uint32_t index) {
  return std::make_shared<Expr>(Expr{
      .node = BoundRefExpr{.ref = RefSlot{.index = index}},
  });
}

ExprPtr bind_identifiers(const ExprPtr &expr, LogicalOp &parent) {
  return ast_rewrite(expr, [&](ExprPtr expr) -> ExprPtr {
    if (std::holds_alternative<IdentifierExpr>(expr->node)) {
      auto ref = parent.get_column(expr);
      if (ref.has_value()) {
        return std::make_shared<Expr>(
            Expr{.node = BoundRefExpr{.ref = ref.value()}});
      } else {
        throw BinderError("unknown binder identifier");
      }
    } else {
      return expr;
    }
  });
}

} // namespace

BinderError::BinderError(std::string message)
    : std::runtime_error(std::move(message)) {}

ScanLogicalOp::ScanLogicalOp(Table &table) : table_(&table), frame_() {}

FrameDescriptor ScanLogicalOp::get_frame() {
  FrameDescriptor frame;
  frame.reserve(frame_.size());
  for (const auto &column : frame_) {
    frame.push_back(column.descriptor);
  }
  return frame;
}

std::optional<RefSlot> ScanLogicalOp::get_column(ExprPtr expr) {
  return std::visit(
      [this](const auto &node) -> std::optional<RefSlot> {
        using T = std::decay_t<decltype(node)>;
        if constexpr (std::is_same_v<T, IdentifierExpr>) {
          const auto &parts = node.name.parts;
          const auto &schema = table_->schema();
          if (parts.empty() || parts.size() > 2 ||
              (parts.size() == 2 && parts.front() != schema.name)) {
            return std::nullopt;
          }

          const auto &column_name = parts.back();
          for (std::uint32_t index = 0; index < frame_.size(); ++index) {
            if (frame_[index].descriptor.name == column_name) {
              return RefSlot{.index = index};
            }
          }

          for (std::uint32_t index = 0; index < schema.columns.size();
               ++index) {
            if (schema.columns[index].name == column_name) {
              const auto ref =
                  RefSlot{.index = static_cast<std::uint32_t>(frame_.size())};
              frame_.push_back({
                  .descriptor =
                      FrameColumnDescriptor{
                          .name = column_name,
                          .expr = bound_ref(index),
                          .visible = true,
                      },
                  .column_index = index,
              });
              return ref;
            }
          }

          return std::nullopt;
        } else {
          return std::nullopt;
        }
      },
      expr->node);
}

FilterLogicalOp::FilterLogicalOp(LogicalOpPtr parent, ExprPtr expr)
    : parent_(std::move(parent)), expr_(bind_identifiers(expr, *parent_)) {}

FrameDescriptor FilterLogicalOp::get_frame() { return parent_->get_frame(); }

std::optional<RefSlot> FilterLogicalOp::get_column(ExprPtr expr) {
  return parent_->get_column(std::move(expr));
}

ProjectLogicalOp::ProjectLogicalOp(LogicalOpPtr parent, FrameDescriptor &frame)
    : parent_(std::move(parent)) {
  frame_.reserve(frame.size());
  for (const auto &column : frame) {
    frame_.push_back({
        .name = column.name,
        .expr = bind_identifiers(column.expr, *parent_),
        .visible = column.visible,
    });
  }
}

FrameDescriptor ProjectLogicalOp::get_frame() { return frame_; }

std::optional<RefSlot> ProjectLogicalOp::get_column(ExprPtr expr) {
  auto bound_expr = bind_identifiers(expr, *parent_);
  for (std::uint32_t index = 0; index < frame_.size(); ++index) {
    if (*frame_[index].expr == *bound_expr) {
      return RefSlot{.index = index};
    }
  }

  const auto ref = RefSlot{.index = static_cast<std::uint32_t>(frame_.size())};
  frame_.push_back({
      .name = "",
      .expr = std::move(bound_expr),
      .visible = false,
  });
  return ref;
}

SortLogicalOp::SortLogicalOp(LogicalOpPtr parent,
                             std::vector<OrderByTerm> &order_by)
    : parent_(std::move(parent)) {
  terms_.reserve(order_by.size());
  for (const auto &term : order_by) {
    ExprPtr expr;
    if (const auto *ordinal = std::get_if<std::uint32_t>(&term.key)) {
      expr = bound_ref(*ordinal - 1);
    } else {
      auto ref = parent_->get_column(std::get<ExprPtr>(term.key));
      if (!ref.has_value()) {
        throw BinderError("unknown binder identifier");
      }
      expr = bound_ref(ref->index);
    }

    terms_.push_back(SortTermDescriptor{
        .expr = std::move(expr),
        .direction = term.direction.value_or(SortDirection::Asc),
    });
  }
}

FrameDescriptor SortLogicalOp::get_frame() { return parent_->get_frame(); }

std::optional<RefSlot> SortLogicalOp::get_column(ExprPtr expr) {
  return parent_->get_column(std::move(expr));
}

} // namespace minidb
