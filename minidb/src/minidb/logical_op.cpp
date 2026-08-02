#include "minidb/logical_op.hpp"
#include "minidb/ast.hpp"
#include "minidb/ast_rewrite.hpp"

#include <algorithm>
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_set>
#include <utility>
#include <variant>

namespace minidb {
namespace {

ExprPtr bound_ref(std::uint32_t index) {
  return std::make_shared<Expr>(Expr{
      .node = BoundRefExpr{.ref = RefSlot{.index = index}},
  });
}

ExprPtr identifier_expr(std::vector<std::string> parts) {
  return std::make_shared<Expr>(Expr{
      .node =
          IdentifierExpr{
              .name = Identifier{.parts = std::move(parts)},
          },
  });
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

std::string join_type_text(JoinType type) {
  switch (type) {
  case JoinType::Cross:
    return "CrossJoin";
  case JoinType::Inner:
    return "InnerJoin";
  case JoinType::Left:
    return "LeftJoin";
  case JoinType::Right:
    return "RightJoin";
  case JoinType::Full:
    return "FullJoin";
  }
  return "Join";
}

} // namespace

BinderError::BinderError(std::string message)
    : std::runtime_error(std::move(message)) {}

FrameDescriptor OneRowLogicalOp::get_frame() { return {}; }

std::optional<RefSlot> OneRowLogicalOp::get_column(ExprPtr) {
  return std::nullopt;
}

std::string OneRowLogicalOp::explain_label() const { return "OneRow"; }

std::vector<LogicalOpPtr> OneRowLogicalOp::inputs() const { return {}; }

ScanLogicalOp::ScanLogicalOp(Table &table, std::optional<std::string> alias)
    : table_(&table), alias_(std::move(alias)),
      relation_name_(alias_.value_or(table.schema().name)), frame_() {}

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
              (parts.size() == 2 && parts.front() != relation_name_)) {
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
                          .qualifiers = {relation_name_},
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

std::string ScanLogicalOp::explain_label() const {
  std::string label = "Scan table=" + table_->schema().name;
  if (alias_.has_value()) {
    label += " alias=" + *alias_;
  }
  return label;
}

std::vector<LogicalOpPtr> ScanLogicalOp::inputs() const { return {}; }

JoinLogicalOp::JoinLogicalOp(LogicalOpPtr left, LogicalOpPtr right,
                             JoinType type, std::optional<ExprPtr> condition)
    : left_(std::move(left)), right_(std::move(right)), type_(type) {
  if (condition.has_value()) {
    condition_ = bind_identifiers(*condition, *this);
  }
}

FrameDescriptor JoinLogicalOp::get_frame() {
  const auto left_size = static_cast<std::uint32_t>(left_->get_frame().size());
  FrameDescriptor result;
  result.reserve(frame_.size());
  for (const auto &column : frame_) {
    auto descriptor = column.descriptor;
    descriptor.expr =
        bound_ref(column.from_right ? left_size + column.input_ref.index
                                    : column.input_ref.index);
    result.push_back(std::move(descriptor));
  }
  return result;
}

std::optional<RefSlot> JoinLogicalOp::get_column(ExprPtr expr) {
  const auto left_ref = left_->get_column(expr);
  const auto right_ref = right_->get_column(expr);
  if (left_ref.has_value() && right_ref.has_value()) {
    const auto *identifier = std::get_if<IdentifierExpr>(&expr->node);
    throw BinderError("ambiguous binder identifier: " +
                      (identifier == nullptr
                           ? format_expression(expr)
                           : identifier_text(identifier->name)));
  }
  if (!left_ref.has_value() && !right_ref.has_value()) {
    return std::nullopt;
  }

  const bool from_right = right_ref.has_value();
  const RefSlot input_ref = from_right ? *right_ref : *left_ref;
  for (std::uint32_t index = 0; index < frame_.size(); ++index) {
    if (frame_[index].from_right == from_right &&
        frame_[index].input_ref.index == input_ref.index) {
      return RefSlot{.index = index};
    }
  }

  const auto input_frame =
      from_right ? right_->get_frame() : left_->get_frame();
  if (input_ref.index >= input_frame.size()) {
    throw BinderError("join input reference is out of range");
  }
  const auto ref = RefSlot{.index = static_cast<std::uint32_t>(frame_.size())};
  frame_.push_back(JoinFrameColumnDescriptor{
      .descriptor = input_frame[input_ref.index],
      .from_right = from_right,
      .input_ref = input_ref,
  });
  return ref;
}

std::string JoinLogicalOp::explain_label() const {
  std::string label = join_type_text(type_);
  if (condition_.has_value()) {
    label += " condition=" + format_expression(*condition_);
  }
  return label;
}

std::vector<LogicalOpPtr> JoinLogicalOp::inputs() const {
  return {left_, right_};
}

FilterLogicalOp::FilterLogicalOp(LogicalOpPtr parent, ExprPtr expr)
    : parent_(std::move(parent)), expr_(bind_identifiers(expr, *parent_)) {}

FrameDescriptor FilterLogicalOp::get_frame() { return parent_->get_frame(); }

std::optional<RefSlot> FilterLogicalOp::get_column(ExprPtr expr) {
  return parent_->get_column(std::move(expr));
}

std::string FilterLogicalOp::explain_label() const {
  return "Filter predicate=" + format_expression(expr_);
}

std::vector<LogicalOpPtr> FilterLogicalOp::inputs() const { return {parent_}; }

ProjectLogicalOp::ProjectLogicalOp(LogicalOpPtr parent,
                                   const FrameDescriptor &frame)
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
  if (const auto *identifier = std::get_if<IdentifierExpr>(&expr->node);
      identifier != nullptr && identifier->name.parts.size() == 1) {
    std::optional<RefSlot> named_match;
    for (std::uint32_t index = 0; index < frame_.size(); ++index) {
      if (frame_[index].name != identifier->name.parts.front()) {
        continue;
      }
      if (named_match.has_value()) {
        throw BinderError("ambiguous projection name: " +
                          identifier->name.parts.front());
      }
      named_match = RefSlot{.index = index};
    }
    if (named_match.has_value()) {
      return named_match;
    }
  }

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

std::string ProjectLogicalOp::explain_label() const { return "Project"; }

std::vector<LogicalOpPtr> ProjectLogicalOp::inputs() const { return {parent_}; }

SortLogicalOp::SortLogicalOp(LogicalOpPtr parent,
                             const std::vector<OrderByTerm> &order_by)
    : parent_(std::move(parent)) {
  terms_.reserve(order_by.size());
  for (const auto &term : order_by) {
    ExprPtr expr;
    if (const auto *ordinal = std::get_if<std::uint32_t>(&term.key)) {
      const auto frame = parent_->get_frame();
      const auto visible_count = std::count_if(
          frame.begin(), frame.end(),
          [](const FrameColumnDescriptor &column) { return column.visible; });
      if (*ordinal == 0 || *ordinal > visible_count) {
        throw BinderError("ORDER BY position is out of range");
      }
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

std::string SortLogicalOp::explain_label() const {
  std::ostringstream out;
  out << "Sort keys=[";
  for (std::size_t index = 0; index < terms_.size(); ++index) {
    if (index != 0) {
      out << ", ";
    }
    out << format_expression(terms_[index].expr) << ' '
        << (terms_[index].direction == SortDirection::Asc ? "ASC" : "DESC");
  }
  out << ']';
  return out.str();
}

std::vector<LogicalOpPtr> SortLogicalOp::inputs() const { return {parent_}; }

DistinctLogicalOp::DistinctLogicalOp(LogicalOpPtr parent)
    : parent_(std::move(parent)) {}

FrameDescriptor DistinctLogicalOp::get_frame() { return parent_->get_frame(); }

std::optional<RefSlot> DistinctLogicalOp::get_column(ExprPtr expr) {
  return parent_->get_column(std::move(expr));
}

std::string DistinctLogicalOp::explain_label() const { return "Distinct"; }

std::vector<LogicalOpPtr> DistinctLogicalOp::inputs() const {
  return {parent_};
}

LimitLogicalOp::LimitLogicalOp(LogicalOpPtr parent, LimitClause limit)
    : parent_(std::move(parent)), limit_(std::move(limit)) {}

FrameDescriptor LimitLogicalOp::get_frame() { return parent_->get_frame(); }

std::optional<RefSlot> LimitLogicalOp::get_column(ExprPtr expr) {
  return parent_->get_column(std::move(expr));
}

std::string LimitLogicalOp::explain_label() const {
  std::string label = "Limit count=" + format_expression(limit_.row_count);
  if (limit_.offset.has_value()) {
    label += " offset=" + format_expression(*limit_.offset);
  }
  return label;
}

std::vector<LogicalOpPtr> LimitLogicalOp::inputs() const { return {parent_}; }

namespace {

struct PlannedRelation {
  Table *table;
  std::string qualifier;
  LogicalOpPtr scan;
};

std::string projection_name(const ExprSelectItem &item) {
  if (item.alias.has_value()) {
    return *item.alias;
  }
  if (const auto *identifier = std::get_if<IdentifierExpr>(&item.expr->node);
      identifier != nullptr && !identifier->name.parts.empty()) {
    return identifier->name.parts.back();
  }
  return format_expression(item.expr);
}

void append_wildcard_columns(FrameDescriptor &frame,
                             const WildcardSelectItem &wildcard,
                             const std::vector<PlannedRelation> &relations) {
  const std::optional<std::string> requested =
      wildcard.qualifier.has_value()
          ? std::optional<std::string>{identifier_text(*wildcard.qualifier)}
          : std::nullopt;
  bool matched = false;
  for (const auto &relation : relations) {
    if (requested.has_value() && *requested != relation.qualifier) {
      continue;
    }
    matched = true;
    for (const auto &column : relation.table->schema().columns) {
      frame.push_back(FrameColumnDescriptor{
          .name = column.name,
          .expr = identifier_expr({relation.qualifier, column.name}),
          .visible = true,
      });
    }
  }
  if (!matched) {
    throw BinderError(requested.has_value()
                          ? "unknown wildcard qualifier: " + *requested
                          : "SELECT * requires a table");
  }
}

std::string frame_column_name(const FrameColumnDescriptor &column) {
  std::string name;
  if (column.qualifiers.size() == 1) {
    name = column.qualifiers.front() + ".";
  }
  name += column.name.empty() ? "<expression>" : column.name;
  return name;
}

std::string format_frame(const FrameDescriptor &frame) {
  if (frame.empty()) {
    return "[]";
  }
  std::ostringstream out;
  out << '[';
  for (std::size_t index = 0; index < frame.size(); ++index) {
    if (index != 0) {
      out << ", ";
    }
    out << '#' << index << ' ' << frame_column_name(frame[index]) << " <- "
        << format_expression(frame[index].expr);
    if (!frame[index].visible) {
      out << " hidden";
    }
  }
  out << ']';
  return out.str();
}

void append_plan(std::ostringstream &out, LogicalOp &op,
                 const std::string &prefix, bool is_last, bool is_root) {
  out << prefix;
  if (!is_root) {
    out << (is_last ? "`- " : "|- ");
  }
  out << op.explain_label() << '\n';

  const std::string child_prefix =
      prefix + (is_root ? "" : (is_last ? "   " : "|  "));
  out << child_prefix << (is_root ? "   " : "")
      << "frame=" << format_frame(op.get_frame()) << '\n';

  const auto children = op.inputs();
  for (std::size_t index = 0; index < children.size(); ++index) {
    append_plan(out, *children[index], child_prefix,
                index + 1 == children.size(), false);
  }
}

} // namespace

LogicalOpPtr make_logical_plan(const SelectStmt &stmt,
                               const TableResolver &resolve_table) {
  if (!resolve_table && !stmt.from.empty()) {
    throw BinderError("table resolver is empty");
  }
  if (stmt.from.empty() &&
      std::any_of(stmt.select_list.begin(), stmt.select_list.end(),
                  [](const SelectItem &item) {
                    return std::holds_alternative<WildcardSelectItem>(item);
                  })) {
    throw BinderError("SELECT * requires a table");
  }
  if (!stmt.joins.empty() && stmt.joins.size() + 1 != stmt.from.size()) {
    throw BinderError("join list does not match FROM list");
  }

  std::vector<PlannedRelation> relations;
  relations.reserve(stmt.from.size());
  std::unordered_set<std::string> relation_names;
  for (const auto &reference : stmt.from) {
    Table &table = resolve_table(reference.table_name);
    const std::string qualifier = reference.alias.value_or(table.schema().name);
    if (!relation_names.insert(qualifier).second) {
      throw BinderError("duplicate table name or alias: " + qualifier);
    }

    auto scan = std::make_shared<ScanLogicalOp>(table, reference.alias);
    for (const auto &column : table.schema().columns) {
      if (!scan->get_column(identifier_expr({qualifier, column.name}))
               .has_value()) {
        throw BinderError("unknown binder identifier: " + qualifier + "." +
                          column.name);
      }
    }
    relations.push_back(PlannedRelation{
        .table = &table,
        .qualifier = qualifier,
        .scan = std::move(scan),
    });
  }

  LogicalOpPtr root;
  if (relations.empty()) {
    root = std::make_shared<OneRowLogicalOp>();
  } else {
    root = relations.front().scan;
    for (std::size_t index = 1; index < relations.size(); ++index) {
      const JoinClause join = stmt.joins.empty()
                                  ? JoinClause{.type = JoinType::Cross}
                                  : stmt.joins[index - 1];
      root = std::make_shared<JoinLogicalOp>(
          std::move(root), relations[index].scan, join.type, join.condition);
    }
  }

  if (stmt.where.has_value()) {
    root = std::make_shared<FilterLogicalOp>(std::move(root), *stmt.where);
  }

  FrameDescriptor projection;
  for (const auto &select_item : stmt.select_list) {
    if (const auto *wildcard = std::get_if<WildcardSelectItem>(&select_item)) {
      append_wildcard_columns(projection, *wildcard, relations);
    } else {
      const auto &item = std::get<ExprSelectItem>(select_item);
      projection.push_back(FrameColumnDescriptor{
          .name = projection_name(item),
          .expr = item.expr,
          .visible = true,
      });
    }
  }
  root = std::make_shared<ProjectLogicalOp>(std::move(root), projection);

  if (stmt.distinct) {
    root = std::make_shared<DistinctLogicalOp>(std::move(root));
  }
  if (!stmt.order_by.empty()) {
    root = std::make_shared<SortLogicalOp>(std::move(root), stmt.order_by);
  }
  if (stmt.limit.has_value()) {
    root = std::make_shared<LimitLogicalOp>(std::move(root), *stmt.limit);
  }
  return root;
}

std::string format_logical_plan(LogicalOp &root) {
  std::ostringstream out;
  append_plan(out, root, "", true, true);
  std::string result = out.str();
  if (!result.empty()) {
    result.pop_back();
  }
  return result;
}

} // namespace minidb
