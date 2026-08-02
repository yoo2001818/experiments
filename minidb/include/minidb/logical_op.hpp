#pragma once

#include "minidb/ast.hpp"
#include "minidb/enum.hpp"
#include "minidb/table.hpp"

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace minidb {

class BinderError : public std::runtime_error {
public:
  explicit BinderError(std::string message);
};

struct FrameColumnDescriptor {
  std::string name;
  ExprPtr expr;
  bool visible;
  std::vector<std::string> qualifiers;
};

using FrameDescriptor = std::vector<FrameColumnDescriptor>;

class LogicalOp;
using LogicalOpPtr = std::shared_ptr<LogicalOp>;

class LogicalOp {
public:
  virtual ~LogicalOp() = default;

  virtual FrameDescriptor get_frame() = 0;
  virtual std::optional<RefSlot> get_column(ExprPtr expr) = 0;
  virtual std::string explain_label() const = 0;
  virtual std::vector<LogicalOpPtr> inputs() const = 0;
};

class OneRowLogicalOp : public LogicalOp {
public:
  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;
};

class ScanLogicalOp : public LogicalOp {
public:
  explicit ScanLogicalOp(Table &table,
                         std::optional<std::string> alias = std::nullopt);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  struct ScanFrameColumnDescriptor {
    FrameColumnDescriptor descriptor;
    std::uint32_t column_index;
  };

  Table *table_;
  std::optional<std::string> alias_;
  std::string relation_name_;
  std::vector<ScanFrameColumnDescriptor> frame_;
};

class JoinLogicalOp : public LogicalOp {
public:
  JoinLogicalOp(LogicalOpPtr left, LogicalOpPtr right, JoinType type,
                std::optional<ExprPtr> condition = std::nullopt);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  struct JoinFrameColumnDescriptor {
    FrameColumnDescriptor descriptor;
    bool from_right;
    RefSlot input_ref;
  };

  LogicalOpPtr left_;
  LogicalOpPtr right_;
  JoinType type_;
  std::optional<ExprPtr> condition_;
  std::vector<JoinFrameColumnDescriptor> frame_;
};

class FilterLogicalOp : public LogicalOp {
public:
  explicit FilterLogicalOp(LogicalOpPtr parent, ExprPtr expr);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  LogicalOpPtr parent_;
  ExprPtr expr_;
};

class ProjectLogicalOp : public LogicalOp {
public:
  explicit ProjectLogicalOp(LogicalOpPtr parent, const FrameDescriptor &frame);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  LogicalOpPtr parent_;
  FrameDescriptor frame_;
};

class SortLogicalOp : public LogicalOp {
public:
  explicit SortLogicalOp(LogicalOpPtr parent,
                         const std::vector<OrderByTerm> &order_by);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  struct SortTermDescriptor {
    ExprPtr expr;
    SortDirection direction;
  };

  LogicalOpPtr parent_;
  std::vector<SortTermDescriptor> terms_;
};

class DistinctLogicalOp : public LogicalOp {
public:
  explicit DistinctLogicalOp(LogicalOpPtr parent);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  LogicalOpPtr parent_;
};

class LimitLogicalOp : public LogicalOp {
public:
  LimitLogicalOp(LogicalOpPtr parent, LimitClause limit);

  FrameDescriptor get_frame() override;
  std::optional<RefSlot> get_column(ExprPtr expr) override;
  std::string explain_label() const override;
  std::vector<LogicalOpPtr> inputs() const override;

private:
  LogicalOpPtr parent_;
  LimitClause limit_;
};

using TableResolver = std::function<Table &(const Identifier &)>;

LogicalOpPtr make_logical_plan(const SelectStmt &stmt,
                               const TableResolver &resolve_table);
std::string format_logical_plan(LogicalOp &root);

} // namespace minidb
