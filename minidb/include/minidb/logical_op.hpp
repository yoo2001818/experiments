#pragma once

#include "minidb/ast.hpp"
#include "minidb/enum.hpp"
#include "minidb/table.hpp"

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace minidb {

struct FrameColumnDescriptor {
  std::uint32_t index;
  std::string name;
  ExprPtr expr;
  bool visible;
};

using FrameDescriptor = std::vector<FrameColumnDescriptor>;

class LogicalOp {
public:
  virtual ~LogicalOp() = default;

  virtual FrameDescriptor get_frame() = 0;
};

using LogicalOpPtr = std::shared_ptr<LogicalOp>;

class ScanLogicalOp : public LogicalOp {
public:
  explicit ScanLogicalOp(Table &table);

  FrameDescriptor get_frame() override;

private:
  struct ScanFrameColumnDescriptor {
    FrameColumnDescriptor descriptor;
    std::uint32_t column_index;
  };

  Table *table_;
  std::vector<ScanFrameColumnDescriptor> frame_;
};

class FilterLogicalOp : public LogicalOp {
public:
  explicit FilterLogicalOp(LogicalOpPtr parent, ExprPtr expr);

  FrameDescriptor get_frame() override;

private:
  LogicalOpPtr parent_;
  ExprPtr expr_;
};

class ProjectLogicalOp : public LogicalOp {
public:
  explicit ProjectLogicalOp(LogicalOpPtr parent, FrameDescriptor &frame);

  FrameDescriptor get_frame() override;

private:
  LogicalOpPtr parent_;
  FrameDescriptor frame_;
};

class SortLogicalOp : public LogicalOp {
public:
  explicit SortLogicalOp(LogicalOpPtr parent,
                         std::vector<OrderByTerm> &order_by);

  FrameDescriptor get_frame() override;

private:
  struct SortTermDescriptor {
    ExprPtr expr;
    SortDirection direction;
  };

  LogicalOpPtr parent_;
  std::vector<SortTermDescriptor> terms_;
};

} // namespace minidb
