#pragma once

#include "minidb/ast.hpp"
#include "minidb/table.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace minidb {

struct FrameColumnDescriptor {
  std::uint32_t index;
  std::string name;
  ExprPtr expr;
};

using FrameDescriptor = std::vector<FrameColumnDescriptor>;

class LogicalOp {
public:
  virtual ~LogicalOp() = default;

  virtual FrameDescriptor get_frame() = 0;
};

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

} // namespace minidb
