#pragma once

#include "minidb/table.hpp"

namespace minidb {

struct FrameLayout {};

class LogicalOp {
public:
};

class ScanLogicalOp : public LogicalOp {
public:
  ScanLogicalOp(Table &table);
};

} // namespace minidb
