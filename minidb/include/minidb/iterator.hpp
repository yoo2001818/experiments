#pragma once

#include "minidb/table.hpp"

#include <optional>

namespace minidb {

class RowIterator {
public:
  virtual ~RowIterator() = default;

  virtual void open() = 0;
  virtual std::optional<RowEntry> next() = 0;
  virtual void close() = 0;
};

class TableScanIterator : public RowIterator {
public:
  explicit TableScanIterator(Table &table);

  void open() override;
  std::optional<RowEntry> next() override;
  void close() override;

private:
  Table *table_;
  std::optional<TableRowIterator> scan_;
};

} // namespace minidb
