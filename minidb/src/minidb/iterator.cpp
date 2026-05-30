#include "minidb/iterator.hpp"

namespace minidb {

TableScanIterator::TableScanIterator(Table &table) : table_(&table) {}

void TableScanIterator::open() { scan_.emplace(*table_); }

std::optional<RowEntry> TableScanIterator::next() {
  if (!scan_.has_value()) {
    return std::nullopt;
  }
  return scan_->next();
}

void TableScanIterator::close() { scan_.reset(); }

} // namespace minidb
