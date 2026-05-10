#pragma once

#include "minidb/catalog.hpp"
#include "minidb/storage.hpp"
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iterator>
#include <optional>
#include <span>
#include <string>
#include <variant>
#include <vector>

namespace minidb {
using BinaryValue = std::vector<std::uint8_t>;

using Value =
    std::variant<std::nullptr_t, std::int64_t, bool, std::string, BinaryValue>;

struct Row {
  std::vector<Value> values;
};

class Table;

struct RowEntry {
  std::int64_t row_offset;
  Row row;
};

class TableRowIterator {
public:
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::input_iterator_tag;
  using iterator_concept = std::input_iterator_tag;
  using pointer = const RowEntry *;
  using reference = const RowEntry &;
  using value_type = RowEntry;

  TableRowIterator() = default;
  explicit TableRowIterator(Table &table);

  reference operator*() const;
  pointer operator->() const;
  TableRowIterator &operator++();
  void operator++(int);
  bool operator==(std::default_sentinel_t) const;

  std::optional<RowEntry> next();

private:
  Table *table_;
  std::int64_t next_offset_ = 0;
  std::optional<RowEntry> current_;

  void advance_();
};

class TableRowScan {
public:
  explicit TableRowScan(Table &table);

  TableRowIterator begin();
  std::default_sentinel_t end() const;

private:
  Table *table_;
};

class Table {
public:
  explicit Table(TableSchema schema, std::filesystem::path working_dir);
  ~Table();

  Table(Table &&) noexcept;
  Table &operator=(Table &&) noexcept = delete;
  Table(const Table &) = delete;
  Table &operator=(const Table &) = delete;

  void create();
  void open();
  void drop();
  void flush();

  void insert(Row const &row);
  std::optional<Row> read(std::int64_t row_offset);
  TableRowScan scan();
  void delete_row(std::int64_t row_offset);
  std::int64_t size();

  const TableSchema &schema() const;
  const std::vector<ColumnSchema> &columns() const;

private:
  // This must remain constant as this is a copy of the root database's schema
  const TableSchema schema_;
  std::filesystem::path working_dir_;
  std::filesystem::path table_path_;

  int row_size_;
  std::vector<int> column_offsets_;

  BinaryFile file_;

  Row decode_row_(std::span<std::byte const> in);
  void encode_row_(Row &row, std::span<std::byte> out);
};
} // namespace minidb
