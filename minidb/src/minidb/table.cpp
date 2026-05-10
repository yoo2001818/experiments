#include "minidb/table.hpp"
#include "minidb/ddl_ast.hpp"
#include "minidb/util_binary.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <limits>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <variant>

namespace minidb {
namespace {

constexpr std::array<std::byte, 4> kRowsMagic{
    std::byte{'m'},
    std::byte{'d'},
    std::byte{'r'},
    std::byte{'w'},
};
constexpr std::uint32_t kRowsVersion = 1;
constexpr std::uint64_t kRowsHeaderSize = 8;

std::string format_storage_name(char prefix, std::uint32_t id) {
  std::ostringstream out;
  out << prefix << '_' << std::setw(8) << std::setfill('0') << id;
  return out.str();
}

std::uint32_t checked_u32_add(std::uint32_t value) {
  if (value == std::numeric_limits<std::uint32_t>::max()) {
    throw std::runtime_error("storage id overflow");
  }
  return value + 1;
}

std::uint32_t required_type_size(const ColumnSchema &column) {
  if (!column.type_size.has_value() || *column.type_size == 0) {
    throw std::runtime_error("column requires a positive type size: " +
                             column.name);
  }
  return *column.type_size;
}

int column_payload_size(const ColumnSchema &column) {
  switch (column.type) {
  case ColumnType::Integer:
    return 8;
  case ColumnType::Boolean:
    return 1;
  case ColumnType::Char:
    return static_cast<int>(required_type_size(column));
  case ColumnType::Binary:
    return static_cast<int>(2 + required_type_size(column));
  }
  throw std::runtime_error("unknown column type");
}

ColumnType ddl_column_type(const DataType &data_type) {
  if (std::holds_alternative<IntegerType>(data_type))
    return ColumnType::Integer;
  if (std::holds_alternative<BooleanType>(data_type))
    return ColumnType::Boolean;
  if (std::holds_alternative<CharType>(data_type))
    return ColumnType::Char;
  if (std::holds_alternative<BinaryType>(data_type))
    return ColumnType::Binary;
  throw std::runtime_error("unknown DDL data type");
}

std::optional<std::uint32_t> ddl_type_size(const DataType &data_type) {
  if (const auto *type = std::get_if<CharType>(&data_type)) {
    return type->length;
  }
  if (const auto *type = std::get_if<BinaryType>(&data_type)) {
    return type->length;
  }
  return std::nullopt;
}

bool ddl_is_nullable(const ColumnDefinition &column) {
  if (column.primary_key)
    return false;
  return column.nullability != Nullability::NotNull;
}

SortDirection schema_direction(const std::optional<SortDirection> &direction) {
  return direction.value_or(SortDirection::Asc);
}

std::filesystem::path tables_dir(const std::filesystem::path &database_path) {
  return database_path / "tables";
}

std::filesystem::path catalog_path(const std::filesystem::path &database_path) {
  return database_path / "catalog.json";
}

} // namespace

Table::Table(TableSchema schema, std::filesystem::path working_dir)
    : schema_(std::move(schema)), working_dir_(std::move(working_dir)),
      table_path_(tables_dir(working_dir_) / schema_.storage_name),
      row_size_(0) {
  const int null_bitmap_size =
      static_cast<int>((schema_.columns.size() + 7) / 8);
  int offset = 1 + null_bitmap_size;
  for (const auto &column : schema_.columns) {
    column_offsets_.push_back(offset);
    offset += column_payload_size(column);
  }
  row_size_ = offset;
}

Table::~Table() = default;
Table::Table(Table &&) noexcept = default;

void Table::create() {
  std::filesystem::create_directories(table_path_ / "indexes");
  file_ = BinaryFile::create(table_path_ / "rows.bin");

  std::array<std::byte, kRowsHeaderSize> header{};
  std::copy(kRowsMagic.begin(), kRowsMagic.end(), header.begin());
  write_u32_le(std::span<std::byte>(header).subspan(4, 4), kRowsVersion);
  file_.append(header);
  file_.flush();
}

void Table::open() {
  file_ = BinaryFile::open(table_path_ / "rows.bin");
  std::array<std::byte, kRowsHeaderSize> header{};
  file_.read_at(0, header);

  if (!std::equal(kRowsMagic.begin(), kRowsMagic.end(), header.begin())) {
    throw std::runtime_error("invalid rows file magic: " + schema_.name);
  }
  if (read_u32_le(std::span<const std::byte>(header).subspan(4, 4)) !=
      kRowsVersion) {
    throw std::runtime_error("unsupported rows file version: " + schema_.name);
  }

  const auto payload_size = file_.size() - kRowsHeaderSize;
  if (row_size_ <= 0 ||
      payload_size % static_cast<std::uint64_t>(row_size_) != 0) {
    throw std::runtime_error("invalid rows file size: " + schema_.name);
  }
}

void Table::drop() {
  file_.close();
  std::filesystem::remove_all(table_path_);
}

void Table::flush() { file_.flush(); }

void Table::insert(const Row &row) {
  std::vector<std::byte> buffer(static_cast<std::size_t>(row_size_));
  Row copy = row;
  encode_row_(copy, buffer);
  file_.append(buffer);
}

std::optional<Row> Table::read(std::int64_t row_offset) {
  if (row_offset < 0 || row_offset >= size())
    return std::nullopt;

  std::vector<std::byte> buffer(static_cast<std::size_t>(row_size_));
  file_.read_at(kRowsHeaderSize +
                    static_cast<std::uint64_t>(row_offset) * row_size_,
                buffer);
  if (buffer[0] == std::byte{1})
    return std::nullopt;
  return decode_row_(buffer);
}

void Table::delete_row(std::int64_t row_offset) {
  if (row_offset < 0 || row_offset >= size()) {
    throw std::runtime_error("row offset out of range");
  }
  const std::byte tombstone{1};
  file_.write_at(kRowsHeaderSize +
                     static_cast<std::uint64_t>(row_offset) * row_size_,
                 std::span<const std::byte>(&tombstone, 1));
}

std::int64_t Table::size() {
  const auto file_size = file_.size();
  if (file_size < kRowsHeaderSize) {
    throw std::runtime_error("rows file is smaller than header");
  }
  return static_cast<std::int64_t>((file_size - kRowsHeaderSize) /
                                   static_cast<std::uint64_t>(row_size_));
}

const TableSchema &Table::schema() const { return schema_; }

const std::vector<ColumnSchema> &Table::columns() const {
  return schema_.columns;
}

Row Table::decode_row_(std::span<std::byte const> in) {
  Row row;
  row.values.reserve(schema_.columns.size());

  const int null_bitmap_offset = 1;
  for (std::size_t i = 0; i < schema_.columns.size(); i += 1) {
    const auto &column = schema_.columns[i];
    const bool is_null =
        (std::to_integer<unsigned char>(
             in[null_bitmap_offset + static_cast<int>(i / 8)]) &
         (1u << (i % 8))) != 0;
    if (is_null) {
      row.values.push_back(nullptr);
      continue;
    }

    const auto offset = static_cast<std::size_t>(column_offsets_[i]);
    switch (column.type) {
    case ColumnType::Integer:
      row.values.push_back(read_i64_le(in.subspan(offset, 8)));
      break;
    case ColumnType::Boolean:
      row.values.push_back(in[offset] == std::byte{1});
      break;
    case ColumnType::Char: {
      const auto size = required_type_size(column);
      const auto bytes = in.subspan(offset, size);
      std::size_t length = 0;
      while (length < bytes.size() && bytes[length] != std::byte{0}) {
        length += 1;
      }
      row.values.push_back(
          std::string(reinterpret_cast<const char *>(bytes.data()), length));
      break;
    }
    case ColumnType::Binary: {
      const auto size = required_type_size(column);
      const auto length = read_u16_le(in.subspan(offset, 2));
      if (length > size) {
        throw std::runtime_error("invalid binary length in row");
      }
      const auto bytes = in.subspan(offset + 2, length);
      row.values.push_back(BinaryValue(
          reinterpret_cast<const std::uint8_t *>(bytes.data()),
          reinterpret_cast<const std::uint8_t *>(bytes.data() + bytes.size())));
      break;
    }
    }
  }
  return row;
}

void Table::encode_row_(Row &row, std::span<std::byte> out) {
  if (row.values.size() != schema_.columns.size()) {
    throw std::runtime_error("row value count does not match table schema");
  }
  std::fill(out.begin(), out.end(), std::byte{0});

  for (std::size_t i = 0; i < schema_.columns.size(); i += 1) {
    const auto &column = schema_.columns[i];
    const auto &value = row.values[i];
    const bool is_null = std::holds_alternative<std::nullptr_t>(value);
    if (is_null) {
      if (!column.is_nullable) {
        throw std::runtime_error("column is not nullable: " + column.name);
      }
      out[1 + i / 8] = static_cast<std::byte>(
          std::to_integer<unsigned char>(out[1 + i / 8]) | (1u << (i % 8)));
      continue;
    }

    const auto offset = static_cast<std::size_t>(column_offsets_[i]);
    switch (column.type) {
    case ColumnType::Integer:
      if (!std::holds_alternative<std::int64_t>(value)) {
        throw std::runtime_error("expected integer for column: " + column.name);
      }
      write_i64_le(out.subspan(offset, 8), std::get<std::int64_t>(value));
      break;
    case ColumnType::Boolean:
      if (!std::holds_alternative<bool>(value)) {
        throw std::runtime_error("expected boolean for column: " + column.name);
      }
      out[offset] = std::get<bool>(value) ? std::byte{1} : std::byte{0};
      break;
    case ColumnType::Char: {
      if (!std::holds_alternative<std::string>(value)) {
        throw std::runtime_error("expected string for column: " + column.name);
      }
      const auto size = required_type_size(column);
      const auto &text = std::get<std::string>(value);
      if (text.size() > size) {
        throw std::runtime_error("string too long for column: " + column.name);
      }
      std::copy(text.begin(), text.end(),
                reinterpret_cast<char *>(out.subspan(offset, size).data()));
      break;
    }
    case ColumnType::Binary: {
      if (!std::holds_alternative<BinaryValue>(value)) {
        throw std::runtime_error("expected binary for column: " + column.name);
      }
      const auto size = required_type_size(column);
      const auto &bytes = std::get<BinaryValue>(value);
      if (bytes.size() > size ||
          bytes.size() > std::numeric_limits<std::uint16_t>::max()) {
        throw std::runtime_error("binary too long for column: " + column.name);
      }
      write_u16_le(out.subspan(offset, 2),
                   static_cast<std::uint16_t>(bytes.size()));
      std::copy(bytes.begin(), bytes.end(),
                reinterpret_cast<std::uint8_t *>(
                    out.subspan(offset + 2, size).data()));
      break;
    }
    }
  }
}

} // namespace minidb
