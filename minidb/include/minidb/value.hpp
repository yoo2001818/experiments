#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace minidb {

struct NullValue {
  bool operator==(const NullValue &) const = default;
};

struct IntegerValue {
  std::int64_t value;

  bool operator==(const IntegerValue &) const = default;
};

struct BooleanValue {
  bool value;

  bool operator==(const BooleanValue &) const = default;
};

struct StringValue {
  std::string value;

  bool operator==(const StringValue &) const = default;
};

struct BinaryValue {
  std::vector<std::uint8_t> value;

  bool operator==(const BinaryValue &) const = default;
};

using Value = std::variant<NullValue, IntegerValue, BooleanValue, StringValue,
                           BinaryValue>;

} // namespace minidb
