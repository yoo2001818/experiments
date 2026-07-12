#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace minidb {

using BinaryValue = std::vector<std::uint8_t>;
using Value =
    std::variant<std::nullptr_t, std::int64_t, bool, std::string, BinaryValue>;

} // namespace minidb
