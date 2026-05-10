#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace minidb {

void write_u16_le(std::span<std::byte> out, std::uint16_t value);
std::uint16_t read_u16_le(std::span<const std::byte> in);
void write_u32_le(std::span<std::byte> out, std::uint32_t value);
std::uint32_t read_u32_le(std::span<const std::byte> in);
void write_i64_le(std::span<std::byte> out, std::int64_t value);
std::int64_t read_i64_le(std::span<const std::byte> in);

} // namespace minidb
