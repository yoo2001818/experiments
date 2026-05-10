#include "minidb/util_binary.hpp"

namespace minidb {

void write_u16_le(std::span<std::byte> out, std::uint16_t value) {
  out[0] = static_cast<std::byte>(value & 0xff);
  out[1] = static_cast<std::byte>((value >> 8) & 0xff);
}

std::uint16_t read_u16_le(std::span<const std::byte> in) {
  return static_cast<std::uint16_t>(
      static_cast<std::uint16_t>(std::to_integer<unsigned char>(in[0])) |
      static_cast<std::uint16_t>(std::to_integer<unsigned char>(in[1]) << 8));
}

void write_u32_le(std::span<std::byte> out, std::uint32_t value) {
  for (std::size_t i = 0; i < 4; i += 1) {
    out[i] = static_cast<std::byte>((value >> (i * 8)) & 0xff);
  }
}

std::uint32_t read_u32_le(std::span<const std::byte> in) {
  std::uint32_t value = 0;
  for (std::size_t i = 0; i < 4; i += 1) {
    value |= static_cast<std::uint32_t>(std::to_integer<unsigned char>(in[i]))
             << (i * 8);
  }
  return value;
}

void write_i64_le(std::span<std::byte> out, std::int64_t value) {
  const auto unsigned_value = static_cast<std::uint64_t>(value);
  for (std::size_t i = 0; i < 8; i += 1) {
    out[i] = static_cast<std::byte>((unsigned_value >> (i * 8)) & 0xff);
  }
}

std::int64_t read_i64_le(std::span<const std::byte> in) {
  std::uint64_t value = 0;
  for (std::size_t i = 0; i < 8; i += 1) {
    value |= static_cast<std::uint64_t>(std::to_integer<unsigned char>(in[i]))
             << (i * 8);
  }
  return static_cast<std::int64_t>(value);
}
} // namespace minidb
