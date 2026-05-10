#pragma once

#include <filesystem>
#include <fstream>
#include <span>

namespace minidb {

class BinaryFile {
public:
  static BinaryFile create(std::filesystem::path path);
  static BinaryFile open(std::filesystem::path path);

  void read_at(std::uint64_t offset, std::span<std::byte> out);
  void write_at(std::uint64_t offset, std::span<std::byte const> data);
  void append(std::span<std::byte const> data);
  std::uint64_t size();
  void flush();

private:
  std::fstream file_;
};

} // namespace minidb
