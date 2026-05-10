#include "minidb/storage.hpp"

#include <cstdint>
#include <stdexcept>

namespace minidb {

BinaryFile BinaryFile::create(std::filesystem::path path) {
  if (path.has_parent_path()) {
    std::filesystem::create_directories(path.parent_path());
  }

  BinaryFile file;
  file.file_.open(path, std::ios::binary | std::ios::in | std::ios::out |
    std::ios::trunc);
  if (!file.file_) {
    throw std::runtime_error("failed to create binary file: " + path.string());
  }
  return file;
}

BinaryFile BinaryFile::open(std::filesystem::path path) {
  BinaryFile file;
  file.file_.open(path, std::ios::binary | std::ios::in | std::ios::out);
  if (!file.file_) {
    throw std::runtime_error("failed to open binary file: " + path.string());
  }
  return file;
}

void BinaryFile::read_at(std::uint64_t offset, std::span<std::byte> out) {
  file_.clear();
  file_.seekg(static_cast<std::streamoff>(offset), std::ios::beg);
  file_.read(reinterpret_cast<char *>(out.data()),
    static_cast<std::streamsize>(out.size()));
  if (!file_ || file_.gcount() != static_cast<std::streamsize>(out.size())) {
    throw std::runtime_error("failed to read binary file");
  }
}

void BinaryFile::write_at(
  std::uint64_t offset,
  std::span<std::byte const> data
) {
  file_.clear();
  file_.seekp(static_cast<std::streamoff>(offset), std::ios::beg);
  file_.write(reinterpret_cast<const char *>(data.data()),
    static_cast<std::streamsize>(data.size()));
  if (!file_) {
    throw std::runtime_error("failed to write binary file");
  }
}

void BinaryFile::append(std::span<std::byte const> data) {
  file_.clear();
  file_.seekp(0, std::ios::end);
  file_.write(reinterpret_cast<const char *>(data.data()),
    static_cast<std::streamsize>(data.size()));
  if (!file_) {
    throw std::runtime_error("failed to append binary file");
  }
}

std::uint64_t BinaryFile::size() {
  file_.clear();
  file_.seekg(0, std::ios::end);
  const auto position = file_.tellg();
  if (position < 0) {
    throw std::runtime_error("failed to get binary file size");
  }
  return static_cast<std::uint64_t>(position);
}

void BinaryFile::flush() {
  file_.flush();
  if (!file_) {
    throw std::runtime_error("failed to flush binary file");
  }
}

void BinaryFile::close() {
  if (file_.is_open()) {
    file_.close();
  }
}

} // namespace minidb
