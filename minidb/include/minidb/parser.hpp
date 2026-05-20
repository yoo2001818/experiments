#pragma once

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <string_view>

#include "minidb/ast.hpp"

namespace minidb {

struct SourceLocation {
  std::size_t offset = 0;
  std::uint32_t line = 1;
  std::uint32_t column = 1;
};

class ParseError : public std::runtime_error {
public:
  ParseError(SourceLocation location, std::string message);

  SourceLocation location() const;

private:
  SourceLocation location_;
};

Statement parse_statement(std::string_view sql);
DdlStatement parse_ddl_statement(std::string_view sql);
DmlStatement parse_dml_statement(std::string_view sql);

} // namespace minidb
