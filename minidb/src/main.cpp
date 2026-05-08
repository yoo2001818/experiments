#include <cctype>
#include <iostream>
#include <string>
#include <string_view>

#include "minidb/ddl_parser.hpp"

namespace {

std::string_view trim(std::string_view value) {
  while (!value.empty() &&
    std::isspace(static_cast<unsigned char>(value.front()))) {
    value.remove_prefix(1);
  }
  while (!value.empty() &&
    std::isspace(static_cast<unsigned char>(value.back()))) {
    value.remove_suffix(1);
  }
  return value;
}

bool ends_statement(std::string_view sql) {
  bool in_string = false;
  bool in_identifier = false;

  for (std::size_t i = 0; i < sql.size(); i += 1) {
    const char ch = sql[i];
    const char next = i + 1 < sql.size() ? sql[i + 1] : '\0';

    if (in_string) {
      if (ch == '\'' && next == '\'') {
        i += 1;
      } else if (ch == '\'') {
        in_string = false;
      }
      continue;
    }

    if (in_identifier) {
      if (ch == '`' && next == '`') {
        i += 1;
      } else if (ch == '`') {
        in_identifier = false;
      }
      continue;
    }

    if (ch == '-' && next == '-') {
      while (i < sql.size() && sql[i] != '\n') {
        i += 1;
      }
      continue;
    }

    if (ch == '/' && next == '*') {
      i += 2;
      while (i + 1 < sql.size() && !(sql[i] == '*' && sql[i + 1] == '/')) {
        i += 1;
      }
      i += 1;
      continue;
    }

    if (ch == '\'') {
      in_string = true;
    } else if (ch == '`') {
      in_identifier = true;
    } else if (ch == ';') {
      return true;
    }
  }

  return false;
}

bool run_dot_command(std::string_view command) {
  if (command == ".quit" || command == ".exit") {
    return false;
  }

  if (command == ".help") {
    std::cout
      << "MiniDB commands:\n"
      << "  .help   Show this message\n"
      << "  .quit   Exit the REPL\n"
      << "  .exit   Exit the REPL\n"
      << "\n"
      << "Enter DDL statements terminated by ';'.\n";
    return true;
  }

  std::cout << "unknown command: " << command << '\n';
  return true;
}

void parse_and_report(std::string_view sql) {
  try {
    (void)minidb::parse_ddl_statement(sql);
    std::cout << "parsed ddl statement\n";
  } catch (const minidb::ParseError& error) {
    const auto location = error.location();
    std::cout << "parse error at " << location.line << ':'
              << location.column << ": " << error.what() << '\n';
  }
}

} // namespace

int main() {
  std::cout << "MiniDB\n";
  std::cout << "Enter .help for help.\n";

  std::string buffer;
  std::string line;

  while (true) {
    std::cout << (buffer.empty() ? "minidb> " : "   ...> ");
    if (!std::getline(std::cin, line)) {
      break;
    }

    const std::string_view trimmed = trim(line);
    if (buffer.empty() && trimmed.empty()) {
      continue;
    }

    if (buffer.empty() && !trimmed.empty() && trimmed.front() == '.') {
      if (!run_dot_command(trimmed)) {
        return 0;
      }
      continue;
    }

    buffer += line;
    buffer += '\n';

    if (ends_statement(buffer)) {
      parse_and_report(buffer);
      buffer.clear();
    }
  }

  if (!trim(buffer).empty()) {
    parse_and_report(buffer);
  }

  std::cout << '\n';
  return 0;
}
