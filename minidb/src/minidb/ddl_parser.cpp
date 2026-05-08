#include "minidb/ddl_parser.hpp"

#include <cctype>
#include <charconv>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace minidb {
namespace {

enum class TokenKind {
  Identifier,
  Number,
  String,
  LeftParen,
  RightParen,
  Comma,
  Semicolon,
  End,

  Asc,
  Binary,
  Boolean,
  Char,
  Comment,
  Create,
  Desc,
  Drop,
  Exists,
  If,
  Index,
  Integer,
  Key,
  Not,
  Null,
  On,
  Primary,
  Table,
  Unique,
  Using,
};

struct Token {
  TokenKind kind;
  std::string text;
  SourceLocation location;
};

std::string upper_ascii(std::string_view value) {
  std::string output;
  output.reserve(value.size());
  for (char ch : value) {
    output.push_back(
        static_cast<char>(std::toupper(static_cast<unsigned char>(ch))));
  }
  return output;
}

const char *token_name(TokenKind kind) {
  switch (kind) {
  case TokenKind::Identifier:
    return "identifier";
  case TokenKind::Number:
    return "number";
  case TokenKind::String:
    return "string";
  case TokenKind::LeftParen:
    return "'('";
  case TokenKind::RightParen:
    return "')'";
  case TokenKind::Comma:
    return "','";
  case TokenKind::Semicolon:
    return "';'";
  case TokenKind::End:
    return "end of input";
  case TokenKind::Asc:
    return "ASC";
  case TokenKind::Binary:
    return "BINARY";
  case TokenKind::Boolean:
    return "BOOLEAN";
  case TokenKind::Char:
    return "CHAR";
  case TokenKind::Comment:
    return "COMMENT";
  case TokenKind::Create:
    return "CREATE";
  case TokenKind::Desc:
    return "DESC";
  case TokenKind::Drop:
    return "DROP";
  case TokenKind::Exists:
    return "EXISTS";
  case TokenKind::If:
    return "IF";
  case TokenKind::Index:
    return "INDEX";
  case TokenKind::Integer:
    return "INTEGER";
  case TokenKind::Key:
    return "KEY";
  case TokenKind::Not:
    return "NOT";
  case TokenKind::Null:
    return "NULL";
  case TokenKind::On:
    return "ON";
  case TokenKind::Primary:
    return "PRIMARY";
  case TokenKind::Table:
    return "TABLE";
  case TokenKind::Unique:
    return "UNIQUE";
  case TokenKind::Using:
    return "USING";
  }
  return "token";
}

TokenKind keyword_kind(std::string_view value) {
  const std::string keyword = upper_ascii(value);
  if (keyword == "ASC")
    return TokenKind::Asc;
  if (keyword == "BINARY")
    return TokenKind::Binary;
  if (keyword == "BOOLEAN")
    return TokenKind::Boolean;
  if (keyword == "CHAR")
    return TokenKind::Char;
  if (keyword == "COMMENT")
    return TokenKind::Comment;
  if (keyword == "CREATE")
    return TokenKind::Create;
  if (keyword == "DESC")
    return TokenKind::Desc;
  if (keyword == "DROP")
    return TokenKind::Drop;
  if (keyword == "EXISTS")
    return TokenKind::Exists;
  if (keyword == "IF")
    return TokenKind::If;
  if (keyword == "INDEX")
    return TokenKind::Index;
  if (keyword == "INTEGER")
    return TokenKind::Integer;
  if (keyword == "KEY")
    return TokenKind::Key;
  if (keyword == "NOT")
    return TokenKind::Not;
  if (keyword == "NULL")
    return TokenKind::Null;
  if (keyword == "ON")
    return TokenKind::On;
  if (keyword == "PRIMARY")
    return TokenKind::Primary;
  if (keyword == "TABLE")
    return TokenKind::Table;
  if (keyword == "UNIQUE")
    return TokenKind::Unique;
  if (keyword == "USING")
    return TokenKind::Using;
  return TokenKind::Identifier;
}

class Lexer {
public:
  explicit Lexer(std::string_view input) : input_(input) {}

  std::vector<Token> tokenize() {
    std::vector<Token> tokens;
    while (true) {
      skip_trivia();
      const SourceLocation location = location_;
      if (is_at_end()) {
        tokens.push_back(Token{TokenKind::End, "", location});
        return tokens;
      }

      const char ch = peek();
      if (is_identifier_start(ch)) {
        tokens.push_back(identifier());
      } else if (std::isdigit(static_cast<unsigned char>(ch))) {
        tokens.push_back(number());
      } else if (ch == '\'') {
        tokens.push_back(string());
      } else if (ch == '`') {
        tokens.push_back(quoted_identifier());
      } else {
        advance();
        switch (ch) {
        case '(':
          tokens.push_back(Token{TokenKind::LeftParen, "(", location});
          break;
        case ')':
          tokens.push_back(Token{TokenKind::RightParen, ")", location});
          break;
        case ',':
          tokens.push_back(Token{TokenKind::Comma, ",", location});
          break;
        case ';':
          tokens.push_back(Token{TokenKind::Semicolon, ";", location});
          break;
        default:
          throw ParseError(location, "unexpected character");
        }
      }
    }
  }

private:
  bool is_at_end() const { return offset_ >= input_.size(); }

  char peek(std::size_t lookahead = 0) const {
    const std::size_t index = offset_ + lookahead;
    if (index >= input_.size())
      return '\0';
    return input_[index];
  }

  char advance() {
    const char ch = input_[offset_++];
    if (ch == '\n') {
      location_.line += 1;
      location_.column = 1;
    } else {
      location_.column += 1;
    }
    location_.offset = offset_;
    return ch;
  }

  static bool is_identifier_start(char ch) {
    return std::isalpha(static_cast<unsigned char>(ch)) || ch == '_';
  }

  static bool is_identifier_continue(char ch) {
    return std::isalnum(static_cast<unsigned char>(ch)) || ch == '_';
  }

  void skip_trivia() {
    while (!is_at_end()) {
      if (std::isspace(static_cast<unsigned char>(peek()))) {
        advance();
        continue;
      }
      if (peek() == '-' && peek(1) == '-') {
        while (!is_at_end() && peek() != '\n')
          advance();
        continue;
      }
      if (peek() == '/' && peek(1) == '*') {
        const SourceLocation start = location_;
        advance();
        advance();
        while (!is_at_end() && !(peek() == '*' && peek(1) == '/')) {
          advance();
        }
        if (is_at_end()) {
          throw ParseError(start, "unterminated block comment");
        }
        advance();
        advance();
        continue;
      }
      break;
    }
  }

  Token identifier() {
    const SourceLocation location = location_;
    const std::size_t start = offset_;
    while (!is_at_end() && is_identifier_continue(peek())) {
      advance();
    }
    const std::string text(input_.substr(start, offset_ - start));
    return Token{keyword_kind(text), text, location};
  }

  Token quoted_identifier() {
    const SourceLocation location = location_;
    advance();
    std::string text;
    while (!is_at_end()) {
      const char ch = advance();
      if (ch == '`') {
        if (peek() == '`') {
          text.push_back(advance());
          continue;
        }
        return Token{TokenKind::Identifier, std::move(text), location};
      }
      text.push_back(ch);
    }
    throw ParseError(location, "unterminated quoted identifier");
  }

  Token number() {
    const SourceLocation location = location_;
    const std::size_t start = offset_;
    while (!is_at_end() && std::isdigit(static_cast<unsigned char>(peek()))) {
      advance();
    }
    return Token{
        TokenKind::Number,
        std::string(input_.substr(start, offset_ - start)),
        location,
    };
  }

  Token string() {
    const SourceLocation location = location_;
    advance();
    std::string text;
    while (!is_at_end()) {
      const char ch = advance();
      if (ch == '\'') {
        if (peek() == '\'') {
          text.push_back(advance());
          continue;
        }
        return Token{TokenKind::String, std::move(text), location};
      }
      text.push_back(ch);
    }
    throw ParseError(location, "unterminated string literal");
  }

  std::string_view input_;
  std::size_t offset_ = 0;
  SourceLocation location_;
};

class Parser {
public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  DdlStatement parse() {
    DdlStatement statement = parse_statement();
    match(TokenKind::Semicolon);
    expect(TokenKind::End);
    return statement;
  }

private:
  const Token &current() const { return tokens_[position_]; }

  const Token &previous() const { return tokens_[position_ - 1]; }

  bool check(TokenKind kind) const { return current().kind == kind; }

  bool match(TokenKind kind) {
    if (!check(kind))
      return false;
    position_ += 1;
    return true;
  }

  const Token &expect(TokenKind kind) {
    if (!match(kind)) {
      throw error("expected " + std::string(token_name(kind)));
    }
    return previous();
  }

  ParseError error(std::string message) const {
    return ParseError(current().location, std::move(message));
  }

  DdlStatement parse_statement() {
    if (match(TokenKind::Create)) {
      if (match(TokenKind::Table))
        return parse_create_table();
      if (match(TokenKind::Unique)) {
        expect(TokenKind::Index);
        return parse_create_index(true);
      }
      if (match(TokenKind::Index))
        return parse_create_index(false);
      throw error("expected TABLE, INDEX, or UNIQUE INDEX after CREATE");
    }

    if (match(TokenKind::Drop)) {
      if (match(TokenKind::Table))
        return parse_drop_table();
      if (match(TokenKind::Index))
        return parse_drop_index();
      throw error("expected TABLE or INDEX after DROP");
    }

    throw error("expected DDL statement");
  }

  CreateTableStmt parse_create_table() {
    bool if_not_exists = false;
    if (match(TokenKind::If)) {
      expect(TokenKind::Not);
      expect(TokenKind::Exists);
      if_not_exists = true;
    }

    CreateTableStmt stmt{
        .if_not_exists = if_not_exists,
        .table_name = expect_identifier(),
    };

    expect(TokenKind::LeftParen);
    do {
      stmt.definitions.push_back(parse_create_definition());
    } while (match(TokenKind::Comma));
    expect(TokenKind::RightParen);

    return stmt;
  }

  CreateDefinition parse_create_definition() {
    if (match(TokenKind::Primary)) {
      expect(TokenKind::Key);
      return parse_index_definition(IndexConstraint::PrimaryKey);
    }

    if (match(TokenKind::Unique)) {
      if (match(TokenKind::Index) || match(TokenKind::Key)) {
        return parse_index_definition(IndexConstraint::Unique);
      }
      throw error("expected INDEX or KEY after UNIQUE");
    }

    if (match(TokenKind::Index) || match(TokenKind::Key)) {
      return parse_index_definition(IndexConstraint::Regular);
    }

    ColumnDefinition column{
        .column_name = expect_identifier(),
        .data_type = parse_data_type(),
    };
    parse_column_options(column);
    return column;
  }

  void parse_column_options(ColumnDefinition &column) {
    while (true) {
      if (match(TokenKind::Not)) {
        expect(TokenKind::Null);
        column.nullability = Nullability::NotNull;
      } else if (match(TokenKind::Null)) {
        column.nullability = Nullability::Null;
      } else if (match(TokenKind::Unique)) {
        column.unique = true;
        match(TokenKind::Key);
      } else if (match(TokenKind::Primary)) {
        expect(TokenKind::Key);
        column.primary_key = true;
      } else if (match(TokenKind::Key)) {
        column.primary_key = true;
      } else if (match(TokenKind::Comment)) {
        column.comment = expect(TokenKind::String).text;
      } else {
        return;
      }
    }
  }

  DataType parse_data_type() {
    if (match(TokenKind::Integer))
      return IntegerType{};
    if (match(TokenKind::Boolean))
      return BooleanType{};
    if (match(TokenKind::Char)) {
      return CharType{.length = parse_length()};
    }
    if (match(TokenKind::Binary)) {
      return BinaryType{.length = parse_length()};
    }
    throw error("expected data type");
  }

  std::uint32_t parse_length() {
    expect(TokenKind::LeftParen);
    const Token &token = expect(TokenKind::Number);
    expect(TokenKind::RightParen);

    std::uint64_t value = 0;
    const char *begin = token.text.data();
    const char *end = token.text.data() + token.text.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end || value == 0 ||
        value > std::numeric_limits<std::uint32_t>::max()) {
      throw ParseError(token.location, "invalid type length");
    }
    return static_cast<std::uint32_t>(value);
  }

  IndexDefinition parse_index_definition(IndexConstraint constraint) {
    IndexDefinition index{.constraint = constraint};
    if (check(TokenKind::Identifier)) {
      index.index_name = expect_identifier();
    }
    if (match(TokenKind::Using)) {
      index.index_type = expect_identifier();
    }
    index.key_parts = parse_key_part_list();
    parse_index_options(index.comment);
    return index;
  }

  CreateIndexStmt parse_create_index(bool unique) {
    CreateIndexStmt stmt{
        .unique = unique,
        .index_name = expect_identifier(),
    };
    expect(TokenKind::On);
    stmt.table_name = expect_identifier();
    stmt.key_parts = parse_key_part_list();
    parse_index_options(stmt.comment);
    return stmt;
  }

  DropTableStmt parse_drop_table() {
    DropTableStmt stmt;
    if (match(TokenKind::If)) {
      expect(TokenKind::Exists);
      stmt.if_exists = true;
    }

    do {
      stmt.table_names.push_back(expect_identifier());
    } while (match(TokenKind::Comma));

    return stmt;
  }

  DropIndexStmt parse_drop_index() {
    DropIndexStmt stmt{
        .index_name = expect_identifier(),
    };
    expect(TokenKind::On);
    stmt.table_name = expect_identifier();
    return stmt;
  }

  std::vector<KeyPart> parse_key_part_list() {
    std::vector<KeyPart> key_parts;
    expect(TokenKind::LeftParen);
    do {
      key_parts.push_back(parse_key_part());
    } while (match(TokenKind::Comma));
    expect(TokenKind::RightParen);
    return key_parts;
  }

  KeyPart parse_key_part() {
    KeyPart key_part{.column_name = expect_identifier()};
    if (match(TokenKind::Asc)) {
      key_part.direction = SortDirection::Asc;
    } else if (match(TokenKind::Desc)) {
      key_part.direction = SortDirection::Desc;
    }
    return key_part;
  }

  void parse_index_options(std::optional<std::string> &comment) {
    while (match(TokenKind::Comment)) {
      comment = expect(TokenKind::String).text;
    }
  }

  std::string expect_identifier() { return expect(TokenKind::Identifier).text; }

  std::vector<Token> tokens_;
  std::size_t position_ = 0;
};

} // namespace

ParseError::ParseError(SourceLocation location, std::string message)
    : std::runtime_error(std::move(message)), location_(location) {}

SourceLocation ParseError::location() const { return location_; }

DdlStatement parse_ddl_statement(std::string_view sql) {
  Lexer lexer(sql);
  Parser parser(lexer.tokenize());
  return parser.parse();
}

} // namespace minidb
