#include "minidb/parser.hpp"

#include <cctype>
#include <charconv>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <variant>
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
  Dot,
  Star,
  Slash,
  Percent,
  Plus,
  Minus,
  Tilde,
  Amp,
  Pipe,
  PipePipe,
  ShiftLeft,
  ShiftRight,
  Less,
  Greater,
  LessEqual,
  GreaterEqual,
  Equal,
  EqualEqual,
  NotEqual,
  BangEqual,
  EndInput,

  And,
  As,
  Asc,
  Between,
  Binary,
  Boolean,
  By,
  Case,
  Char,
  Comment,
  Create,
  Default,
  Delete,
  Desc,
  Distinct,
  Drop,
  Else,
  End,
  Exists,
  False,
  From,
  If,
  In,
  Index,
  Insert,
  Integer,
  Into,
  Is,
  Key,
  Like,
  Limit,
  Not,
  Null,
  Offset,
  On,
  Or,
  Order,
  Primary,
  Row,
  Select,
  Set,
  Table,
  Then,
  True,
  Unique,
  Update,
  Using,
  Value,
  Values,
  When,
  Where,
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
  case TokenKind::Dot:
    return "'.'";
  case TokenKind::Star:
    return "'*'";
  case TokenKind::Slash:
    return "'/'";
  case TokenKind::Percent:
    return "'%'";
  case TokenKind::Plus:
    return "'+'";
  case TokenKind::Minus:
    return "'-'";
  case TokenKind::Tilde:
    return "'~'";
  case TokenKind::Amp:
    return "'&'";
  case TokenKind::Pipe:
    return "'|'";
  case TokenKind::PipePipe:
    return "'||'";
  case TokenKind::ShiftLeft:
    return "'<<'";
  case TokenKind::ShiftRight:
    return "'>>'";
  case TokenKind::Less:
    return "'<'";
  case TokenKind::Greater:
    return "'>'";
  case TokenKind::LessEqual:
    return "'<='";
  case TokenKind::GreaterEqual:
    return "'>='";
  case TokenKind::Equal:
    return "'='";
  case TokenKind::EqualEqual:
    return "'=='";
  case TokenKind::NotEqual:
    return "'<>'";
  case TokenKind::BangEqual:
    return "'!='";
  case TokenKind::EndInput:
    return "end of input";
  case TokenKind::And:
    return "AND";
  case TokenKind::As:
    return "AS";
  case TokenKind::Asc:
    return "ASC";
  case TokenKind::Between:
    return "BETWEEN";
  case TokenKind::Binary:
    return "BINARY";
  case TokenKind::Boolean:
    return "BOOLEAN";
  case TokenKind::By:
    return "BY";
  case TokenKind::Case:
    return "CASE";
  case TokenKind::Char:
    return "CHAR";
  case TokenKind::Comment:
    return "COMMENT";
  case TokenKind::Create:
    return "CREATE";
  case TokenKind::Default:
    return "DEFAULT";
  case TokenKind::Delete:
    return "DELETE";
  case TokenKind::Desc:
    return "DESC";
  case TokenKind::Distinct:
    return "DISTINCT";
  case TokenKind::Drop:
    return "DROP";
  case TokenKind::Else:
    return "ELSE";
  case TokenKind::End:
    return "END";
  case TokenKind::Exists:
    return "EXISTS";
  case TokenKind::False:
    return "FALSE";
  case TokenKind::From:
    return "FROM";
  case TokenKind::If:
    return "IF";
  case TokenKind::In:
    return "IN";
  case TokenKind::Index:
    return "INDEX";
  case TokenKind::Insert:
    return "INSERT";
  case TokenKind::Integer:
    return "INTEGER";
  case TokenKind::Into:
    return "INTO";
  case TokenKind::Is:
    return "IS";
  case TokenKind::Key:
    return "KEY";
  case TokenKind::Like:
    return "LIKE";
  case TokenKind::Limit:
    return "LIMIT";
  case TokenKind::Not:
    return "NOT";
  case TokenKind::Null:
    return "NULL";
  case TokenKind::Offset:
    return "OFFSET";
  case TokenKind::On:
    return "ON";
  case TokenKind::Or:
    return "OR";
  case TokenKind::Order:
    return "ORDER";
  case TokenKind::Primary:
    return "PRIMARY";
  case TokenKind::Row:
    return "ROW";
  case TokenKind::Select:
    return "SELECT";
  case TokenKind::Set:
    return "SET";
  case TokenKind::Table:
    return "TABLE";
  case TokenKind::Then:
    return "THEN";
  case TokenKind::True:
    return "TRUE";
  case TokenKind::Unique:
    return "UNIQUE";
  case TokenKind::Update:
    return "UPDATE";
  case TokenKind::Using:
    return "USING";
  case TokenKind::Value:
    return "VALUE";
  case TokenKind::Values:
    return "VALUES";
  case TokenKind::When:
    return "WHEN";
  case TokenKind::Where:
    return "WHERE";
  }
  return "token";
}

TokenKind keyword_kind(std::string_view value) {
  const std::string keyword = upper_ascii(value);
  if (keyword == "AND")
    return TokenKind::And;
  if (keyword == "AS")
    return TokenKind::As;
  if (keyword == "ASC")
    return TokenKind::Asc;
  if (keyword == "BETWEEN")
    return TokenKind::Between;
  if (keyword == "BINARY")
    return TokenKind::Binary;
  if (keyword == "BOOLEAN")
    return TokenKind::Boolean;
  if (keyword == "BY")
    return TokenKind::By;
  if (keyword == "CASE")
    return TokenKind::Case;
  if (keyword == "CHAR")
    return TokenKind::Char;
  if (keyword == "COMMENT")
    return TokenKind::Comment;
  if (keyword == "CREATE")
    return TokenKind::Create;
  if (keyword == "DEFAULT")
    return TokenKind::Default;
  if (keyword == "DELETE")
    return TokenKind::Delete;
  if (keyword == "DESC")
    return TokenKind::Desc;
  if (keyword == "DISTINCT")
    return TokenKind::Distinct;
  if (keyword == "DROP")
    return TokenKind::Drop;
  if (keyword == "ELSE")
    return TokenKind::Else;
  if (keyword == "END")
    return TokenKind::End;
  if (keyword == "EXISTS")
    return TokenKind::Exists;
  if (keyword == "FALSE")
    return TokenKind::False;
  if (keyword == "FROM")
    return TokenKind::From;
  if (keyword == "IF")
    return TokenKind::If;
  if (keyword == "IN")
    return TokenKind::In;
  if (keyword == "INDEX")
    return TokenKind::Index;
  if (keyword == "INSERT")
    return TokenKind::Insert;
  if (keyword == "INTEGER")
    return TokenKind::Integer;
  if (keyword == "INTO")
    return TokenKind::Into;
  if (keyword == "IS")
    return TokenKind::Is;
  if (keyword == "KEY")
    return TokenKind::Key;
  if (keyword == "LIKE")
    return TokenKind::Like;
  if (keyword == "LIMIT")
    return TokenKind::Limit;
  if (keyword == "NOT")
    return TokenKind::Not;
  if (keyword == "NULL")
    return TokenKind::Null;
  if (keyword == "OFFSET")
    return TokenKind::Offset;
  if (keyword == "ON")
    return TokenKind::On;
  if (keyword == "OR")
    return TokenKind::Or;
  if (keyword == "ORDER")
    return TokenKind::Order;
  if (keyword == "PRIMARY")
    return TokenKind::Primary;
  if (keyword == "ROW")
    return TokenKind::Row;
  if (keyword == "SELECT")
    return TokenKind::Select;
  if (keyword == "SET")
    return TokenKind::Set;
  if (keyword == "TABLE")
    return TokenKind::Table;
  if (keyword == "THEN")
    return TokenKind::Then;
  if (keyword == "TRUE")
    return TokenKind::True;
  if (keyword == "UNIQUE")
    return TokenKind::Unique;
  if (keyword == "UPDATE")
    return TokenKind::Update;
  if (keyword == "USING")
    return TokenKind::Using;
  if (keyword == "VALUE")
    return TokenKind::Value;
  if (keyword == "VALUES")
    return TokenKind::Values;
  if (keyword == "WHEN")
    return TokenKind::When;
  if (keyword == "WHERE")
    return TokenKind::Where;
  return TokenKind::Identifier;
}

bool is_integer_literal(std::string_view text) {
  if (text.empty())
    return false;
  for (char ch : text) {
    if (!std::isdigit(static_cast<unsigned char>(ch)))
      return false;
  }
  return true;
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
        tokens.push_back(Token{TokenKind::EndInput, "", location});
        return tokens;
      }

      const char ch = peek();
      if (is_identifier_start(ch)) {
        tokens.push_back(identifier());
      } else if (std::isdigit(static_cast<unsigned char>(ch)) ||
                 (ch == '.' &&
                  std::isdigit(static_cast<unsigned char>(peek(1))))) {
        tokens.push_back(number());
      } else if (ch == '\'') {
        tokens.push_back(string());
      } else if (ch == '`') {
        tokens.push_back(quoted_identifier());
      } else {
        tokens.push_back(symbol());
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
    if (peek() == '.' && std::isdigit(static_cast<unsigned char>(peek(1)))) {
      advance();
      while (!is_at_end() &&
             std::isdigit(static_cast<unsigned char>(peek()))) {
        advance();
      }
    }
    if (peek() == 'e' || peek() == 'E') {
      advance();
      if (peek() == '+' || peek() == '-') {
        advance();
      }
      if (!std::isdigit(static_cast<unsigned char>(peek()))) {
        throw ParseError(location, "invalid numeric literal");
      }
      while (!is_at_end() &&
             std::isdigit(static_cast<unsigned char>(peek()))) {
        advance();
      }
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

  Token symbol() {
    const SourceLocation location = location_;
    const char ch = advance();
    switch (ch) {
    case '(':
      return Token{TokenKind::LeftParen, "(", location};
    case ')':
      return Token{TokenKind::RightParen, ")", location};
    case ',':
      return Token{TokenKind::Comma, ",", location};
    case ';':
      return Token{TokenKind::Semicolon, ";", location};
    case '.':
      return Token{TokenKind::Dot, ".", location};
    case '*':
      return Token{TokenKind::Star, "*", location};
    case '/':
      return Token{TokenKind::Slash, "/", location};
    case '%':
      return Token{TokenKind::Percent, "%", location};
    case '+':
      return Token{TokenKind::Plus, "+", location};
    case '-':
      return Token{TokenKind::Minus, "-", location};
    case '~':
      return Token{TokenKind::Tilde, "~", location};
    case '&':
      return Token{TokenKind::Amp, "&", location};
    case '|':
      if (peek() == '|') {
        advance();
        return Token{TokenKind::PipePipe, "||", location};
      }
      return Token{TokenKind::Pipe, "|", location};
    case '<':
      if (peek() == '=') {
        advance();
        return Token{TokenKind::LessEqual, "<=", location};
      }
      if (peek() == '>') {
        advance();
        return Token{TokenKind::NotEqual, "<>", location};
      }
      if (peek() == '<') {
        advance();
        return Token{TokenKind::ShiftLeft, "<<", location};
      }
      return Token{TokenKind::Less, "<", location};
    case '>':
      if (peek() == '=') {
        advance();
        return Token{TokenKind::GreaterEqual, ">=", location};
      }
      if (peek() == '>') {
        advance();
        return Token{TokenKind::ShiftRight, ">>", location};
      }
      return Token{TokenKind::Greater, ">", location};
    case '=':
      if (peek() == '=') {
        advance();
        return Token{TokenKind::EqualEqual, "==", location};
      }
      return Token{TokenKind::Equal, "=", location};
    case '!':
      if (peek() == '=') {
        advance();
        return Token{TokenKind::BangEqual, "!=", location};
      }
      break;
    }
    throw ParseError(location, "unexpected character");
  }

  std::string_view input_;
  std::size_t offset_ = 0;
  SourceLocation location_;
};

class Parser {
public:
  explicit Parser(std::vector<Token> tokens) : tokens_(std::move(tokens)) {}

  Statement parse() {
    Statement statement = parse_statement();
    match(TokenKind::Semicolon);
    expect(TokenKind::EndInput);
    return statement;
  }

private:
  const Token &current() const { return tokens_[position_]; }

  const Token &previous() const { return tokens_[position_ - 1]; }

  const Token &peek(std::size_t lookahead = 1) const {
    const std::size_t index = position_ + lookahead;
    if (index >= tokens_.size())
      return tokens_.back();
    return tokens_[index];
  }

  bool check(TokenKind kind) const { return current().kind == kind; }

  bool check_next(TokenKind kind) const { return peek().kind == kind; }

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

  Statement parse_statement() {
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

    if (check(TokenKind::Select))
      return parse_select();
    if (check(TokenKind::Insert))
      return parse_insert();
    if (check(TokenKind::Update))
      return parse_update();
    if (check(TokenKind::Delete))
      return parse_delete();

    throw error("expected SQL statement");
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
        .table_name = expect_identifier_string(),
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
        .column_name = expect_identifier_string(),
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
      index.index_name = expect_identifier_string();
    }
    if (match(TokenKind::Using)) {
      index.index_type = expect_identifier_string();
    }
    index.key_parts = parse_key_part_list();
    parse_index_options(index.comment);
    return index;
  }

  CreateIndexStmt parse_create_index(bool unique) {
    CreateIndexStmt stmt{
        .unique = unique,
        .index_name = expect_identifier_string(),
    };
    expect(TokenKind::On);
    stmt.table_name = expect_identifier_string();
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
      stmt.table_names.push_back(expect_identifier_string());
    } while (match(TokenKind::Comma));

    return stmt;
  }

  DropIndexStmt parse_drop_index() {
    DropIndexStmt stmt{
        .index_name = expect_identifier_string(),
    };
    expect(TokenKind::On);
    stmt.table_name = expect_identifier_string();
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
    KeyPart key_part{.column_name = expect_identifier_string()};
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

  SelectStmt parse_select() {
    expect(TokenKind::Select);
    SelectStmt stmt;
    stmt.distinct = match(TokenKind::Distinct);

    do {
      stmt.select_list.push_back(parse_select_item());
    } while (match(TokenKind::Comma));

    if (match(TokenKind::From)) {
      do {
        stmt.from.push_back(parse_table_reference());
      } while (match(TokenKind::Comma));
    }

    if (match(TokenKind::Where)) {
      stmt.where = parse_expr();
    }

    parse_order_by_and_limit(stmt.order_by, stmt.limit);
    return stmt;
  }

  SelectItem parse_select_item() {
    if (match(TokenKind::Star)) {
      return WildcardSelectItem{};
    }

    if (check(TokenKind::Identifier)) {
      const std::size_t start = position_;
      Identifier qualifier = parse_identifier();
      if (match(TokenKind::Dot)) {
        if (match(TokenKind::Star)) {
          return WildcardSelectItem{.qualifier = qualifier};
        }
        position_ = start;
      } else {
        position_ = start;
      }
    }

    ExprSelectItem item{.expr = parse_expr()};
    if (match(TokenKind::As)) {
      item.alias = expect_identifier_string();
    } else if (check(TokenKind::Identifier)) {
      item.alias = expect_identifier_string();
    }
    return item;
  }

  TableReference parse_table_reference() {
    TableReference reference{.table_name = parse_identifier()};
    if (match(TokenKind::As)) {
      reference.alias = expect_identifier_string();
    } else if (check(TokenKind::Identifier)) {
      reference.alias = expect_identifier_string();
    }
    return reference;
  }

  InsertStmt parse_insert() {
    expect(TokenKind::Insert);
    match(TokenKind::Into);

    InsertStmt stmt{.table_name = parse_identifier()};
    if (match(TokenKind::LeftParen)) {
      do {
        stmt.column_names.push_back(parse_identifier());
      } while (match(TokenKind::Comma));
      expect(TokenKind::RightParen);
    }

    if (match(TokenKind::Set)) {
      stmt.source = parse_insert_set_source();
    } else if (match(TokenKind::Value) || match(TokenKind::Values)) {
      stmt.source = parse_insert_values_source();
    } else if (check(TokenKind::Select)) {
      stmt.source =
          InsertSelectSource{.select = std::make_shared<SelectStmt>(
                                 parse_select())};
    } else if (match(TokenKind::Table)) {
      stmt.source = InsertTableSource{.table_name = parse_identifier()};
    } else {
      throw error("expected INSERT source");
    }
    return stmt;
  }

  InsertSetSource parse_insert_set_source() {
    InsertSetSource source;
    do {
      InsertAssignment assignment{.column_name = parse_identifier()};
      expect(TokenKind::Equal);
      assignment.value = parse_insert_value();
      source.assignments.push_back(std::move(assignment));
    } while (match(TokenKind::Comma));
    return source;
  }

  InsertValuesSource parse_insert_values_source() {
    InsertValuesSource source;
    do {
      match(TokenKind::Row);
      source.rows.push_back(parse_insert_value_row());
    } while (match(TokenKind::Comma));
    return source;
  }

  std::vector<InsertValue> parse_insert_value_row() {
    std::vector<InsertValue> row;
    expect(TokenKind::LeftParen);
    if (!check(TokenKind::RightParen)) {
      do {
        row.push_back(parse_insert_value());
      } while (match(TokenKind::Comma));
    }
    expect(TokenKind::RightParen);
    return row;
  }

  InsertValue parse_insert_value() {
    if (match(TokenKind::Default)) {
      return DefaultValue{};
    }
    return parse_expr();
  }

  UpdateStmt parse_update() {
    expect(TokenKind::Update);
    UpdateStmt stmt{.table = parse_table_reference()};
    expect(TokenKind::Set);
    do {
      UpdateAssignment assignment{.column_name = parse_identifier()};
      expect(TokenKind::Equal);
      if (match(TokenKind::Default)) {
        assignment.value = DefaultValue{};
      } else {
        assignment.value = parse_expr();
      }
      stmt.assignments.push_back(std::move(assignment));
    } while (match(TokenKind::Comma));

    if (match(TokenKind::Where)) {
      stmt.where = parse_expr();
    }
    parse_order_by_and_limit(stmt.order_by, stmt.limit);
    return stmt;
  }

  DeleteStmt parse_delete() {
    expect(TokenKind::Delete);
    expect(TokenKind::From);
    DeleteStmt stmt{.table_name = parse_identifier()};
    if (match(TokenKind::Where)) {
      stmt.where = parse_expr();
    }
    parse_order_by_and_limit(stmt.order_by, stmt.limit);
    return stmt;
  }

  void parse_order_by_and_limit(std::vector<OrderByTerm> &order_by,
                                std::optional<LimitClause> &limit) {
    if (match(TokenKind::Order)) {
      expect(TokenKind::By);
      do {
        order_by.push_back(parse_order_by_term());
      } while (match(TokenKind::Comma));
    }

    if (match(TokenKind::Limit)) {
      ExprPtr first = parse_expr();
      if (match(TokenKind::Comma)) {
        limit = LimitClause{.row_count = parse_expr(), .offset = first};
      } else {
        LimitClause clause{.row_count = first};
        if (match(TokenKind::Offset)) {
          clause.offset = parse_expr();
        }
        limit = std::move(clause);
      }
    }
  }

  OrderByTerm parse_order_by_term() {
    OrderByTerm term;
    if (check(TokenKind::Number) && is_integer_literal(current().text) &&
        (check_next(TokenKind::Comma) || check_next(TokenKind::Asc) ||
         check_next(TokenKind::Desc) || check_next(TokenKind::Limit) ||
         check_next(TokenKind::Semicolon) || check_next(TokenKind::EndInput))) {
      term.key = parse_position();
    } else {
      term.key = parse_expr();
    }

    if (match(TokenKind::Asc)) {
      term.direction = SortDirection::Asc;
    } else if (match(TokenKind::Desc)) {
      term.direction = SortDirection::Desc;
    }
    return term;
  }

  std::uint32_t parse_position() {
    const Token &token = expect(TokenKind::Number);
    std::uint64_t value = 0;
    const char *begin = token.text.data();
    const char *end = token.text.data() + token.text.size();
    const auto result = std::from_chars(begin, end, value);
    if (result.ec != std::errc{} || result.ptr != end || value == 0 ||
        value > std::numeric_limits<std::uint32_t>::max()) {
      throw ParseError(token.location, "invalid ORDER BY position");
    }
    return static_cast<std::uint32_t>(value);
  }

  ExprPtr parse_expr(int min_precedence = 1) {
    ExprPtr left = parse_prefix_expr();

    while (true) {
      if (match(TokenKind::Is)) {
        constexpr int precedence = 4;
        if (precedence < min_precedence) {
          position_ -= 1;
          break;
        }
        const bool negated = match(TokenKind::Not);
        left = make_expr(IsExpr{.value = left,
                                .negated = negated,
                                .test = parse_expr(precedence + 1)});
        continue;
      }

      if (check(TokenKind::Between) ||
          (check(TokenKind::Not) && check_next(TokenKind::Between))) {
        constexpr int precedence = 4;
        if (precedence < min_precedence)
          break;
        const bool negated = match(TokenKind::Not);
        expect(TokenKind::Between);
        ExprPtr lower = parse_expr(precedence + 1);
        expect(TokenKind::And);
        ExprPtr upper = parse_expr(precedence + 1);
        left = make_expr(BetweenExpr{.value = left,
                                     .negated = negated,
                                     .lower = lower,
                                     .upper = upper});
        continue;
      }

      if (check(TokenKind::In) ||
          (check(TokenKind::Not) && check_next(TokenKind::In))) {
        constexpr int precedence = 4;
        if (precedence < min_precedence)
          break;
        const bool negated = match(TokenKind::Not);
        expect(TokenKind::In);
        expect(TokenKind::LeftParen);
        std::vector<ExprPtr> values;
        if (!check(TokenKind::RightParen)) {
          do {
            values.push_back(parse_expr());
          } while (match(TokenKind::Comma));
        }
        expect(TokenKind::RightParen);
        left = make_expr(
            InExpr{.value = left, .negated = negated, .values = values});
        continue;
      }

      if (check(TokenKind::Like) ||
          (check(TokenKind::Not) && check_next(TokenKind::Like))) {
        constexpr int precedence = 4;
        if (precedence < min_precedence)
          break;
        const bool negated = match(TokenKind::Not);
        expect(TokenKind::Like);
        left = make_expr(LikeExpr{.value = left,
                                  .negated = negated,
                                  .pattern = parse_expr(precedence + 1)});
        continue;
      }

      const auto binary = binary_operator(current().kind);
      if (!binary.has_value())
        break;
      if (binary->precedence < min_precedence)
        break;

      position_ += 1;
      ExprPtr right = parse_expr(binary->precedence + 1);
      left = make_expr(
          BinaryExpr{.left = left, .op = binary->op, .right = right});
    }

    return left;
  }

  ExprPtr parse_prefix_expr() {
    if (match(TokenKind::Not)) {
      return make_expr(
          UnaryExpr{.op = UnaryOperator::Not, .operand = parse_expr(3)});
    }
    if (match(TokenKind::Plus)) {
      return make_expr(
          UnaryExpr{.op = UnaryOperator::Plus, .operand = parse_expr(9)});
    }
    if (match(TokenKind::Minus)) {
      return make_expr(
          UnaryExpr{.op = UnaryOperator::Minus, .operand = parse_expr(9)});
    }
    if (match(TokenKind::Tilde)) {
      return make_expr(
          UnaryExpr{.op = UnaryOperator::BitwiseNot, .operand = parse_expr(9)});
    }
    return parse_primary_expr();
  }

  ExprPtr parse_primary_expr() {
    if (match(TokenKind::Number)) {
      return make_expr(LiteralExpr{
          .value = NumericLiteral{.text = previous().text},
      });
    }
    if (match(TokenKind::String)) {
      return make_expr(LiteralExpr{
          .value = StringLiteral{.value = previous().text},
      });
    }
    if (match(TokenKind::Null)) {
      return make_expr(LiteralExpr{.value = NullLiteral{}});
    }
    if (match(TokenKind::True)) {
      return make_expr(LiteralExpr{.value = BooleanLiteral{.value = true}});
    }
    if (match(TokenKind::False)) {
      return make_expr(LiteralExpr{.value = BooleanLiteral{.value = false}});
    }
    if (match(TokenKind::LeftParen)) {
      ExprPtr expr = parse_expr();
      expect(TokenKind::RightParen);
      return expr;
    }
    if (check(TokenKind::Case)) {
      return parse_case_expr();
    }
    if (check(TokenKind::Identifier)) {
      Identifier identifier = parse_identifier();
      if (match(TokenKind::LeftParen)) {
        std::vector<ExprPtr> arguments;
        if (!check(TokenKind::RightParen)) {
          do {
            arguments.push_back(parse_expr());
          } while (match(TokenKind::Comma));
        }
        expect(TokenKind::RightParen);
        return make_expr(FunctionCallExpr{.function_name = identifier,
                                          .arguments = arguments});
      }
      return make_expr(IdentifierExpr{.name = identifier});
    }

    throw error("expected expression");
  }

  ExprPtr parse_case_expr() {
    expect(TokenKind::Case);
    CaseExpr case_expr;
    if (!check(TokenKind::When)) {
      case_expr.operand = parse_expr();
    }
    do {
      expect(TokenKind::When);
      CaseWhen when{.condition = parse_expr()};
      expect(TokenKind::Then);
      when.result = parse_expr();
      case_expr.when_clauses.push_back(std::move(when));
    } while (check(TokenKind::When));
    if (match(TokenKind::Else)) {
      case_expr.else_expr = parse_expr();
    }
    expect(TokenKind::End);
    return make_expr(std::move(case_expr));
  }

  struct BinaryOperatorInfo {
    BinaryOperator op;
    int precedence;
  };

  std::optional<BinaryOperatorInfo> binary_operator(TokenKind kind) const {
    switch (kind) {
    case TokenKind::Or:
      return BinaryOperatorInfo{.op = BinaryOperator::Or, .precedence = 1};
    case TokenKind::And:
      return BinaryOperatorInfo{.op = BinaryOperator::And, .precedence = 2};
    case TokenKind::Less:
      return BinaryOperatorInfo{.op = BinaryOperator::Less, .precedence = 4};
    case TokenKind::Greater:
      return BinaryOperatorInfo{.op = BinaryOperator::Greater, .precedence = 4};
    case TokenKind::LessEqual:
      return BinaryOperatorInfo{.op = BinaryOperator::LessEqual,
                                .precedence = 4};
    case TokenKind::GreaterEqual:
      return BinaryOperatorInfo{.op = BinaryOperator::GreaterEqual,
                                .precedence = 4};
    case TokenKind::Equal:
    case TokenKind::EqualEqual:
      return BinaryOperatorInfo{.op = BinaryOperator::Equal, .precedence = 4};
    case TokenKind::NotEqual:
    case TokenKind::BangEqual:
      return BinaryOperatorInfo{.op = BinaryOperator::NotEqual,
                                .precedence = 4};
    case TokenKind::Amp:
      return BinaryOperatorInfo{.op = BinaryOperator::BitwiseAnd,
                                .precedence = 5};
    case TokenKind::Pipe:
      return BinaryOperatorInfo{.op = BinaryOperator::BitwiseOr,
                                .precedence = 5};
    case TokenKind::ShiftLeft:
      return BinaryOperatorInfo{.op = BinaryOperator::ShiftLeft,
                                .precedence = 5};
    case TokenKind::ShiftRight:
      return BinaryOperatorInfo{.op = BinaryOperator::ShiftRight,
                                .precedence = 5};
    case TokenKind::Plus:
      return BinaryOperatorInfo{.op = BinaryOperator::Add, .precedence = 6};
    case TokenKind::Minus:
      return BinaryOperatorInfo{.op = BinaryOperator::Subtract,
                                .precedence = 6};
    case TokenKind::Star:
      return BinaryOperatorInfo{.op = BinaryOperator::Multiply,
                                .precedence = 7};
    case TokenKind::Slash:
      return BinaryOperatorInfo{.op = BinaryOperator::Divide, .precedence = 7};
    case TokenKind::Percent:
      return BinaryOperatorInfo{.op = BinaryOperator::Modulo, .precedence = 7};
    case TokenKind::PipePipe:
      return BinaryOperatorInfo{.op = BinaryOperator::Concat, .precedence = 8};
    default:
      return std::nullopt;
    }
  }

  template <typename Node> ExprPtr make_expr(Node node) {
    return std::make_shared<Expr>(Expr{.node = std::move(node)});
  }

  Identifier parse_identifier() {
    Identifier identifier;
    identifier.parts.push_back(expect_identifier_string());
    while (check(TokenKind::Dot) && check_next(TokenKind::Identifier)) {
      match(TokenKind::Dot);
      identifier.parts.push_back(expect_identifier_string());
    }
    return identifier;
  }

  std::string expect_identifier_string() {
    return expect(TokenKind::Identifier).text;
  }

  std::vector<Token> tokens_;
  std::size_t position_ = 0;
};

DdlStatement ddl_from_statement(const Statement &statement) {
  if (const auto *stmt = std::get_if<CreateTableStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<CreateIndexStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<DropTableStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<DropIndexStmt>(&statement))
    return *stmt;
  throw ParseError(SourceLocation{}, "expected DDL statement");
}

DmlStatement dml_from_statement(const Statement &statement) {
  if (const auto *stmt = std::get_if<SelectStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<InsertStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<UpdateStmt>(&statement))
    return *stmt;
  if (const auto *stmt = std::get_if<DeleteStmt>(&statement))
    return *stmt;
  throw ParseError(SourceLocation{}, "expected DML statement");
}

} // namespace

ParseError::ParseError(SourceLocation location, std::string message)
    : std::runtime_error(std::move(message)), location_(location) {}

SourceLocation ParseError::location() const { return location_; }

Statement parse_statement(std::string_view sql) {
  Lexer lexer(sql);
  Parser parser(lexer.tokenize());
  return parser.parse();
}

DdlStatement parse_ddl_statement(std::string_view sql) {
  return ddl_from_statement(parse_statement(sql));
}

DmlStatement parse_dml_statement(std::string_view sql) {
  return dml_from_statement(parse_statement(sql));
}

} // namespace minidb
