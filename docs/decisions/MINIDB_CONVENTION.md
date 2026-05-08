# MiniDB C++ Conventions

- Namespace: `minidb`
- Types, classes, structs, enums: `PascalCase`
  - `Expr`, `SelectStmt`, `ColumnRef`, `Catalog`
- Functions and methods: `snake_case`
  - `parse_statement`, `add_table`, `find_column`
- Variables and public data fields: `snake_case`
  - `table_name`, `column_refs`, `is_nullable`
- Private class fields: trailing underscore
  - `tables_`, `page_size_`
- Enum values: `PascalCase`
  - `BinaryOp::Equal`, `BinaryOp::LessEqual`
- Constants: `kPascalCase`
  - `kPageSize`, `kMaxIdentifierLength`

AST nodes should generally be plain structs with public `snake_case` fields.
