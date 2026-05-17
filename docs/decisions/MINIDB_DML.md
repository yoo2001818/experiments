# MiniDB DML Syntax

## Expression

Okay, it generally seems like it'd be unproductive to generate expression syntax
from first principles. Here is the expected syntaxes:

### Expected syntaxes

This is not at all complete or rigorous, just indicating features I might want to implement.

- literal_value
  - numeric_literal
  - string_literal
  - NULL
  - TRUE
  - FALSE
- identifier
  - table_name.column_name
  - column_name
- `( expr )`
- unary_operator
  - `- expr`
  - `+ expr`
  - `~ expr`
  - `NOT expr`
- binary_operator
  - concat: `||`
  - multiplicative: `*` `/` `%`
  - additive: `+` `-`
  - bitwise: `&` `|` `<<` `>>`
  - compare: `<` `>` `<=` `>=`
  - compare_2: `=` `==` `<>` `!=`
  - boolean: `AND` `OR`
- function_call
  - `function_name([expr] [, expr] ...)`
- `IS [NOT] expr`
- `[NOT] BETWEEN expr AND expr`
- `[NOT] IN (expr [, expr] ...)`
- `[NOT] LIKE expr`
- `CASE [expr] { WHEN expr THEN expr } ... [ELSE expr] END`

### Formalized syntax here

(I'll ask an AI to make EBNF-style syntax from that, assuming that a Pratt parser would be used)

## SELECT

```
SELECT [DISTINCT] select_expr [, select_expr] ...
[FROM table_references]
[WHERE where_condition]
[ORDER BY {col_name | expr | position} [ASC | DESC], ...]
[LIMIT {[offset,] row_count | row_count OFFSET offset}];
```

## INSERT

```
INSERT [INTO] tbl_name [(col_name [, col_name] ...)]
{ {VALUES | VALUE} (value_list) [, (value_list)] ... };

INSERT [INTO] tbl_name
SET assignment_list;

INSERT [INTO] tbl_name [(col_name [, col_name] ...)]
{ SELECT ... | TABLE table_name | VALUES row_constructor_list};

value: { expr | DEFAULT }
value_list: value [, value] ...
row_constructor_list: ROW(value_list)[, ROW(value_list)][, ...]
assignment:
  col_name = {value | [tbl_name.]col_name}
assignment_list: assignment [, assignment] ...
```

## UPDATE

```
UPDATE table_reference SET assignment_list
[WHERE where_condition]
[ORDER BY ...]
[LIMIT row_count]
```

## DELETE

```
DELETE FROM tbl_name
[WHERE where_condition]
[ORDER BY ...]
[LIMIT row_count]
```
