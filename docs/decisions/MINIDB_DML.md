# MiniDB DML Syntax

## Expression

Okay, it generally seems like it'd be unproductive to generate expression syntax
from first principles. Here is the expected syntaxes, which I will ask an AI
to complete it:

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
  - `` `quoted identifier` ``
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

### Formalized syntax

The following grammar is intended as the parser-facing shape of expressions.
Operators are listed from lowest to highest precedence. Binary operators are
left-associative unless noted otherwise.

```ebnf
expr:
  or_expr

or_expr:
  and_expr { OR and_expr }

and_expr:
  not_expr { AND not_expr }

not_expr:
    NOT not_expr
  | predicate_expr

predicate_expr:
  bitwise_expr
    [
      comparison_operator bitwise_expr
    | IS [ NOT ] bitwise_expr
    | [ NOT ] BETWEEN bitwise_expr AND bitwise_expr
    | [ NOT ] IN "(" [ expr_list ] ")"
    | [ NOT ] LIKE bitwise_expr
    ]

comparison_operator:
  "<" | ">" | "<=" | ">=" | "=" | "==" | "<>" | "!="

bitwise_expr:
  additive_expr { bitwise_operator additive_expr }

bitwise_operator:
  "&" | "|" | "<<" | ">>"

additive_expr:
  multiplicative_expr { additive_operator multiplicative_expr }

additive_operator:
  "+" | "-"

multiplicative_expr:
  concat_expr { multiplicative_operator concat_expr }

multiplicative_operator:
  "*" | "/" | "%"

concat_expr:
  unary_expr { "||" unary_expr }

unary_expr:
    unary_operator unary_expr
  | primary_expr

unary_operator:
  "-" | "+" | "~"

primary_expr:
    literal_value
  | identifier
  | function_call
  | case_expr
  | "(" expr ")"

function_call:
  identifier "(" [ expr_list ] ")"

expr_list:
  expr { "," expr }

case_expr:
  CASE [ expr ] when_clause { when_clause } [ ELSE expr ] END

when_clause:
  WHEN expr THEN expr

literal_value:
    numeric_literal
  | string_literal
  | NULL
  | TRUE
  | FALSE

identifier:
  identifier_part { "." identifier_part }

identifier_part:
    bare_identifier
  | quoted_identifier

bare_identifier:
  identifier_start { identifier_continue }

quoted_identifier:
  "`" { quoted_identifier_character | escaped_backtick } "`"

quoted_identifier_character:
  any character except "`"

escaped_backtick:
  "``"

identifier_start:
  ascii_letter | "_"

identifier_continue:
  ascii_letter | digit | "_"

numeric_literal:
    decimal_integer [ fractional_part ] [ exponent_part ]
  | fractional_part [ exponent_part ]

decimal_integer:
  digit { digit }

fractional_part:
  "." digit { digit }

exponent_part:
  ( "e" | "E" ) [ "+" | "-" ] digit { digit }

string_literal:
  "'" { string_character | escaped_quote } "'"

string_character:
  any character except "'"

escaped_quote:
  "''"

digit:
  "0" | "1" | "2" | "3" | "4" | "5" | "6" | "7" | "8" | "9"

ascii_letter:
    "a" | "b" | "c" | "d" | "e" | "f" | "g" | "h" | "i"
  | "j" | "k" | "l" | "m" | "n" | "o" | "p" | "q" | "r"
  | "s" | "t" | "u" | "v" | "w" | "x" | "y" | "z"
  | "A" | "B" | "C" | "D" | "E" | "F" | "G" | "H" | "I"
  | "J" | "K" | "L" | "M" | "N" | "O" | "P" | "Q" | "R"
  | "S" | "T" | "U" | "V" | "W" | "X" | "Y" | "Z"
```

Pratt parser binding powers can follow the same precedence order:

| Precedence | Operators / forms                                                                                                         |
| ---------- | ------------------------------------------------------------------------------------------------------------------------- |
| 1          | `OR`                                                                                                                      |
| 2          | `AND`                                                                                                                     |
| 3          | prefix `NOT`                                                                                                              |
| 4          | `<`, `>`, `<=`, `>=`, `=`, `==`, `<>`, `!=`, `IS`, `IS NOT`, `BETWEEN`, `NOT BETWEEN`, `IN`, `NOT IN`, `LIKE`, `NOT LIKE` |
| 5          | <code>&amp;</code>, <code>&#124;</code>, `<<`, `>>`                                                                       |
| 6          | `+`, `-`                                                                                                                  |
| 7          | `*`, `/`, `%`                                                                                                             |
| 8          | <code>&#124;&#124;</code>                                                                                                 |
| 9          | prefix `-`, prefix `+`, prefix `~`                                                                                        |
| 10         | literals, identifiers, function calls, parenthesized expressions, `CASE`                                                  |

`BETWEEN` consumes the following `AND` as part of the predicate, not as the
boolean operator. `CASE` has two forms: a simple case with an initial expression
and a searched case without one. Numeric signs are parsed as unary operators,
not as part of `numeric_literal`. Literal token details may be implemented in
the tokenizer or in the parser, but the accepted source syntax should match the
productions above. Reserved keywords are not accepted as `bare_identifier`
but may be used as `quoted_identifier`.

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
