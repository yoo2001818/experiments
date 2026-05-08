# MiniDB DDL syntax

Mostly copied from MySQL documentation...

```
CREATE TABLE [IF NOT EXISTS] tbl_name
    (create_definition,...)

create_definition: {
    col_name column_definition
  | {INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option] ...
  | PRIMARY KEY [index_name] [index_type] (key_part,...) [index_option] ...
  | UNIQUE {INDEX | KEY} [index_name] [index_type] (key_part,...) [index_option] ...
}

column_definition: {
    data_type [NOT NULL | NULL]
      [UNIQUE [KEY]] [[PRIMARY] KEY]
      [COMMENT 'string']
}

data_type:
  | INTEGER
  | BOOLEAN
  | CHAR(length)
  | BINARY(length)

key_part: col_name [ASC | DESC]

CREATE [UNIQUE] INDEX index_name
    ON tbl_name (key_part,...)
    [index_option]

index_option: {
  COMMENT 'string'
}

DROP TABLE [IF EXISTS]
    tbl_name [, tbl_name] ...

DROP INDEX index_name ON tbl_name
```
