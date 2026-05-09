# MiniDB Storage Layout

All binary encodings for integers assume little-endian unless specified.

## File Layout

- minidb.db/
  - catalog.json
  - tables/
    - {table_storage_name}/
      - rows.bin
      - indexes/
        - {index_storage_name}.bin

## Catalog JSON

```json
{
  "version": 1,
  "next_table_storage_id": 2,
  "tables": [
    {
      "name": "users",
      "storage_name": "t_00000001",
      "next_index_storage_id": 2,
      "columns": [
        {
          "name": "id",
          "type": "INTEGER",
          "size": null,
          "nullable": false,
          "unique": false,
          "primary_key": false,
          "comment": null
        },
        {
          "name": "name",
          "type": "CHAR",
          "size": 16,
          "nullable": false,
          "unique": false,
          "primary_key": false,
          "comment": null
        }
      ],
      "indexes": [
        {
          "name": "index_name",
          "storage_name": "i_00000001",
          "unique": false,
          "primary_key": false,
          "type": "BTREE",
          "keys": [{ "column": "name", "direction": "ASC" }]
        }
      ]
    }
  ]
}
```

## Row Binary Layout

- tombstone (u8) - 0: alive, 1: deleted
- null bitmap (u8[], `ceil(num_columns / 8)` bytes)
  - column i uses bit `i % 8` in byte `i / 8`
  - if set, payload bytes for that column are present but ignored
- ...columns
  - integer:
    - value (i64)
  - boolean:
    - value (u8) - 0: false, 1: true, intentionally not binary packing
  - char:
    - value (u8[], N bytes, where N = size) - `\0` padded
  - binary:
    - length (u16)
    - value (u8[], N bytes, where N = size)

## Rows File

Not implementing pages would be better for now, so it shall follow this pattern:

- header
  - magic (u8\[4]) - "mdrw"
  - version (u32) - 1
- ...rows
  - (see row binary layout above)

## Index File

TBD, to be implemented when the database actually starts storing data
