# Storage Concerns

The original plan of MiniDB was to have a simple ".db" file that contains
everything, like SQLite. However, this approach requires me to build pages,
page manager, cache, serialization formats, the entire ordeal before I get to
write a single row or a single schema.

This is obviously not ideal, as this means I'm building something like a
filesystem, before making the database do database things. That is to say,
this is a bad yak to shave. I don't need an another filesystem.

Since the point of the project is to create a hobby-level research database, not
a "mini portable database" like SQLite, a single-file approach would be flawed
and would need to duplicate many of the things that a filesystem already does.

Well, many features involving database modifications and MVCC, etc, all require
pages, page allocation, compaction, etc. But this is not the thing I should do
right now. The point is to avoid the entire yak dragons to reach the bare
minimum database engine.

Hence, it would be much wiser to just use different files for each needs. Each
table gets a file. The table catalog should be a single JSON file, not anything
else. Otherwise I end up making a serialization layer just for the table catalog.

One additional concern is that variable row sizes tend to pull in nasty things.
One example is indirect pointers to different pages, like TOAST in PostgreSQL.
The other example is the entire headache when the rows get updated to larger
sizes. Now I have to deal with fragmentation, compaction, etc.
This is a filesystem concern all over again.

So, for the sake of focusing on a MVP that enables you to query things, it'd be
best to skip all of that. A fixed-size row, and an append-only heap would be
reasonable choice. No VARCHAR or anything, just something that resembles
COBOL-era ISAM.

This means that the table implementation now only has to manage paging, aka
packing rows every 4kB because aliasing is a known pain, and a single file that
contains all the rows. Maybe a tombstone flag and a row ID as well.

This greatly simplifies everything; many blockers in the roadmap is addressed by
simply postponing everything until I actually need it. Meaning:

1. No "binary representation of the table catalog" other than JSON
2. No page manager and no file system
3. Maybe a minimalistic in-memory cache manager, or consider using mmap
4. No B-tree or any other complicated logic initially, and yes, no indexes

This means that I can skip straight into DML implementation with its full-scan
glory. Which is great for maintaining project velocity.

The only thing that should be addressed is then the row format. However, since
the table columns all have fixed size, it would be trivial, as a fixed pointer
offset can be used to derive the position. Perhaps I would need a null bitset,
a tombstone, a row ID, but that's it. Then, a page can also be simple enough.

A page should store... row data without any preamble or anything, because it's
not necessary at this point. A 4kB page could contain 4096 / row_size rows,
with each row at row_index \* row_size, and that is the entire definition.

Maybe file names too, because now the database needs multiple files to operate.

- minidb.db/
  - catalog.json
  - tables/
    - {table_name}/
      - rows.bin
      - indexes/
        - {index_name}.bin

That is literally it for now.
