# DML Binder Revisited

Well, the previous binder article became too complicated, therefore it'd be
a good idea to create a shorter article about design decisions. Since it's
been a while since I wrote that, here's a short recap:

- Intermediate frame layout IS directly related to the rest of the evaluator,
  the evaluator should ideally get an AST with inputs that are bound to
  specific slots
- Additionally, there should be a conventional concept of "scope" or "namespace"
  that know all the possible candidates and resolve them recursively (or not)
- There can be multiple phases in a single query; this is particularly visible
  when there are aggregation or subqueries

Considering this, how can it be implemented? Perhaps it'd be much quicker to
just write some code about it. But before that, a quick thought about what
the binder should do...

1. Assuming we already have which tables to query (FROM a, b), fill maps with
   affected tables and columns so `id` or `products.id` can be resolved to a
   physical column
   - This is a big assumption, yes, but still
2. Each phase has an input and an optional output intermediate frame layout
   array, which is filled recursively whenever a new value needs to show up.
   - "from phase" picks up columns from tables, which is used for WHERE/SELECT
   - the SELECT intermediate layout itself is the output layout of the query
   - ORDER BY references columns from "select phase", hence it can modify the
     select phase layout by referencing new columns
   - it's more like a pipe, so the input and the output must be identical
     at the phase border
3. With the phase system set up, AST walker can walk through the query and
   replace each identifiers into bound identifiers going `$0`, etc, while
   optionally deriving the type information as well
4. Aggregation makes it more complicated as `AVG($0)` itself can become a
   column in the intermediate frame; It could be seen that a function call
   itself can be considered as an identifier

With this, the examples in the former document would be useful.

```js
{
  from: ['products'],
  // references used in FROM phase
  from_refs: ['products.price', 'products.product_name'],
  where: '$0 > 1000',
  select_refs: [
    { hidden: false, name: 'applied_price', expr: '$0 * 1.1' },
    { hidden: true, expr: '$1' },
  ],
  order_by: [
    { expr: '$0', direction: 'ASC' },
    { expr: '$1', direction: 'DESC' },
  ],
}
```
