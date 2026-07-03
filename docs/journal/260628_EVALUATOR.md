# Evaluator

.... It's been a long time since I thought about the DML implementation, and
the simple "expression engine" has become frankly everything needed to
run the query itself. Except the fact it runs full scan. Which is bad.

Anyway, if I implement the binder, I would have means to analyze the
dependencies of columns of SELECT queries. Which is something, but it's not
inherently useful.

The former binder article covers some of that, but it's almost a month old and
the article is already too long. Anyway.

First of all, how would the query itself be expressed before it goes through
the query planner? We need a logical query with semantics already resolved.

Second, how would we evaluate the expressions embedded in the query?

The query needs an intermediate model, which would be used for subsequent
implementations, which needs to incorporate the binder as well.

## The intermediate model

Let's take a look at the current `SelectStmt` first.

```cpp
struct SelectStmt {
  bool distinct = false;
  std::vector<SelectItem> select_list;
  std::vector<TableReference> from;
  std::optional<ExprPtr> where;
  std::vector<OrderByTerm> order_by;
  std::optional<LimitClause> limit;
};
```

This is, well, raw AST. How should it be expressed after it goes through the
binder, so that everything is already resolved semantically, and further
processing can happen to make it more efficient, etc? This is called a logical
plan.

- Scan (From)
- Filter
- Aggregate
- Project
- Sort

Unfortunately, the phase and scope model mentioned earlier directly
corresponds to this. Which means that, well, I might not even need a separate
data structure for this - only the scope object.

## Logical plan

So... yes, change of the plan: The binder should work closely with the
logical plan, that is, it should resolve symbols and modify the logical plan -
each operator - to include new columns into the data frame as it constructs
the operators needed to express the query.

One problem though - the scan logical operator can only scan through one table,
whereas the original binder design didn't anticipate any kind of joins. No
problem - we'll have to make a "join" logical operator that does simple
cartesian joins. This means that the join operator needs to "route" the columns
to the correct one as well.

```cpp
struct RefSlot {
  std::uint32_t index;
}

struct RefScanDependency {
  std::string canonical_name;
  ColumnRef col_ref;
}

enum class JoinDirection { LEFT, RIGHT };

struct RefJoinDependency {
  std::string canonical_name;
  JoinDirection direction;
  ColumnRef col_ref;
}

struct RefSelectDependency {
  std::string canonical_name;
  Expr expr;
  bool is_hidden;
}

class LogicalOp {
  virtual bool can_resolve(Identifier const& identifier);
  virtual RefSlot resolve(Identifier const& identifier);
}

class ScanLogicalOp : public LogicalOp {
  Table& table;
  std::vector<RefScanDependency> frame;
  ScanLogicalOp(Table& table): table(table) {
  }
  virtual RefSlot resolve(Identifier const& identifier) {
    // It has access to columns specified in FROM, it can just use that
    // Create/retrieve the RefSlot and return them
  }
}

class JoinLogicalOp : public LogicalOp {
  std::unique_ptr<LogicalOp> left;
  std::unique_ptr<LogicalOp> right;
  std::vector<RefJoinDependency> frame;
}

class ProjectLogicalOp : public LogicalOp {
  Scope * parent;
  std::vector<RefSelectDependency> frame;
  void append(Expr const& expr, Identifier const& alias) {
    // Appends SELECT (expr) a, (expr) b, ... into the intermediate frame
  }
  virtual RefSlot resolve(Identifier const& identifier) {
    // If the ref is already resolved in the frame, just use it
    // otherwise, resolve from the parent and append it to the frame
    auto parent_ref = parent->resolve(identifier);
    this->frame.push({ canonical_name, parent_ref, true });
  }
}
```

## Physical plan?

Well, the structure inevitably leads to implementing physical plans as well.
I think it's quite plausible that `LogicalOp` could emit `PhysicalOp` directly,
so that a simple implementation could be done right in front of it.
