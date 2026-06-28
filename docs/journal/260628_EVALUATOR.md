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
