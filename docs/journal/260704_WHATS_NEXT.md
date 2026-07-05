# What's Next?

Unfortunately, the 'binder' has snowballed into completely new things, and it
no longer lines up with the current implementation, and the backlog of new
concepts makes it difficult to how to move forward.

It doesn't help that the codebase hasn't been edited for almost 2 months at
this point - Most of the code is AI generated, but I had some rough ideas about
how they operated, which I don't anymore.

So, a quick to-do list for the "binder" seems to be necessary.

- Evaluator implementation
- Evaluator refs
- BoundRefExpr rewrites
- Logical plans
- Scope resolving
- Physical plans

## Evaluator implementation

A really simple evaluator can be implemented first, without any inputs. Since
this can be done without any other SQL madness, with minimal integration to the
existing codebase, it should be tackled first.

That is, any identifier references can just return null in this phase, as the
point is to just make an evaluator. After implementing it as a separate module,
it can be called through trivial queries requiring no tables, like
`SELECT 1 + 1;`.

## Evaluator refs

Then, the evaluator should be able to read from the `BoundRefExpr` and the
input array, as seen from the `ast_evaluate` function in the evaluator document.

This is of course no means usable at this point, but unit tests are possible,
so let's use that.

## BoundRefExpr rewrites

## Logical plans

Then the logical plans. The actual binder architecture is not important at this
point - just the fact that the query can be expressed in a certain way.
