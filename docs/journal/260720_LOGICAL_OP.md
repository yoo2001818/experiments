# Logical Operator Excerpt

Okay - it feels like it's generally awful to think about the high-level
structure in C++. I tried coming up with the structure in C++ first, but
there are so much to think about, it's too much.

So I think it's better to think about the C++ implementation after I laid out
how it can be done in TypeScript first.

```ts
interface FrameColumnDescriptor {
  index: number;
  name: string;
  expr: Expr;
}

type FrameDescriptor = FrameColumnDescriptor[];

interface LogicalOp {
  getFrame(): FrameDescriptor;
}

interface ScanFrameColumnDescriptor {
  descriptor: FrameColumnDescriptor;
  columnIndex: number;
}

class ScanLogicalOp extends LogicalOp {
  table: Table;
  frame: ScanFrameColumnDescriptor[] = [];
  constructor(table: Table) {
    this.table = table;
  }

  getFrame(): FrameDescriptor {
    return this.frame.map((column) => column.descriptor);
  }
}

class FilterLogicalOp extends LogicalOp {
  parent: LogicalOp;
  constructor(parent: LogicalOp) {
    this.parent = parent;
  }

  getFrame(): FrameDescriptor {
    return this.parent.getFrame();
  }
}

interface CrossJoinFrameColumnDescriptor {
  descriptor: FrameColumnDescriptor;
  columnIndex: number;
  tableDirection: 'left' | 'right;
}

class CrossJoinLogicalOp extends LogicalOp {
  left: LogicalOp;
  right: LogicalOp;
  frame: CrossJoinFrameColumnDescriptor[] = [];

  constructor(left: LogicalOp, right: LogicalOp) {
    this.left = left;
    this.right = right;
  }

  getFrame(): FrameDescriptor {
    return this.frame.map((column) => column.descriptor);
  }
}

class ProjectLogicalOp extends LogicalOp {
  parent: LogicalOp;
  frame: FrameDescriptor = [];

  constructor(parent: LogicalOp) {
    this.parent = parent;
  }

  getFrame(): FrameDescriptor {
    return this.parent.getFrame();
  }
}
```

While unions/variants could make more sense here, since I don't know how it
will end up, traditional OOP seems to be more manageable for now.
