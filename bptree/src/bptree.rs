const maxElementsCount: u64 = 4;

struct node {
    is_leaf: bool,
    keys: Vec<i32>,
}

struct leafElement {}

struct Bptree {
    root: node,
}
