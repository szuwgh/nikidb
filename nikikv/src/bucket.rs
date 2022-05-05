use std::collections::HashMap;

use crate::cursor::Cursor;
use crate::error::NKResult;
use crate::page::{Node, Page, Pgid};
use crate::tx::Tx;
use std::rc::{Rc, Weak};

pub struct Bucket {
    bucket: IBucket,
    nodes: HashMap<Pgid, Node>, //tx: Tx,
    tx: Weak<Tx>,
}

enum PageNode {
    Page(*const Page),
    Node(Node),
}

impl From<Node> for PageNode {
    fn from(n: Node) -> Self {
        PageNode::Node(n)
    }
}

impl Bucket {
    pub fn new(root: Pgid, tx: Weak<Tx>) -> Bucket {
        Self {
            bucket: IBucket {
                root: root,
                sequence: 0,
            },
            nodes: HashMap::new(),
            tx: tx,
        }
    }

    pub fn create_bucket(&mut self) {
        let mut c = self.cursor();
    }

    fn cursor(&mut self) -> Cursor {
        Cursor { bucket: self }
    }

    pub fn put(key: &[u8], value: &[u8]) {}

    pub fn get(key: &[u8]) {}

    pub fn page_node(&self, id: Pgid) { //-> PageNode
                                        // if let Some(node) = self.nodes.get(&id) {
                                        //     return PageNode::Node(node.clone());
                                        // }
    }

    pub fn value() {}
}

pub struct IBucket {
    root: Pgid,
    sequence: u64,
}

impl IBucket {
    pub fn new(root: Pgid) -> IBucket {
        Self {
            root: root,
            sequence: 0,
        }
    }
}
