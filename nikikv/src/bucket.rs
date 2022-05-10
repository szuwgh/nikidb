use std::collections::HashMap;

use crate::cursor::Cursor;
use crate::error::NKResult;
use crate::page::{Node, Page, Pgid};
use crate::tx::TxImpl;
use std::sync::{Arc, Weak};

pub(crate) struct Bucket {
    pub(crate) ibucket: IBucket,
    nodes: HashMap<Pgid, Node>, //tx: Tx,
    pub(crate) weak_tx: Weak<TxImpl>,
}

#[derive(Clone)]
pub(crate) enum PageNode {
    Page(*const Page),
    Node(Node),
}

impl From<Node> for PageNode {
    fn from(n: Node) -> Self {
        PageNode::Node(n)
    }
}

impl Bucket {
    pub(crate) fn new(root: Pgid, tx: Weak<TxImpl>) -> Bucket {
        Self {
            ibucket: IBucket {
                root: root,
                sequence: 0,
            },
            nodes: HashMap::new(),
            weak_tx: tx,
        }
    }

    pub(crate) fn create_bucket(&mut self) {
        let mut c = self.cursor();
    }

    fn cursor(&mut self) -> Cursor {
        Cursor::new(self)
        // Cursor { bucket: self }
    }

    pub(crate) fn put(key: &[u8], value: &[u8]) {}

    pub(crate) fn get(key: &[u8]) {}

    pub(crate) fn page_node(&self, id: Pgid) -> NKResult<PageNode> {
        if let Some(node) = self.nodes.get(&id) {
            return Ok(PageNode::Node(node.clone()));
        }
        let page = self.tx().unwrap().db().page(id);
        Ok(PageNode::Page(page))
    }

    pub(crate) fn tx(&self) -> Option<Arc<TxImpl>> {
        self.weak_tx.upgrade()
    }

    pub(crate) fn value() {}
}

pub(crate) struct IBucket {
    pub(crate) root: Pgid,
    sequence: u64,
}

impl IBucket {
    pub(crate) fn new(root: Pgid) -> IBucket {
        Self {
            root: root,
            sequence: 0,
        }
    }
}
