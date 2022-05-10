use std::borrow::BorrowMut;

use crate::bucket::{Bucket, PageNode};
use crate::error::NKResult;
use crate::page::{Node, Page, PageFlag, Pgid};

pub(crate) struct Cursor<'a> {
    pub(crate) bucket: &'a Bucket,
    stack: Vec<ElemRef>,
}

#[derive(Clone)]
pub(crate) struct ElemRef {
    page_node: PageNode,
    index: usize, //寻找 key 在哪个 element
}

struct Item<'a>(&'a [u8], &'a [u8], u32);

impl ElemRef {
    fn is_leaf(&self) -> bool {
        match &self.page_node {
            PageNode::Node(n) => n.is_leaf,
            PageNode::Page(p) => unsafe { (*(*p)).flags == PageFlag::LeafPageFlag },
        }
    }

    fn count(&self) -> usize {
        match &self.page_node {
            PageNode::Node(n) => n.inodes.len(),
            PageNode::Page(p) => self.get_page(p).count as usize,
        }
    }

    fn get_page(&self, p: &*const Page) -> &Page {
        unsafe { &**p }
    }
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(bucket: &'a Bucket) -> Cursor<'a> {
        Self {
            bucket: bucket,
            stack: Vec::new(),
        }
    }

    fn first(&mut self) {}

    fn last(&mut self) {}

    fn next(&mut self) -> NKResult<Option<Item>> {
        loop {
            let mut index: usize = 0;
            for i in self.stack.len() - 1..0 {}
        }
        self.key_value()
    }

    fn prev(&mut self) {}

    fn delete(&mut self) {}

    fn seek(&mut self, key: &[u8]) -> NKResult<()> {
        let mut item: Option<Item> = None;
        item = self.seek_elem(key)?;
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        if ref_elem.index >= ref_elem.count() {}
        Ok(())
    }

    fn seek_elem(&mut self, key: &[u8]) -> NKResult<Option<Item>> {
        self.stack.clear();
        self.search(key, self.bucket.ibucket.root)?;
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        if ref_elem.index >= ref_elem.count() {
            return Ok(None);
        }
        self.key_value()
    }

    fn key_value(&self) -> NKResult<Option<Item>> {
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        match &ref_elem.page_node {
            PageNode::Node(n) => Ok(None),
            PageNode::Page(p) => {
                let leaf = ref_elem.get_page(p).leaf_page_element(ref_elem.index);
                Ok(Some(Item(leaf.key(), leaf.value(), leaf.flags)))
            }
        }
    }

    //查询
    fn search(&mut self, key: &[u8], id: Pgid) -> NKResult<()> {
        let page_node = self.bucket.page_node(id)?;
        let elem_ref = ElemRef {
            page_node: page_node,
            index: 0,
        };
        self.stack.push(elem_ref.clone());
        if elem_ref.is_leaf() {
            self.nsearch(key)?;
        }
        match elem_ref.page_node {
            PageNode::Node(n) => self.search_node(key, &n)?,
            PageNode::Page(p) => self.search_page(key, unsafe { &*p })?,
        }
        Ok(())
    }

    //搜索叶子节点的数据
    fn nsearch(&mut self, key: &[u8]) -> NKResult<()> {
        let e = self.stack.last_mut().ok_or("stack empty")?;
        match &e.page_node {
            PageNode::Node(n) => {}
            PageNode::Page(p) => {
                let p = unsafe { &**p };
                let inodes = p.leaf_page_elements();
                let index = match inodes.binary_search_by(|inode| inode.key().cmp(key)) {
                    Ok(v) => (v),
                    Err(e) => (e),
                };
                e.index = index;
            }
        }
        Ok(())
    }

    fn search_page(&mut self, key: &[u8], p: &Page) -> NKResult<()> {
        let inodes = p.branch_page_elements();
        let (exact, mut index) = match inodes.binary_search_by(|inode| inode.key().cmp(key)) {
            Ok(v) => (true, v),
            Err(e) => (false, e),
        };
        if !exact && index > 0 {
            index -= 1;
        }
        self.stack.last_mut().ok_or("stack empty")?.index = index;
        self.search(key, inodes[index].pgid)?;
        Ok(())
    }

    fn search_node(&self, key: &[u8], p: &Node) -> NKResult<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_sort_search() {
        let s = [0, 1, 1, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55];

        println!("{:?}", s.binary_search(&60));
    }
}
