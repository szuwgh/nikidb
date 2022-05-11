use std::borrow::BorrowMut;

use crate::bucket::{Bucket, PageNode};
use crate::error::NKResult;
use crate::page::{BucketLeafFlag, Node, Page, PageFlag, Pgid};

pub(crate) struct Cursor<'a> {
    pub(crate) bucket: &'a Bucket,
    stack: Vec<ElemRef>,
}

#[derive(Clone)]
pub(crate) struct ElemRef {
    page_node: PageNode,
    index: usize, //寻找 key 在哪个 element
}

pub(crate) struct Item<'a>(Option<&'a [u8]>, Option<&'a [u8]>, u32);

impl<'a> Item<'a> {
    fn from(key: &'a [u8], value: &'a [u8], flags: u32) -> Item<'a> {
        Self(Some(key), Some(value), flags)
    }

    fn null() -> Item<'a> {
        Self(None, None, 0)
    }

    pub(crate) fn key(&self) -> Option<&'a [u8]> {
        self.0
    }

    pub(crate) fn flags(&self) -> u32 {
        self.2
    }
}

impl ElemRef {
    fn is_leaf(&self) -> bool {
        match &self.page_node {
            PageNode::Node(n) => n.is_leaf,
            PageNode::Page(p) => self.get_page(p).flags == PageFlag::LeafPageFlag,
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

    fn first(&mut self) -> NKResult<()> {
        loop {
            let ref_elem = self.stack.last().ok_or("stack empty")?;
            if ref_elem.is_leaf() {
                break;
            }
            let pgid = match &ref_elem.page_node {
                PageNode::Node(n) => n.inodes.get(ref_elem.index).ok_or("get node fail")?.pgid,
                PageNode::Page(p) => {
                    ref_elem
                        .get_page(p)
                        .branch_page_element(ref_elem.index)
                        .pgid
                }
            };
            let page_node = self.bucket.page_node(pgid)?;
            self.stack.push(ElemRef {
                page_node: page_node,
                index: 0,
            });
        }
        Ok(())
    }

    fn last(&mut self) {}

    fn next(&mut self) -> NKResult<Item<'a>> {
        loop {
            let mut index: usize = 0;
            let mut i: i32 = -1;
            for _i in (0..self.stack.len() - 1).rev() {
                //取上一页数据
                let elem = self.stack.get_mut(_i).ok_or("get elem fail")?;
                if elem.index < elem.count() {
                    elem.index += 1;
                    i = _i as i32;
                    break;
                }
            }
            if i == -1 {
                return Ok(Item::null());
            }
            self.stack.truncate((i + 1) as usize);
            self.first()?;
            if self.stack.last().unwrap().count() == 0 {
                continue;
            }
            return self.key_value();
        }
    }

    fn prev(&mut self) {}

    fn delete(&mut self) {}

    pub(crate) fn seek(&mut self, key: &[u8]) -> NKResult<Item<'a>> {
        //  let mut item: Option<Item> = None;
        let mut item = self.seek_elem(key)?;
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        if ref_elem.index >= ref_elem.count() {
            item = self.next()?;
        }
        if item.key().is_none() {
            return Ok(Item::null());
        } else if (item.flags() & BucketLeafFlag) != 0 {
            item.1 = None;
        }
        Ok(item)
    }

    fn seek_elem(&mut self, key: &[u8]) -> NKResult<Item<'a>> {
        self.stack.clear();
        self.search(key, self.bucket.ibucket.root)?;
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        if ref_elem.index >= ref_elem.count() {
            return Ok(Item::null());
        }
        self.key_value()
    }

    fn key_value(&mut self) -> NKResult<Item<'a>> {
        let ref_elem = self.stack.last().ok_or("stack empty")?;
        match &ref_elem.page_node {
            PageNode::Node(n) => Ok(Item::null()),
            PageNode::Page(p) => {
                let elem = ref_elem.get_page(p).leaf_page_element(ref_elem.index);
                Ok(Item::from(
                    unsafe { &*(elem.key() as *const [u8]) },
                    unsafe { &*(elem.value() as *const [u8]) },
                    elem.flags,
                ))
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
        //
        match &elem_ref.page_node {
            PageNode::Node(n) => self.search_node(key, n)?,
            PageNode::Page(p) => self.search_page(key, elem_ref.get_page(p))?,
        }
        Ok(())
    }

    //搜索叶子节点的数据
    fn nsearch(&mut self, key: &[u8]) -> NKResult<()> {
        let e = self.stack.last_mut().ok_or("stack empty")?;
        match &e.page_node {
            PageNode::Node(n) => {}
            PageNode::Page(p) => {
                let inodes = e.get_page(p).leaf_page_elements();
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
