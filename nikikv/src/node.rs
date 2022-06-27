use crate::bucket::{Bucket, IBucket, MAX_FILL_PERCENT, MIN_FILL_PERCENT};
use crate::db::DBImpl;
use crate::error::NKResult;
use crate::page::{
    BranchPageElementSize, BranchPageFlag, BucketLeafFlag, LeafPageElementSize, LeafPageFlag, Page,
    Pgid, MIN_KEY_PERPAGE,
};
use crate::tx::TxImpl;
use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;
use std::rc::Weak;
use std::sync::Arc;
use std::vec;

#[derive(Clone)]
pub(crate) struct Node(pub(crate) Rc<RefCell<NodeImpl>>);

#[derive(Clone)]
pub(crate) struct NodeImpl {
    // pub(crate) bucket: *mut Bucket,
    pub(crate) is_leaf: bool,
    pub(crate) inodes: Vec<INode>,
    pub(crate) parent: Option<Weak<RefCell<NodeImpl>>>,
    unbalanced: bool,
    spilled: bool,
    pub(crate) pgid: Pgid,
    pub(crate) children: Vec<Node>,
    key: Option<Vec<u8>>,
}

impl NodeImpl {
    pub(crate) fn new() -> NodeImpl {
        Self {
            //  bucket: bucket,
            is_leaf: false,
            inodes: Vec::new(),
            parent: None,
            unbalanced: false,
            spilled: false,
            pgid: 0,
            children: Vec::new(),
            key: None,
        }
    }

    pub fn leaf(mut self, is_leaf: bool) -> NodeImpl {
        self.is_leaf = is_leaf;
        self
    }

    pub fn parent(mut self, parent: Weak<RefCell<NodeImpl>>) -> NodeImpl {
        self.parent = Some(parent);
        self
    }

    pub(crate) fn build(self) -> Node {
        Node(Rc::new(RefCell::new(self)))
    }
}

impl Node {
    #[inline]
    pub(crate) fn node_mut(&mut self) -> RefMut<'_, NodeImpl> {
        (*(self.0)).borrow_mut()
    }
    #[inline]
    pub(crate) fn node(&self) -> Ref<'_, NodeImpl> {
        self.0.borrow()
    }

    pub(crate) fn child_at(
        &mut self,
        bucket: &mut Bucket,
        index: usize,
        parent: Option<Weak<RefCell<NodeImpl>>>,
    ) -> Node {
        if self.node().is_leaf {
            panic!("invalid childAt{} on a leaf node", index);
        }
        bucket.node(self.node().inodes[index].pgid, parent)
    }

    fn min_keys(&self) -> usize {
        if self.node().is_leaf {
            1
        } else {
            2
        }
    }

    pub(crate) fn size(&self) -> usize {
        let mut sz = Page::header_size();
        let elsz = self.page_element_size();
        let a = self.node();
        for i in 0..a.inodes.len() {
            let item = a.inodes.get(i).unwrap();
            sz += elsz + item.key.len() + item.value.len();
        }
        sz
    }

    fn page_element_size(&self) -> usize {
        if self.node().is_leaf {
            return LeafPageElementSize;
        }
        BranchPageElementSize
    }

    pub(crate) fn print(&self, db: &DBImpl) {
        println!("node pgid:{}", self.node().pgid);
        for n in self.node().inodes.iter() {
            print!(
                "flags:{},key:{:?},value:{:?},pgid:{} || ",
                n.flags, n.key, n.value, n.pgid
            );
            println!("");
            if n.flags & BucketLeafFlag as u32 != 0 {
                let ibucket = crate::u8_to_struct::<IBucket>(n.value.as_slice());
                let p = unsafe { &*db.page(ibucket.root) };
                let mut node = NodeImpl::new().build();
                node.read(p);
                node.print(db);
            }
        }
        println!("");
    }

    pub(crate) fn read(&mut self, p: &Page) {
        let mut node_mut = self.node_mut();
        node_mut.pgid = p.id;
        println!("p.flags:{}", p.flags);
        node_mut.is_leaf = (p.flags & LeafPageFlag) != 0;
        let count = p.count as usize;
        node_mut.inodes = Vec::with_capacity(count);
        for i in 0..count {
            let mut inode = INode::new();
            println!("is_leaf:{}", node_mut.is_leaf);
            if node_mut.is_leaf {
                let elem = p.leaf_page_element(i);
                inode.flags = elem.flags;
                inode.key = elem.key().to_vec();
                inode.value = elem.value().to_vec();
            } else {
                let elem = p.branch_page_element(i);
                inode.pgid = elem.pgid;
                inode.key = elem.key().to_vec();
            }
            assert!(inode.key.len() > 0, "read: zero-length inode key");
            node_mut.inodes.push(inode);
        }

        if node_mut.inodes.len() > 0 {
            let key = { node_mut.inodes.first().unwrap().key.clone() };
            node_mut.key = Some(key);
        } else {
            node_mut.key = None
        }
    }

    pub(crate) fn del(&mut self, key: &[u8]) {
        let (exact, index) = {
            match self
                .node()
                .inodes
                .binary_search_by(|inode| inode.key.as_slice().cmp(key))
            {
                Ok(v) => (true, v),
                Err(e) => (false, e),
            }
        };
        if !exact {
            return;
        }
        self.node_mut().inodes.remove(index);
        self.node_mut().unbalanced = true;
    }

    pub(crate) fn put(
        &mut self,
        old_key: &[u8],
        new_key: &[u8],
        value: &[u8],
        pgid: Pgid,
        flags: u32,
    ) {
        // if pgid > bucket.tx().unwrap().meta.borrow().pgid {
        //     panic!(
        //         "pgid {} above high water mark {}",
        //         pgid,
        //         bucket.tx().unwrap().meta.borrow().pgid,
        //     )
        // } else
        if old_key.len() <= 0 {
            panic!("put: zero-length old key")
        } else if new_key.len() <= 0 {
            panic!("put: zero-length new key")
        }
        let (exact, index) = {
            match self
                .node()
                .inodes
                .binary_search_by(|inode| inode.key.as_slice().cmp(old_key))
            {
                Ok(v) => (true, v),
                Err(e) => (false, e),
            }
        };
        let mut n1 = self.node_mut();
        if !exact {
            n1.inodes.insert(index, INode::new());
        }
        let inode = n1.inodes.get_mut(index).unwrap();
        inode.flags = flags;
        inode.key = new_key.to_vec();
        inode.value = value.to_vec();
        inode.pgid = pgid;
        assert!(inode.key.len() > 0, "put: zero-length inode key")
    }

    pub(crate) fn write(&self, p: &mut Page) {
        if self.node().is_leaf {
            p.flags = LeafPageFlag;
        } else {
            p.flags = BranchPageFlag;
        }
        if self.node().inodes.len() > 0xFFF {
            panic!(
                "inode overflow: {} (pgid={})",
                self.node().inodes.len(),
                p.id
            );
        }
        p.count = self.node().inodes.len() as u16;
        if p.count == 0 {
            return;
        }

        let mut buf_ptr = unsafe {
            p.data_ptr_mut()
                .add(self.page_element_size() * self.node().inodes.len())
        };

        for (i, item) in self.node().inodes.iter().enumerate() {
            assert!(item.key.len() > 0, "write: zero-length inode key");
            if self.node().is_leaf {
                let elem = p.leaf_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem.as_ptr() as usize) } as u32;
                elem.flags = item.flags as u32;
                elem.ksize = item.key.len() as u32;
                elem.vsize = item.value.len() as u32;
            } else {
                let elem = p.branch_page_element_mut(i);
                elem.pos = unsafe { buf_ptr.sub(elem.as_ptr() as usize) } as u32;
                elem.ksize = item.key.len() as u32;
                elem.pgid = item.pgid;
                assert!(elem.pgid != p.id, "write: circular dependency occurred");
            }
            let (klen, vlen) = (item.key.len(), item.value.len());
            unsafe {
                std::ptr::copy_nonoverlapping(item.key.as_ptr(), buf_ptr, klen);
                buf_ptr = buf_ptr.add(klen);
                std::ptr::copy_nonoverlapping(item.value.as_ptr(), buf_ptr, vlen);
                buf_ptr = buf_ptr.add(vlen);
            }
        }
    }

    pub(crate) fn root(&self, node: Node) -> Node {
        if let Some(parent_node) = &self.node().parent {
            let p = parent_node.upgrade().map(Node).unwrap();
            p.root(p.clone())
        } else {
            node
        }
    }

    fn child_index(&self, key: &[u8]) -> usize {
        match self
            .node()
            .inodes
            .binary_search_by(|inode| inode.key.as_slice().cmp(key))
        {
            Ok(v) => v,
            Err(e) => e,
        }
    }

    fn parent(&self) -> Option<Node> {
        match &self.node().parent {
            None => None,
            Some(p) => p.upgrade().map(Node),
        }
    }

    fn next_sibling(&self, bucket: &mut Bucket) -> Option<Node> {
        match self.parent() {
            None => None,
            Some(mut p) => {
                let index = self.child_index(self.node().key.as_ref().unwrap());
                if index > self.node().children.len() - 1 {
                    return None;
                }
                Some(p.child_at(bucket, index + 1, Some(Rc::downgrade(&p.0))))
            }
        }
    }

    fn prev_sibling(&mut self, bucket: &mut Bucket) -> Option<Node> {
        match self.parent() {
            None => None,
            Some(mut p) => {
                let index = self.child_index(self.node().key.as_ref().unwrap());
                if index == 0 {
                    return None;
                }
                Some(p.child_at(bucket, index - 1, Some(Rc::downgrade(&p.0))))
            }
        }
    }

    pub(crate) fn free(&mut self, bucket: &Bucket) {
        if self.node().pgid != 0 {
            let tx = bucket.tx().unwrap();
            let db = tx.db();
            db.freelist
                .try_write()
                .unwrap()
                .free(bucket.tx().unwrap().meta.borrow().txid, unsafe {
                    &*db.page(self.node().pgid)
                });
            self.node_mut().pgid = 0;
        }
    }

    fn num_children(&self) -> usize {
        self.node().inodes.len()
    }

    fn remove_child(&mut self, target: Node) {
        let index = self
            .node()
            .children
            .iter()
            .position(|c| Rc::ptr_eq(&target.0, &c.0));
        if let Some(i) = index {
            self.node_mut().children.remove(i);
        }
    }

    //删除元素 重平衡
    pub(crate) fn rebalance(&mut self, page_size: usize, b: *const Bucket) -> NKResult<()> {
        let bucket = unsafe { &mut *(b as *mut Bucket) };
        if !self.node_mut().unbalanced {
            return Ok(());
        }
        self.node_mut().unbalanced = false;
        let threshold = page_size / 4;
        if self.size() > threshold && self.node().inodes.len() > self.min_keys() {
            return Ok(());
        }
        //当前节点是根节点，特殊处理
        if self.parent().is_none() {
            //当前节点是branch节点并且只有一个一个inode, 分裂当前节点
            if !self.node().is_leaf && self.node().inodes.len() == 1 {
                // 将root节点的叶子节点上移
                // 创建一个新的子节点，以当前节点作为root节点
                let mut child =
                    bucket.node(self.node().inodes[0].pgid, Some(Rc::downgrade(&self.0)));

                let mut node_mut = self.node_mut();
                node_mut.is_leaf = child.node().is_leaf;
                node_mut.inodes = child.node_mut().inodes.drain(..).collect();
                node_mut.children = child.node_mut().children.drain(..).collect();
                //删除老得叶子节点
                child.node_mut().parent = None;
                bucket.nodes.borrow_mut().remove(&child.node().pgid);
                child.free(bucket);
            }
            return Ok(());
        }
        let mut p = self.parent().unwrap();
        if self.num_children() == 0 {
            if let Some(k) = &self.node().key {
                p.del(k);
            }
            p.remove_child(self.clone());
            bucket.nodes.borrow_mut().remove(&self.node().pgid);
            self.free(bucket);
            p.rebalance(page_size, bucket)?;
        }

        let use_next_sibing = (p.child_index(self.node().key.as_ref().unwrap()) == 0);
        let mut target = if use_next_sibing {
            self.next_sibling(bucket).unwrap()
        } else {
            self.prev_sibling(bucket).unwrap()
        };

        if use_next_sibing {
            for inode in target.node().inodes.iter() {
                if let Some(child) = bucket.nodes.borrow_mut().get_mut(&inode.pgid) {
                    child.parent().unwrap().remove_child(child.clone());
                    child.node_mut().parent = Some(Rc::downgrade(&self.0));
                    child
                        .parent()
                        .unwrap()
                        .node_mut()
                        .children
                        .push(child.clone());
                }
            }

            let mut p = self.parent().unwrap();
            self.node_mut()
                .inodes
                .append(&mut target.node_mut().inodes.drain(..).collect::<Vec<INode>>());
            p.del(target.node().key.as_ref().unwrap());
            p.remove_child(target.clone());
            bucket.nodes.borrow_mut().remove(&target.node().pgid);
            target.free(bucket);
        } else {
            {
                for inode in self.node().inodes.iter() {
                    if let Some(child) = bucket.nodes.borrow_mut().get_mut(&inode.pgid) {
                        child.parent().unwrap().remove_child(child.clone());
                        child.node_mut().parent = Some(Rc::downgrade(&target.0));
                        child
                            .parent()
                            .unwrap()
                            .node_mut()
                            .children
                            .push(child.clone());
                    }
                }
            }
            let mut p = self.parent().unwrap();
            target
                .node_mut()
                .inodes
                .append(&mut self.node_mut().inodes.drain(..).collect::<Vec<INode>>());
            p.del(self.node().key.as_ref().unwrap());
            p.remove_child(self.clone());
            bucket.nodes.borrow_mut().remove(&self.node().pgid);
            self.free(bucket);
        }
        self.parent().unwrap().rebalance(page_size, b)
    }

    //添加元素 分裂
    fn split(&mut self, page_size: usize, fill_percent: f64) -> Vec<Node> {
        let mut nodes = vec![self.clone()];
        let mut node = self.clone();
        while let Some(b) = node.split_two(page_size, fill_percent) {
            nodes.push(b.clone());
            node = b;
        }
        nodes
    }

    fn split_index(&self, threshold: usize) -> (usize, usize) {
        let mut index: usize = 0;
        let mut sz: usize = 0;
        let n = self.node();
        let max = n.inodes.len() - MIN_KEY_PERPAGE;
        let nodes = &n.inodes;
        for (i, node) in nodes.iter().enumerate().take(max) {
            index = i;
            let elsize = self.page_element_size() + node.key.len() + node.value.len();
            if i > MIN_KEY_PERPAGE && sz + elsize > threshold {
                break;
            }
            sz += elsize;
        }
        (index, sz)
    }

    fn split_two(&mut self, page_size: usize, mut fill_percent: f64) -> Option<Node> {
        if self.node().inodes.len() <= MIN_KEY_PERPAGE * 2 || self.node_less_than(page_size) {
            return None;
        }
        println!();
        if fill_percent < MIN_FILL_PERCENT {
            fill_percent = MIN_FILL_PERCENT;
        } else if fill_percent > MAX_FILL_PERCENT {
            fill_percent = MAX_FILL_PERCENT;
        }
        let threshold = (page_size as f64 * fill_percent) as usize;
        let (split_index, _) = self.split_index(threshold);

        let mut next = NodeImpl::new().leaf(self.node().is_leaf).build();
        next.node_mut().inodes = self.node_mut().inodes.drain(split_index..).collect();
        Some(next)
    }

    fn node_less_than(&self, v: usize) -> bool {
        let mut sz = Page::header_size();
        let elsz = self.page_element_size();
        let a = self.node();
        for i in 0..a.inodes.len() {
            let item = a.inodes.get(i).unwrap();
            sz += elsz + item.key.len() + item.value.len();
            if sz >= v {
                return false;
            }
        }
        return true;
    }

    //node spill return parent
    pub(crate) fn spill(&mut self, atx: Arc<TxImpl>, bucket: &Bucket) -> NKResult<Node> {
        if self.node().spilled {
            return Ok(self.clone());
        }

        self.node_mut()
            .children
            .sort_by(|a, b| (*a).node().inodes[0].key.cmp(&(*b).node().inodes[0].key));
        for mut child in self.node_mut().children.clone() {
            child.spill(atx.clone(), bucket)?;
        }

        self.node_mut().children.clear();
        let tx = atx.clone();
        let db = tx.db();

        let mut nodes = self.split(db.get_page_size() as usize, bucket.fill_percent);

        // 这里设置父节点信息
        let parent_node = if nodes.len() == 1 {
            None
        } else {
            if let Some(parent) = &self.node().parent {
                let mut p = parent.upgrade().map(Node).unwrap();
                for n in nodes.iter_mut() {
                    n.node_mut().parent = Some(parent.clone());
                }
                p.node_mut().children.extend_from_slice(&nodes[1..]);
                Some(p)
            } else {
                let mut parent = NodeImpl::new().leaf(false).build();
                parent
                    .node_mut()
                    .children
                    .extend_from_slice(nodes.as_slice());
                Some(parent)
            }
        };
        for n in nodes.iter_mut() {
            if n.node().pgid > 0 {
                db.freelist
                    .try_write()
                    .unwrap()
                    .free(tx.meta.borrow().txid, unsafe { &*db.page(n.node().pgid) });
                n.node_mut().pgid = 0;
            }

            let mut p = db.allocate(n.size() / db.get_page_size() as usize + 1)?;
            let page = p.to_page_mut();
            if page.id >= tx.meta.borrow().pgid {
                panic!(
                    "pgid {} above high water mark{}",
                    page.id,
                    tx.meta.borrow().pgid
                );
            }
            n.node_mut().pgid = page.id;

            n.write(page);
            println!(
                "xxxx,pgid:{},flag:{},is_leaf:{}",
                page.id,
                page.flags,
                n.node().is_leaf
            );
            tx.pages.borrow_mut().insert(page.id, p);
            n.node_mut().spilled = true;

            if let Some(parent) = &n.node().parent {
                let mut parent_node = parent.upgrade().map(Node).unwrap();
                if let Some(key) = &n.node().key {
                    parent_node.put(key, key, &vec![], n.node().pgid, 0);
                } else {
                    let n1 = n.node();
                    let inode = n1.inodes.first().unwrap();
                    parent_node.put(&inode.key, &inode.key, &vec![], n.node().pgid, 0);
                }
            }
        }

        if let Some(mut p) = parent_node {
            p.node_mut().children.clear();
            return p.spill(atx, bucket);
        }
        return Ok(self.clone());
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct INode {
    pub(crate) flags: u32,
    pub(crate) pgid: Pgid,
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

impl INode {
    fn new() -> INode {
        Self {
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ptr::null_mut;

    use crate::db::MmapUtil;

    use super::*;
    #[test]
    fn test_node_new() {
        let mut buf = vec![0u8; 512];
        let mmap = MmapUtil::default();
        let mut node1 = NodeImpl::new().leaf(true).build();
        node1.put(b"aaa", b"aaa", b"001", 0, 0);
        node1.put(b"bbb", b"bbb", b"002", 0, 0);
        let page = mmap.page_in_buffer_mut(&mut buf, 0);
        node1.write(page);
        let mut node2 = NodeImpl::new().leaf(true).build();
        node2.read(page);
        for n in node2.node().inodes.iter() {
            print!(
                "flags:{},key:{:?},value:{:?},pgid:{} || ",
                n.flags, n.key, n.value, n.pgid
            );
        }
    }
}
