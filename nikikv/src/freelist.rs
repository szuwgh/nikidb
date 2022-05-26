use std::collections::HashMap;

use crate::{page::{Pgid, Page}, tx::{Txid, self}};

pub(crate) struct FreeList {
    ids: Vec<Pgid>,
    pending: HashMap<Txid, Vec<Pgid>> ,  
    cache: HashMap<Pgid, bool>, 
}

impl FreeList {
    pub(crate) fn free(&mut self, txid: Txid,  p: &Page) {
        // 该接口主要用于写事务提交之前释放已占用page。
        // 将待释放的page id加入到pending和cache中。
        // 如果待释放的page的overflow大于零，则对其关联的其他page做同样的处理。
        if p.id <= 1 {
            panic!("cannot free page 0 or 1: {}", p.id);
        }
        let ids = self.pending.entry(txid).or_insert(Vec::new());
        for id in p.id..p.id+p.overflow as Pgid {  
            if self.cache.contains_key(&id) {
                panic!("page {} already freed", id);
            }
            ids.push(id);
            self.cache.insert(id, true);
        }
    }  

    pub(crate) fn allocate(n: usize) -> Pgid {
        0
    }
}
