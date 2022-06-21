use crate::{
    error::NKResult,
    page::{FreeListPageFlag, Page, Pgid},
    tx::Txid,
};
use std::mem::size_of;
use std::{collections::HashMap, future::pending};

pub(crate) struct FreeList {
    pub(crate) ids: Vec<Pgid>,
    pub(crate) pending: HashMap<Txid, Vec<Pgid>>,
    pub(crate) cache: HashMap<Pgid, bool>,
}

impl Default for FreeList {
    fn default() -> Self {
        Self {
            ids: Vec::new(),
            pending: HashMap::new(),
            cache: HashMap::new(),
        }
    }
}

impl FreeList {
    pub(crate) fn size(&self) -> usize {
        let mut count = self.count();
        if count > 0xFFFF {
            count += 1;
        }
        return Page::header_size() + size_of::<Pgid>() * count;
    }

    fn count(&self) -> usize {
        self.pending_count() + self.free_count()
    }

    fn free_count(&self) -> usize {
        self.ids.len()
    }

    fn pending_count(&self) -> usize {
        self.pending.iter().map(|x| x.1.len()).sum()
    }

    pub(crate) fn rollback() -> NKResult<()> {
        Ok(())
    }

    // 该接口主要用于写事务提交之前释放已占用page。
    // 将待释放的page id加入到pending和cache中。
    // 如果待释放的page的overflow大于零，则对其关联的其他page做同样的处理。
    pub(crate) fn free(&mut self, txid: Txid, p: &Page) {
        if p.id <= 1 {
            panic!("cannot free page 0 or 1: {}", p.id);
        }
        let ids = self.pending.entry(txid).or_insert(Vec::new());
        for id in p.id..=p.id + p.overflow as Pgid {
            if self.cache.contains_key(&id) {
                panic!("page {} already freed", id);
            }

            ids.push(id);
            self.cache.insert(id, true);
        }
    }

    // 从freelist中的空闲page中寻找n个page id连续的page。如果分配成功，
    // 说明被分配的pages已经被占用，则将其从空闲page列表和cache中清除，
    // 并返回起始page id。如果分配失败，则返回零
    pub(crate) fn allocate(&mut self, n: usize) -> Pgid {
        if self.ids.len() == 0 {
            return 0;
        }
        println!(
            "allocate freelist ids->:{:?},size:{},peeding:{:?}",
            self.ids, n, self.pending,
        );
        let mut initial: Pgid = 0;
        let mut previd: Pgid = 0;
        let item = self.ids.iter().enumerate().position(|(_i, _id)| {
            let id = *_id;
            if id <= 1 {
                panic!("invalid page allocation: {}", id);
            }
            if previd == 0 || id - previd != 1 {
                initial = id;
            }
            if (id - initial) + 1 == n as Pgid {
                return true;
            }
            previd = id;
            false
        });
        return match item {
            Some(index) => {
                self.ids.drain(index - (n - 1)..index + 1);
                for i in 0..n {
                    self.cache.remove(&(initial + (i as Pgid)));
                }
                initial
            }
            None => 0,
        };
    }

    pub(crate) fn read(&mut self, p: &Page) {
        let mut idx: usize = 0;
        let mut count = p.count as usize;
        if count == 0xFFFF {
            idx = 1;
            count = *p.freelist().first().unwrap() as usize;
        }
        if count == 0 {
            self.ids.clear();
        } else {
            let ids = p.freelist();
            self.ids = ids[idx..count].to_vec();
            self.ids.sort_unstable();
        }
        self.reindex();
    }

    pub(crate) fn reindex(&mut self) {
        let mut new_cache: HashMap<Pgid, bool> = HashMap::new();
        for id in self.ids.iter() {
            new_cache.insert(*id, true);
        }
        for (_key, ids) in self.pending.iter() {
            for id in ids.iter() {
                new_cache.insert(*id, true);
            }
        }
        self.cache = new_cache;
    }

    pub(crate) fn write(&self, p: &mut Page) {
        p.flags |= FreeListPageFlag;

        let count = self.count();
        if count == 0 {
            p.count = count as u16;
        } else if count < 0xFFFF {
            p.count = count as u16;
            let m = p.freelist_mut();
            self.copy_all(m);
            m.sort_unstable();
        } else {
            p.count = 0xFFFF;
            let m = p.freelist_mut();
            m[0] = count as u64;
            self.copy_all(&mut m[1..]);
            m[1..].sort_unstable();
        }
    }

    pub(crate) fn copy_all(&self, mut dst: &mut [Pgid]) {
        let mut m: Vec<Pgid> = Vec::with_capacity(self.pending_count());
        for list in self.pending.values() {
            dst[..list.len()].copy_from_slice(list);
            dst = &mut dst[list.len()..];
        }
        dst[..self.ids.len()].copy_from_slice(self.ids.as_slice())
    }

    pub(crate) fn release(&mut self, txid: Txid) {
        let mut m: Vec<Pgid> = Vec::new();
        let mut remove_txid: Vec<Txid> = Vec::new();
        for (tid, ids) in self.pending.iter() {
            if *tid < txid {
                m.extend_from_slice(ids);
                remove_txid.push(*tid);
            }
        }
        for txid in remove_txid {
            self.pending.remove(&txid);
        }
        m.sort_unstable();
        self.ids = merge_pgids(self.ids.as_slice(), &m);
    }
}

pub(crate) fn merge_pgids(a: &[Pgid], b: &[Pgid]) -> Vec<Pgid> {
    let mut dst = Vec::with_capacity(a.len() + b.len());
    dst.extend(a);
    dst.extend(b);
    dst.sort_unstable();
    dst
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    #[test]
    fn test_freelist_allocate() {
        // let ids: Vec<Pgid> = vec![
        //     2, 3, 6, 7, 8, 10, 12, 13, 14, 15, 17, 18, 20, 21, 22, 23, 24,
        // ];
        let ids: Vec<Pgid> = vec![2, 3];
        let mut freelist = FreeList {
            ids: ids,
            pending: HashMap::new(),
            cache: HashMap::new(),
        };
        let pgid = freelist.allocate(1);
        println!("pgid:{}", pgid);
        println!("ids:{:?}", freelist.ids);
    }

    #[test]
    fn test_count() {
        let ids: Vec<Pgid> = vec![2, 3, 6, 7, 5];

        let id1: Vec<Pgid> = vec![2, 3, 6, 7];
        let id2: Vec<Pgid> = vec![2, 3, 6, 7];

        let mut map: HashMap<Txid, Vec<Pgid>> = HashMap::new();

        map.insert(1, id1);
        map.insert(2, id2);

        let mut freelist = FreeList {
            ids: ids,
            pending: map,
            cache: HashMap::new(),
        };

        println!("free_count:{}", freelist.free_count());
        println!("pending_count:{:?}", freelist.pending_count());
    }

    #[test]
    fn test_copy_all() {
        let ids: Vec<Pgid> = vec![1, 2, 5, 6, 7];

        let id1: Vec<Pgid> = vec![3, 8];
        let id2: Vec<Pgid> = vec![9, 10];

        let mut map: HashMap<Txid, Vec<Pgid>> = HashMap::new();

        map.insert(1, id1);
        map.insert(2, id2);

        let mut freelist = FreeList {
            ids: ids,
            pending: map,
            cache: HashMap::new(),
        };
        let mut dst: Vec<Pgid> = vec![0; 10];
        freelist.copy_all(&mut dst);
        dst.sort_unstable();
        println!("dst:{:?}", dst);
    }
}
