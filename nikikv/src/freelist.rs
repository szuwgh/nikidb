use crate::page::Pgid;

pub(crate) struct FreeList {}

impl FreeList {
    pub(crate) fn free() {}

    pub(crate) fn allocate(n: u32) -> Pgid {
        0
    }
}
