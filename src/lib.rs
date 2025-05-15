// supress warnings to de-clutter terminal output
#![allow(unused_variables)]
#![allow(dead_code)]

//! Phase 1: In-memory LSM Tree
//! Objective: Add necessary field and method implementations for an in-memory LSM Tree and make the test below pass.

use std::collections::BTreeMap;

pub struct LSMTree {
    memtable: BTreeMap<String, Option<String>>,
}

impl LSMTree {
    // creates a new instance of LSM Tree
    pub fn new() -> Self {
        Self {
            memtable: BTreeMap::new(),
        }
    }

    // add k and v into the memtable
    pub fn put(&mut self, k: &str, v: &str) {
        self.memtable.insert(k.to_string(), v.to_string().into());
    }

    // return the value associated with the given key
    pub fn get(&self, k: &str) -> Option<String> {
        match self.memtable.get(k) {
            Some(Some(v)) => return Some(v.to_string()),
            Some(None) | None => return None,
        }
    }

    // deletes the value associated with the given key `k`
    // NOTE: deletes are just a put in disguise in an LSM Tree, with None as the value in this case.
    pub fn delete(&mut self, k: &str) {
        self.memtable.insert(k.to_string(), None);
    }
}

#[cfg(test)]
mod tests {
    use crate::LSMTree;

    #[test]
    fn test_lsm_basic_crud() {
        let mut lsmtree = LSMTree::new();
        lsmtree.put("hello", "world");
        lsmtree.put("foo", "bar");
        lsmtree.delete("hello");
        assert!(lsmtree.get("foo").unwrap() == "bar");
        assert!(lsmtree.get("hello").is_none());
    }
}
