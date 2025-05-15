// supress warnings to de-clutter terminal output
#![allow(unused_variables)]
#![allow(dead_code)]

//! Phase 1: In-memory LSM Tree
//! Objective: Add necessary field and method implementations for an in-memory LSM Tree and make the test below pass.

pub struct LSMTree {
    // add a field `memtable` here which is a btree map of String as keys and Option<String> as values
}

impl LSMTree {
    // creates a new instance of LSM Tree
    pub fn new() -> Self {
        // TODO: Initialize the `memtable` field with sane defaults.
        todo!()
    }

    // add k and v into the memtable
    pub fn put(&mut self, k: &str, v: &str) {
        // TODO: insert the given key k and value v to the memtable
        todo!()
    }

    // return the value associated with the given key
    pub fn get(&self, k: &str) -> Option<String> {
        // TODO: retrieve the value for given k from memtable
        todo!()
    }

    // deletes the value associated with the given key `k`
    // NOTE: deletes are just a put in disguise in an LSM Tree, with None as the value in this implementation.
    pub fn delete(&mut self, k: &str) {
        // TODO: set the value to None for the given key in memtable.
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use crate::LSMTree;

    // TODO: make this test pass
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
