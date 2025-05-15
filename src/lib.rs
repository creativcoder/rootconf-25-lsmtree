// supress warnings to de-clutter terminal output
#![allow(unused_variables)]
#![allow(dead_code)]

//! Phase 2: Flushing memtable to SSTable files on reaching memtable size limit.
//!
//! Objective: Add necessary field and method implementations for flushing the memtable and make the test `test_lsm_trigger_flush_basic` below pass.
//!
//! Explanation: We want to support flushing memtable to SSTables when they hit a size limit. For this our LSMTree struct, needs a new struct SSTableManager
//! defined below and size limit field. SSTableManager will contain APIs to track the created SSTable files and assigns ids to newly created files.
//! It has more functionality that will be added in next phases.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    path::PathBuf,
};

// This is a byte marker used to denote a deletion in LSM Tree SSTable files.
// 💡 Actual implementations use something different, like a 0x01 (in rocksdb and leveldb)
const TOMBSTONE_MARKER: char = '🪦';

pub struct LSMTree {
    memtable: BTreeMap<String, Option<String>>,
    // TODO: add a field memtable_limit that stores the max amount of items to be kept in memtable. Hint: use `usize` as the data type.

    // TODO: add a field called `sstable_mgr` of type SSTableManager. Look below for a struct named `SStableManager`
}

impl LSMTree {
    // creates a new instance of LSM Tree
    pub fn new() -> Self {
        // TODO: declare a variable `data_dir` which is a PathBuf object with path as "data" (in the current directory). Look for `PathBuf` type in rust stdlib.
        // the "data" directory is where we'll store our sstables.

        // TODO: create the "data" directory if it doesn't exist using the PathBuf object above. hint: lookup `create_dir` in rust stdlib

        Self {
            memtable: BTreeMap::new(),
            // TODO: add the initialization of memtable_limit field from the updated struct definition above.
            // Hint: keep a memtable size limit of 10 to start with.

            // TODO: initialize SSTableManager (the new() method)
            // passing `data_dir` (a PathBuf) to it as a reference.
        }
    }

    // add k and v into the memtable
    pub fn put(&mut self, k: &str, v: &str) {
        self.memtable.insert(k.to_string(), Some(v.to_string()));
        // TODO: check if memtable size threshold is reached, and perform `memtable_flush` if true.
        // Hint: you can check the size of the BtreeMap. Look for rust std lib docs.
    }

    // return the value associated with the given key
    pub fn get(&self, k: &str) -> Option<String> {
        match self.memtable.get(k) {
            Some(Some(v)) => return Some(v.to_string()),
            Some(None) | None => return None,
        }
    }

    // deletes the value associated with the given key `k`
    // NOTE: deletes are just a put in disguise in an LSM Tree, with `None` as the value in this case.
    pub fn delete(&mut self, k: &str) {
        self.memtable.insert(k.to_string(), None);
    }

    // flushes the memtable contents to a file
    fn flush_memtable(&mut self) {
        // TODO: check if memtable is already empty, if true, return.

        // TODO: get a new sstable file and its id by calling method `new_sstable` on SSTableManager in LSMTree struct. Implement `new_sstable` method below.
        // hint: let (mut sst_file, sst_id) = ...

        // TODO: write all key value pairs from memtable to the new sstable file each on a new line as: "k:v" (where k: key and v: value) followed by a newline (\n)
        // for the deleted key, write the value as TOMBSTONE_MARKER. As we don't delete keys in a LSM Tree. They are handled later by compaction.

        // TODO: ensure file is fully synced and flushed in OS file system buffers.
        // Hint: see methods on File instance: https://doc.rust-lang.org/std/fs/struct.File.html

        // TODO: clear the memtable, as our data is in file now.

        // TODO: add the new sstable id by calling `add_sstable` method on SSTableManager, so we have track of it in our LSM instance.

        // TODO: remove the todo!() below
        todo!()
    }
}

// A convenient wrapper struct that manages SSTables and issues new file ids to newly created SSTable files.
struct SSTableManager {
    // Directory where the sstables resides.
    data_dir: PathBuf,
    // a naive incrementing counter for file ids. 💡 Actual implementations use a combination of timestamp and unique identifiers.
    next_sstable_id: usize,
    // A list of sstables created in the past.
    sstables: VecDeque<usize>,
}

impl SSTableManager {
    pub fn new(path_buf: &PathBuf) -> Self {
        SSTableManager {
            data_dir: path_buf.clone(),
            next_sstable_id: 0,
            sstables: VecDeque::new(),
        }
    }

    pub fn new_sstable(&mut self) -> (File, usize) {
        // TODO: increment the next_sstable_id by 1

        // TODO: create a file with create, write and append mode using the incremented file id. The file should be inside data/ and named as x.sst where x is the
        // incremented file id - i.e., data/1.sst
        // hint: read up on `OpenOptions` from rust std lib on how to create a file.

        // TODO: return created file and the id as a 2 element tuple (foo, bar)

        // TODO: remove the todo!() below
        todo!()
    }

    // Adds the give sstable id to the queue of sstables.
    pub fn add_sstable(&mut self, id: usize) {
        // TODO: add the given `id` to `sstables` queue

        // TODO: remove the todo!() below
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::LSMTree;

    // a help function to reset `data`` directory for tests.
    fn clear_data_dir() {
        let data_dir = PathBuf::from("data");
        if data_dir.exists() {
            std::fs::remove_dir_all("data").unwrap();
        }
    }

    #[test]
    fn test_lsm_basic_crud() {
        let mut lsmtree = LSMTree::new();
        lsmtree.put("hello", "world");
        lsmtree.put("foo", "bar");
        lsmtree.delete("hello");
        assert!(lsmtree.get("foo").unwrap() == "bar");
        assert!(lsmtree.get("hello").is_none());
    }

    // TODO: make this pass
    #[test]
    fn test_lsm_trigger_flush_basic() {
        clear_data_dir();
        let mut lsmtree = LSMTree::new();
        lsmtree.put("a", "v1");
        lsmtree.flush_memtable();
        assert!(std::fs::exists("data/1.sst").unwrap());
    }
}
