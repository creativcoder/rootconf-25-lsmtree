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
    io::Write,
    path::PathBuf,
};

use std::fmt::Write as _;

// This is a byte marker used to denote a deletion in LSM Tree SSTable files.
// 💡 Actual implementations use something different, like a 0x01 (in rocksdb and leveldb)
const TOMBSTONE_MARKER: char = '🪦';

pub struct LSMTree {
    memtable: BTreeMap<String, Option<String>>,
    memtable_limit: usize,
    sstable_mgr: SSTableManager,
}

impl LSMTree {
    // creates a new instance of LSM Tree
    pub fn new() -> Self {
        let data_dir = PathBuf::from("data");

        if !data_dir.exists() {
            std::fs::create_dir(&data_dir).unwrap();
        }

        Self {
            memtable: BTreeMap::new(),
            memtable_limit: 10,

            sstable_mgr: SSTableManager::new(&data_dir),
        }
    }

    // add k and v into the memtable
    pub fn put(&mut self, k: &str, v: &str) {
        self.memtable.insert(k.to_string(), Some(v.to_string()));
        if self.memtable.len() == self.memtable_limit {
            self.flush_memtable();
        }
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
        if self.memtable.is_empty() {
            return;
        }

        let (mut sst_file, sst_id) = self.sstable_mgr.new_sstable();

        for (k, v) in &self.memtable {
            let mut line = String::new();
            match v {
                Some(v) => {
                    writeln!(&mut line, "{}:{}", k, v).unwrap();
                    sst_file.write_all(line.as_bytes()).unwrap();
                }
                None => {
                    writeln!(&mut line, "{}:{}", k, TOMBSTONE_MARKER).unwrap();
                    sst_file.write_all(line.as_bytes()).unwrap();
                }
            }
        }

        sst_file.sync_data().unwrap();

        self.memtable.clear();

        self.sstable_mgr.add_sstable(sst_id);
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
        self.next_sstable_id += 1;

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(self.data_dir.join(&format!("{}.sst", self.next_sstable_id)))
            .unwrap();

        (file, self.next_sstable_id)
    }

    // Adds the give sstable id to the queue of sstables.
    pub fn add_sstable(&mut self, id: usize) {
        self.sstables.push_back(id);
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

    #[test]
    fn test_lsm_trigger_flush_basic() {
        clear_data_dir();
        let mut lsmtree = LSMTree::new();
        lsmtree.put("a", "v1");
        lsmtree.flush_memtable();
        assert!(std::fs::exists("data/1.sst").unwrap());
    }
}
