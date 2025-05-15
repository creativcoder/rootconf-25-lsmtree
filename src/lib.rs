// supress warnings to de-clutter terminal output
#![allow(unused_variables)]
#![allow(dead_code)]

//! Phase 3: Reading data from sstables too, recovery of sstables on restart.
//!
//! Objective: Add necessary field and method implementations for reading from sstables and ensure sstables are recovered when creating a new LSMTree instance
//! when a data dir already present from previous session.
//! Make `test_lsm_reads_from_sstable` and `test_lsm_recovers_and_reads_older_sstables` below pass.
//!
//! Explanation: Once the sstables are flushed to disk, the LSM Tree should be able to read data from those files as well if a key isn't found in the memtable
//! and if there's sstable files in the data directory.

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufReader, Write},
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

        let mut sstable_mgr = SSTableManager::new(&data_dir);
        // TODO: call `recover()` method on `sstable_mgr` here to load any existing older sstable files

        Self {
            memtable: BTreeMap::new(),
            memtable_limit: 10,
            sstable_mgr,
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
            // TODO: split this match into separate arms
            // hint: for Some(None) case, simply return `None` as a delete was performed in the memtable itself.
            Some(None) | None => return None,
            // TODO: in the bare `None` case, we have to read from sstables because the key `k` might reside in one of the sstables.
            // hint: iterate over all sstable files.
            // hint: order of iteration (most recent first) is important to avoid reading old version of values for the given key `k`.

            // TODO: within the loop, call `get_sstable` on SSTableManager passing the file id and the key. Finish impl of `get_sstable` below on SSTableManager.
        }

        // TODO: add a None as a fallback return type.
    }

    // deletes the value associated with the given key `k`
    // NOTE: deletes are just a put in disguise in an LSM Tree, with None as the value in this case.
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

    // retrieves the given key `k` from the list of sstables.
    pub fn get_sstable(&self, sst_file_id: usize, key: &str) -> Option<String> {
        // TODO: open the file in the data dir with the given `sst_file_id` in read mode. Hint: use OpenOptions type from rust stdlib.

        // TODO: wrap this file in a BufReader to be able to read the file line by line. Hint: search BufReader in rust stdlib

        // TODO: for each line, call `read_kv_line` helper method below passing a reference to the line to get (k, v)
        // hint: let (k,v) = read_kv_..

        // TODO: perform this check: if key matches k and if the value is TOMBSTONE_MARKER return a None else return the value v cloned and wrapped in Some().

        // TODO: remove the todo!() below and replace with a None as a fallback return type
        todo!()
    }

    // recovers the ids of sstables from the data dir.
    fn recover(&mut self) {
        // We're using the helper function `files_with_extension` to get sstables file list, else initializing
        // with an empty vec.
        let old_sst_ids = if let Ok(old_sst_files) = files_with_extension(&self.data_dir, "sst") {
            let mut files: Vec<usize> = old_sst_files
                .map(|p| {
                    p.display()
                        .to_string()
                        .trim_end_matches(".sst")
                        .trim_start_matches("data/")
                        .parse()
                        .unwrap()
                })
                .collect();
            // smaller ids at first, being the oldest.
            files.sort();
            files
        } else {
            vec![]
        };

        // TODO: uncomment the line below to store the older files, thereby loading all previous file ids available for reads.
        // self.sstables = VecDeque::from(old_sst_ids)
    }
}

// helper function to read a line of key value pair from the sstable.
fn read_kv_line(l: &Result<String, std::io::Error>) -> (String, String) {
    let line = l.as_ref().unwrap();
    let mut kv = line.split(":");
    let k = kv.next().unwrap();
    let v = kv.next().unwrap();
    (k.to_string(), v.to_string())
}

// returns an iterator of files in the given `dir_path` with the given `extension`
pub fn files_with_extension(
    dir_path: &PathBuf,
    extension: &str,
) -> std::io::Result<impl Iterator<Item = PathBuf>> {
    // NOTE: read_dir doesn't guarantee same sorted order.
    let entries = std::fs::read_dir(dir_path)?;
    let ext = extension.to_string();

    let iter = entries.filter_map(move |entry| {
        let entry = entry.ok()?;
        let path = entry.path();

        if path.is_file() && path.extension()?.to_str()? == ext {
            Some(path)
        } else {
            None
        }
    });

    Ok(iter)
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

    // TODO: make this pass
    // run with `cargo test test_lsm_reads_from_sstable`
    #[test]
    fn test_lsm_reads_from_sstable() {
        let mut lsmtree = LSMTree::new();
        lsmtree.put("hello", "world");
        lsmtree.put("foo", "bar");
        lsmtree.delete("hello");
        // force flush memtable so reads can happen from sstable.
        lsmtree.flush_memtable();
        assert!(lsmtree.get("hello").is_none());
        assert!(lsmtree.get("foo").unwrap() == "bar");
    }

    // TODO: make this pass
    // run with cargo test `test_lsm_recovers_and_reads_older_sstables`
    #[test]
    fn test_lsm_recovers_and_reads_older_sstables() {
        let mut lsmtree = LSMTree::new();
        lsmtree.put("hello", "world");
        lsmtree.put("foo", "bar");
        lsmtree.delete("hello");
        lsmtree.flush_memtable();
        drop(lsmtree);
        // re-initialize another LSMTree instance.
        let mut lsmtree = LSMTree::new();
        // confirm that memtable is empty on a new instance.
        assert!(lsmtree.memtable.is_empty());
        assert!(lsmtree.get("hello").is_none());
        assert!(lsmtree.get("foo").unwrap() == "bar");
    }
}
