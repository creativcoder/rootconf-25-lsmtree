// supress warnings to de-clutter terminal output
#![allow(unused_variables)]
#![allow(dead_code)]

//! Phase 4: Compaction on SSTables.
//!
//! Objective: Add necessary field and method implementations for compacting sstables
//! and make the tests below pass. Once compaction
//! method is implemented on SSTableManager, wire the method call within flush so that,
//! compaction is automatically invoked whenever
//! there's a sstable flush.
//!
//! Explanation: Over time the flushed SSTables starts to accumulate and would contain
//! stale entries of keys and deleted instances of keys as tombstone bytes.
//! In order to reclaim space and keep our LSM Tree read efficient, we need to perform
//! compaction on our sstables, which is simply removing
//! older values for keys in the sstable, and removing tombstone values of keys (older deleted values).

use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{BufRead, BufReader, Write},
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
        sstable_mgr.recover();

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
            Some(None) => return None,
            None => {
                for i in self.sstable_mgr.sstables.iter().rev() {
                    match self.sstable_mgr.get_sstable(*i, k) {
                        Some(v) => return Some(v.clone()),
                        None => {}
                    }
                }
            }
        }

        None
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
        // TODO: call compact() here on SSTableManager, as flushing adds a new file to data directory, possibly hitting compaction condition at one point.
    }

    // Performs compaction of sstables if compaction condition is triggered.
    fn compact(&mut self) {
        if self.sstable_mgr.should_compact() {
            self.sstable_mgr.compact_sstables();
        }
    }

    // helper for tests, that performs compaction, regardless of trigger condition.
    fn force_compact(&mut self) {
        self.sstable_mgr.compact_sstables();
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
    // used to check if compaction can be triggered - it's simply max count of files in the data directory.
    compaction_trigger: usize,
}

impl SSTableManager {
    pub fn new(path_buf: &PathBuf) -> Self {
        SSTableManager {
            data_dir: path_buf.clone(),
            next_sstable_id: 0,
            sstables: VecDeque::new(),
            compaction_trigger: 8,
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
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(self.data_dir.join(&format!("{}.sst", sst_file_id)))
            .unwrap();

        let mut buf_reader = BufReader::new(file);

        for l in buf_reader.lines() {
            let (k, v) = read_kv_line(&l);
            if k == key {
                if v == TOMBSTONE_MARKER.to_string() {
                    return None;
                } else {
                    return Some(v.to_string());
                }
            }
        }

        None
    }

    // recovers the ids of sstables from the data dir.
    fn recover(&mut self) {
        // We're using the helper function `files_with_extension` to get file list, else initializing
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

        self.sstables = old_sst_ids.into();
    }

    fn should_compact(&mut self) -> bool {
        // TODO: check if count of sstable files is equal to field `compaction_trigger`
        // TODO: remove the todo!() below
        todo!()
    }

    // Compacts sstables.
    // In this toy implementation, we only take the oldest two sstables and attempt to merge duplicates or deletes from them one by one, using the merge
    // algorithm from merge sort.
    // once that is done, we rename the merged file to the 2nd oldest file, remove the oldest file from the data directory
    // and pop remove the associated id of the file from the `sstables` queue
    fn compact_sstables(&mut self) {
        // bail early if we don't have enough required sstables to compact from.
        if self.sstables.len() < 2 {
            return;
        }

        // 1. pick the oldest two sstable and create a BufReader from them.
        let s1_path = self.data_dir.join(format!("{}.sst", self.sstables[0]));
        let sstable = std::fs::OpenOptions::new()
            .read(true)
            .open(&s1_path)
            .unwrap();
        let s1_buf = BufReader::new(sstable);

        let s2_path = self.data_dir.join(format!("{}.sst", self.sstables[1]));
        let sstable = std::fs::OpenOptions::new()
            .read(true)
            .open(&s2_path)
            .unwrap();
        let s2_buf = BufReader::new(sstable);

        // 2. create a lines iterator out of them
        let mut s1_lines = s1_buf.lines();
        let mut s2_lines = s2_buf.lines();

        // 3. create two variable thar points to first line from both the sstable files.
        let mut s1_next = s1_lines.next();
        let mut s2_next = s2_lines.next();

        // 4. create a merged map that will store the merged key and values from the two files.
        let mut merged_map: BTreeMap<String, String> = BTreeMap::new();
        // 5. loop over the cursor for both files and do a match and merge them into a single sstable comparing the keys.
        loop {
            match (&s1_next, &s2_next) {
                (Some(line_s1), Some(line_s2)) => {
                    let (s1_k, s1_v) = read_kv_line(line_s1);
                    let (s2_k, s2_v) = read_kv_line(line_s2);
                    // TODO: compare the keys and push to `merged_map` accordingly and increment the respective line iterator.
                }
                (None, Some(line_s2)) => {
                    let (s2_k, s2_v) = read_kv_line(line_s2);
                    // TODO: insert s2_k into merged map and advance its iterator.
                }
                (Some(line_s1), None) => {
                    let (s1_k, s1_v) = read_kv_line(line_s1);
                    // TODO: insert s1_k into merged map and advance its iterator.
                }
                (None, None) => {
                    // TODO: we have reached the end of both files, create a temp file ("temp.sst")

                    // TODO: write only the non deleted keys to this file from `merged_map`

                    // TODO: ensure file is synced to disk from file system buffers.

                    // TODO: remove the oldest files

                    // TODO: rename the temp file ("temp.sst") to the 2nd oldest file.

                    // TODO: pop remove the oldest file from front of sstables queue.

                    // TODO: break from loop
                }
            }

            todo!("remove me after implementing the TODOs above in the loop");
        }
        // TODO: remove the todo!() below
        todo!()
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
    use std::{
        io::{BufRead, BufReader},
        path::PathBuf,
    };

    use crate::LSMTree;

    use super::{files_with_extension, read_kv_line};

    // a help function to reset `data`` directory for tests.
    fn clear_data_dir() {
        let data_dir = PathBuf::from("data");
        if data_dir.exists() {
            std::fs::remove_dir_all("data").unwrap();
        }
    }

    // helper to find the given key `k` in the sstable `path`
    fn find_key_in_sstable(key: &str, path: &PathBuf) -> Option<String> {
        let ids = files_with_extension(path, "sst").unwrap();
        let mut ids: Vec<String> = ids
            .map(|i: PathBuf| {
                let a = i.file_name().unwrap().to_str().unwrap();
                a.parse().unwrap()
            })
            .collect();
        ids.sort();
        let data_dir = PathBuf::from("data");
        for f in ids.iter().rev() {
            let sstable = std::fs::OpenOptions::new()
                .read(true)
                .open(data_dir.join(f))
                .unwrap();
            let s1_buf = BufReader::new(sstable);
            let line = s1_buf.lines();
            for l in line {
                let (k, v) = read_kv_line(&l);
                if key == k {
                    return Some(v.to_string());
                }
            }
        }

        None
    }

    // helper to find the given key `k` in a particular sstable file
    fn find_key_in_sstable_file(key: &str, sst_file_name: &PathBuf) -> Option<String> {
        let sstable = std::fs::OpenOptions::new()
            .read(true)
            .open(sst_file_name)
            .unwrap();
        let s1_buf = BufReader::new(sstable);
        let line = s1_buf.lines();
        for l in line {
            let (k, v) = read_kv_line(&l);
            if key == k {
                return Some(v.to_string());
            }
        }

        None
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

    // TODO: make this test pass
    #[test]
    fn test_lsm_flush_triggers_compaction() {
        clear_data_dir();
        let mut lsmtree = LSMTree::new();
        lsmtree.memtable_limit = 1;
        lsmtree.sstable_mgr.compaction_trigger = 3;

        lsmtree.put("a", "v1");
        lsmtree.put("b", "v2");
        lsmtree.put("c", "v3");

        assert!(find_key_in_sstable_file("a", &PathBuf::from("data/2.sst")).is_some());
        assert!(find_key_in_sstable_file("b", &PathBuf::from("data/2.sst")).is_some());
        assert!(find_key_in_sstable_file("c", &PathBuf::from("data/2.sst")).is_none());
    }
}
