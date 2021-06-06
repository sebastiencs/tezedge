use std::{cell::{Ref, RefCell}, collections::{BTreeMap, HashMap}, sync::Arc};

use crossbeam_channel::{Receiver, Sender};
use serde::{Serialize, Deserialize};

use crate::{hash::EntryHash, working_tree::Entry};

use tezos_spsc::{Consumer, Producer};

#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct HashId(usize);

#[derive(Debug)]
pub struct HashInterner {
    hashes: slab::Slab<EntryHash>,
    // values: Vec<Option<Arc<[u8]>>>,
    frees: Consumer<HashId>,
    values: MapValues,
}

// pub(crate) struct HashInterner {
//     inner: RefCell<HashInternerInner>
// }

impl HashInterner {
    fn new(consumer: Consumer<HashId>) -> Self {
        Self {
            // inner: RefCell::new(HashInternerInner {
                hashes: slab::Slab::default(),
                frees: consumer,
                values: MapValues::default(),
            // })
        }
    }

    pub(crate) fn put(&mut self, hash: EntryHash) -> HashId {
        // let mut inner = self.inner.borrow_mut();
        let free_id = self.frees.pop().ok();

        let res = if let Some(free_id) = free_id {
            self.values.remove(free_id);
            self.hashes[free_id.0] = hash;
            free_id
        } else {
            let id = self.hashes.insert(hash);
            HashId(id)
        };

        res
    }

    pub(crate) fn get(&self, hash_id: HashId) -> Option<&EntryHash> {
        self.hashes.get(hash_id.0)
    }

    pub(crate) fn get_value(&self, hash_id: HashId) -> Option<&[u8]> {
        self.values.get(hash_id)
    }
}

/// Kind of a secondary map HashId -> Arc<[u8]>
#[derive(Debug, Default)]
struct MapValues {
    values: Vec<Option<Arc<[u8]>>>,
}

impl MapValues {
    fn set(&mut self, hash_id: HashId, value: Arc<[u8]>) {
        let index = hash_id.0 as usize;

        if index >= self.values.len() {
            self.values.resize_with(index + 1, Default::default);
        }

        self.values[index] = Some(value);
    }

    fn get(&self, hash_id: HashId) -> Option<&[u8]> {
        let index = hash_id.0 as usize;

        self.values.get(index)?.as_ref().map(AsRef::as_ref)
    }

    fn remove(&mut self, free_id: HashId) {
        let index = free_id.0;
        self.values[index] = None;
    }
}

pub struct NewGC {
    current: BTreeMap<HashId, Arc<[u8]>>,
    pub hashes: HashInterner,
    sender: Sender<Command>,
    pub context_hashes: BTreeMap<u64, HashId>,
}

struct GCThread {
    stores: Stores,
    send_free: Producer<HashId>,
    recv: Receiver<Command>,
}

/// Commands used by MarkMoveGCed to interact with GC thread.
enum Command {
    StartNewCycle {
        in_last_cycle: BTreeMap<HashId, Arc<[u8]>>
    },
    MarkReused {
        reused: Vec<HashId>,
    },
    Exit,
}

struct Stores {
    stores: Vec<BTreeMap<HashId, Arc<[u8]>>>,
}

impl Default for Stores {
    fn default() -> Self {
        Self {
            stores: vec![Default::default(); 7]
        }
    }
}

impl Stores {
    fn move_to_newest_store(&mut self, hash_id: HashId) -> Option<Arc<[u8]>> {
        let len = self.stores.len();
        let mut value = None;

        for store in &mut self.stores[..len - 1] {
            if let Some(item) = store.remove(&hash_id) {
                value = Some(item);
            };
        }

        let value = value?;
        self.stores.last_mut().unwrap().insert(hash_id, Arc::clone(&value));
        return Some(value);
    }

    fn roll(&mut self) -> Vec<HashId> {
        let unused = self.stores.remove(0);

        let mut vec = Vec::with_capacity(unused.len());
        for id in unused.keys() {
            vec.push(*id);
        }
        vec
    }

    fn add_new_store(&mut self, new: BTreeMap<HashId, Arc<[u8]>>) {
        self.stores.push(new);
    }
}

impl Default for NewGC {
    fn default() -> Self {
        Self::new()
    }
}

impl NewGC {
    pub fn new() -> Self {
        let (sender, recv) = crossbeam_channel::unbounded();
        let (prod, cons) = tezos_spsc::bounded(1_000_000);

        std::thread::spawn(move || {
            let stores = Stores::default();

            GCThread { stores, recv, send_free: prod }.run()
        });

        let current = Default::default();
        let hashes = HashInterner::new(cons);
        // let values = MapValues::default();
        Self {
            current,
            hashes,
            sender,
            context_hashes: Default::default(),
            // values,
            // frees: cons,
        }
    }

    pub fn write_batch(&mut self, batch: Vec<(HashId, Arc<[u8]>)>) {
        // let hashes = self.hashes.inner.borrow_mut();

        // println!(
        //     "SLAB={:?}:{:?} VALUES={:?}:{:?} CURRENT={:?}",
        //     self.hashes.hashes.len(),
        //     self.hashes.hashes.capacity(),
        //     self.hashes.values.values.len(),
        //     self.hashes.values.values.capacity(),
        //     self.current.len(),
        // );

        for (hash_id, value) in batch {
            self.hashes.values.set(hash_id, Arc::clone(&value));
            self.current.insert(hash_id, value);
        }
    }

    pub fn new_cycle_started(&mut self) {
        println!("CYCLE STARTED");
        self.sender.send(Command::StartNewCycle {
            in_last_cycle: std::mem::take(&mut self.current)
        }).unwrap();
    }

    pub fn block_applied(&mut self, reused: Vec<HashId>) {
        self.sender.send(Command::MarkReused { reused }).unwrap()
    }
}

impl GCThread {
    fn run(mut self) {
        while let Ok(msg) = self.recv.recv() {
            match msg {
                Command::StartNewCycle { in_last_cycle } => {
                    self.start_new_cycle(in_last_cycle)
                }
                Command::MarkReused { reused } => {
                    self.mark_reused(reused)
                }
                Command::Exit => {
                    return;
                }
            }
        }
    }

    fn start_new_cycle(&mut self, new_store: BTreeMap<HashId, Arc<[u8]>>) {
        let unused = self.stores.roll();
        self.stores.add_new_store(new_store);

        // Notify the main thread that the ids are free to reused
        // TODO: Handle to big vec

        println!("FREE SOME IDS AFTER CYCLE LEN={:?}", unused.len());

        self.send_free.push_slice(&unused).unwrap();
    }

    fn mark_reused(&mut self, mut reused: Vec<HashId>) {
        // let mut set: HashSet<HashId> = HashSet::new();
        // set.extend(reused);

        let now = std::time::Instant::now();

        while let Some(hash_id) = reused.pop() {

            let value = match self.stores.move_to_newest_store(hash_id) {
                Some(v) => v,
                None => continue
            };

            let entry: Entry = match bincode::deserialize(&value) {
                Ok(value) => value,
                Err(err) => {
                    eprintln!("WorkingTree GC: error while decerializing entry: {:?}", err);
                    continue;
                }
            };

            match entry {
                Entry::Blob(_) => {}
                Entry::Tree(tree) => {
                    // Push every entry in this directory
                    for node in tree.values() {
                        reused.push(node.entry_hash.borrow().unwrap().clone());
                    }
                }
                Entry::Commit(commit) => {
                    // Push the root tree for this commit
                    reused.push(commit.root_hash);
                }
            }

        }

        let elapsed = now.elapsed();

        if elapsed > std::time::Duration::from_millis(20) {
            println!("MARK REUSED DONE IN {:?}", now.elapsed());
        }
    }
}
