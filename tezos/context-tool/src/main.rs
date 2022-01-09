use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, RwLock},
};

use clap::{Parser, Subcommand};
use crypto::hash::{ContextHash, HashTrait};
use tezos_context::{
    kv_store::persistent::FileSizes,
    persistent::{
        file::{File, TAG_SIZES},
        KeyValueStoreBackend,
    },
    working_tree::{
        string_interner::StringId, working_tree::WorkingTreeStatistics, Commit, ObjectReference,
    },
    ContextKeyValueStore, IndexApi, ObjectHash, Persistent, ShellContextApi, TezedgeContext,
    TezedgeIndex,
};

mod stats;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(about, version, author)]
struct Args {
    #[clap(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Build integrity database (file `sizes.db`)
    BuildIntegrity {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
        #[clap(short, long)]
        /// Path to write the resulting `sizes.db`
        output_dir: Option<String>,
    },
    /// Check if the context match its `sizes.db`
    IsValidContext {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
    },
    /// Display `sizes.db` file
    DumpChecksums {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
    },
    ContextSize {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
        /// Commit to inspect, default to last commit
        #[clap(short, long)]
        context_hash: Option<String>,
    },
    MakeSnapshot {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
        /// Commit to make the snapshot from, default to last commit
        #[clap(short, long)]
        acontext_hash: Option<String>,
        /// Path of the result
        #[clap(short, long)]
        output: String,
    },
}

fn reload_context_readonly(context_path: String) -> Persistent {
    println!("Reading context...");

    let now = std::time::Instant::now();

    let sizes_file = File::<{ TAG_SIZES }>::try_new(&context_path, true).unwrap();
    let sizes = FileSizes::make_list_from_file(&sizes_file).unwrap_or(Vec::new());
    assert!(!sizes.is_empty(), "sizes.db is invalid");

    let mut ctx = Persistent::try_new(Some(context_path.as_str()), true, true).unwrap();
    ctx.reload_database().unwrap();

    println!("{:?}", ctx.memory_usage());

    println!(
        "Context at {:?} is valid in {:?}",
        context_path,
        now.elapsed()
    );

    ctx
}

fn main() {
    let args = Args::parse();

    match args.command {
        Commands::DumpChecksums { context_path } => {
            let sizes_file = File::<{ TAG_SIZES }>::try_new(&context_path, true).unwrap();
            let sizes = FileSizes::make_list_from_file(&sizes_file).unwrap_or(Vec::new());
            println!("checksums={:#?}", sizes);
        }
        Commands::BuildIntegrity {
            context_path,
            output_dir,
        } => {
            let output_dir = output_dir.unwrap_or("".to_string());

            // Make sure `sizes.db` doesn't already exist
            match File::<{ TAG_SIZES }>::create_new_file(&output_dir) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    panic!(
                        "The resulting file `sizes.db` already exist at `{:?}`",
                        output_dir
                    );
                }
                Err(e) => panic!("{:?}", e),
            };

            let mut ctx = Persistent::try_new(Some(context_path.as_str()), true, true).unwrap();
            let mut output_file = File::<{ TAG_SIZES }>::try_new(&output_dir, false).unwrap();

            ctx.compute_integrity(&mut output_file).unwrap();

            let sizes = ctx.get_file_sizes();
            println!("Result={:#?}", sizes);
        }
        Commands::IsValidContext { context_path } => {
            reload_context_readonly(context_path);
        }
        Commands::ContextSize {
            context_path,
            context_hash,
        } => {
            let ctx = reload_context_readonly(context_path);

            let context_hash = if let Some(context_hash) = context_hash.as_ref() {
                ContextHash::from_b58check(&context_hash).unwrap()
            } else {
                ctx.get_last_context_hash().unwrap()
            };

            println!("Computing size for {:?}", context_hash.to_base58_check());

            let index = TezedgeIndex::new(Arc::new(RwLock::new(ctx)), None);

            let now = std::time::Instant::now();

            let mut stats = Some(WorkingTreeStatistics::default());

            let context = index.checkout(&context_hash).unwrap().unwrap();
            context.tree.traverse_working_tree(&mut stats).unwrap();

            let repo = context.index.repository.read().unwrap();
            let repo_stats = repo.get_read_statistics().unwrap().unwrap();

            let mut stats = stats.unwrap();
            stats.objects_total_bytes = repo_stats.objects_total_bytes;
            stats.lowest_offset = repo_stats.lowest_offset;
            stats.nshapes = repo_stats.unique_shapes.len();
            stats.shapes_total_bytes = repo_stats.shapes_length * std::mem::size_of::<StringId>();

            println!("{:#?}", stats::DebugWorkingTreeStatistics(stats));
            // println!("{:#?}", repo_stats);
            println!("Time {:?}", now.elapsed());
        }
        Commands::MakeSnapshot {
            context_path,
            acontext_hash: context_hash,
            output,
        } => {
            let start = std::time::Instant::now();

            let ctx = reload_context_readonly(context_path);

            let checkout_context_hash: ContextHash =
                if let Some(context_hash) = context_hash.as_ref() {
                    ContextHash::from_b58check(context_hash).unwrap()
                } else {
                    ctx.get_last_context_hash().unwrap()
                };

            println!("CHECKOUT={:?}", checkout_context_hash);

            let (mut tree, mut storage, string_interner, parent_hash, commit) = {
                // This block reads the whole tree of the commit, to extract `Storage`, that's
                // where all the objects (directories and blobs) are stored

                let read_repo: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(ctx));
                let index = TezedgeIndex::new(Arc::clone(&read_repo), None);
                let context = index.checkout(&checkout_context_hash).unwrap().unwrap();

                // Take the commit from repository
                let commit: Commit = index
                    .fetch_commit_from_context_hash(&checkout_context_hash)
                    .unwrap()
                    .unwrap();

                // If the commit has a parent, fetch it
                // It is necessary for the snapshot to have it in its db
                let parent_hash: Option<ObjectHash> = match commit.parent_commit_ref {
                    Some(parent) => {
                        let repo = read_repo.read().unwrap();
                        Some(repo.get_hash(parent).unwrap().into_owned())
                    }
                    None => None,
                };

                // Traverse the tree, to store it in the `Storage`
                context.tree.traverse_working_tree(&mut None).unwrap();

                // Extract the `Storage`, `StringInterner` and `WorkingTree` from
                // the index
                (
                    Rc::try_unwrap(context.tree).ok().unwrap(),
                    context.index.storage.take(),
                    context.index.string_interner.take().unwrap(),
                    parent_hash,
                    commit,
                )
            };

            {
                // This block creates the new database

                // Remove all `HashId` and `AbsoluteOffset` from the `Storage`
                // They will be recomputed
                storage.forget_references();

                // Create a new `StringInterner` that contains only the strings used
                // for this commit
                let string_interner = storage.make_string_interner(string_interner);

                let storage = Rc::new(RefCell::new(storage));
                let string_interner = Rc::new(RefCell::new(Some(string_interner)));

                // Create the new writable repository at the `output` path
                let mut write_repo =
                    Persistent::try_new(Some("/tmp/new_ctx"), false, false).unwrap();
                write_repo.enable_hash_dedup();

                // Put the parent hash in the new repository
                let parent_ref: Option<ObjectReference> = match parent_hash {
                    Some(parent_hash) => Some(write_repo.put_hash(parent_hash).unwrap().into()),
                    None => None,
                };

                let write_repo: Arc<RwLock<ContextKeyValueStore>> =
                    Arc::new(RwLock::new(write_repo));

                let index =
                    TezedgeIndex::with_storage(write_repo.clone(), storage, string_interner);

                // Make the `WorkingTree` use our new index
                tree.index = index.clone();

                {
                    // Compute the hashes of the whole tree and remove the duplicate ones
                    let mut repo = write_repo.write().unwrap();
                    tree.get_root_directory_hash(&mut *repo).unwrap();
                    index.storage.borrow_mut().deduplicate_hashes(&*repo);
                }

                let context = TezedgeContext::new(index, parent_ref, Some(Rc::new(tree)));

                let now = std::time::Instant::now();

                let commit_context_hash = context
                    .commit(
                        commit.author.clone(),
                        commit.message.clone(),
                        commit.time as i64,
                    )
                    .unwrap();

                println!("RESULT={:?} in {:?}", commit_context_hash, now.elapsed());

                println!("Total time {:?}", start.elapsed());

                // Make sure our new context hash is the same
                assert_eq!(checkout_context_hash, commit_context_hash);
            }

            {
                // Fully read the new snapshot and re-compute all the hashes, to
                // be 100% sure that we have a valid snapshot

                let read_ctx = reload_context_readonly("/tmp/new_ctx".to_string());
                let read_repo: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(read_ctx));

                let index = TezedgeIndex::new(Arc::clone(&read_repo), None);
                let context = index.checkout(&checkout_context_hash).unwrap().unwrap();

                // Fetch all objects into `Storage`
                context.tree.traverse_working_tree(&mut None).unwrap();

                // Remove all `HashId` to re-compute them
                context.index.storage.borrow_mut().forget_references();

                let context_hash = context
                    .hash(commit.author, commit.message, commit.time as i64)
                    .unwrap();

                assert_eq!(checkout_context_hash, context_hash);
            }
        }
    }
}
