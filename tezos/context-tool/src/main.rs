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
        string_interner::StringId, working_tree::WorkingTreeStatistics, ObjectReference,
    },
    ContextKeyValueStore, IndexApi, Persistent, ShellContextApi, TezedgeContext, TezedgeIndex,
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

fn reload_context(context_path: String) -> Persistent {
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
            reload_context(context_path);
        }
        Commands::ContextSize {
            context_path,
            context_hash,
        } => {
            let ctx = reload_context(context_path);

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

            let ctx = reload_context(context_path);

            let checkout_context_hash = if let Some(context_hash) = context_hash.as_ref() {
                ContextHash::from_b58check(&context_hash).unwrap()
            } else {
                ctx.get_last_context_hash().unwrap()
            };

            let ((mut tree, storage, string_interner), parent_hash, commit) = {
                println!("CHECKOUT={:?}", checkout_context_hash);

                let read_ctx: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(ctx));

                let index = TezedgeIndex::new(Arc::clone(&read_ctx), None);

                let context = index.checkout(&checkout_context_hash).unwrap().unwrap();

                let commit = index
                    .fetch_commit_from_context_hash(&checkout_context_hash)
                    .unwrap()
                    .unwrap();

                let parent_hash = match commit.parent_commit_ref {
                    Some(parent) => Some(
                        read_ctx
                            .read()
                            .unwrap()
                            .get_hash(parent)
                            .unwrap()
                            .into_owned(),
                    ),
                    None => None,
                };

                context.tree.traverse_working_tree(&mut None).unwrap();

                (context.take_tree(), parent_hash, commit)
            };

            let mut write_ctx = Persistent::try_new(Some("/tmp/new_ctx"), false, false).unwrap();

            let parent_ref: Option<ObjectReference> = match parent_hash {
                Some(parent_hash) => Some(write_ctx.get_vacant_object_hash().unwrap().write_with(
                    |entry| {
                        *entry = parent_hash;
                    },
                ))
                .map(Into::into),
                None => None,
            };

            let write_ctx: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(write_ctx));

            storage.borrow_mut().forget_references();

            let string_interner = string_interner.borrow();
            let strings = string_interner.as_ref().unwrap();
            let string_interner = storage.borrow_mut().make_string_interner(strings);

            // storage.borrow_mut().offsets_to_hash_id = Default::default();
            let string_interner = Rc::new(RefCell::new(Some(string_interner)));
            let index = TezedgeIndex::with_storage(write_ctx, storage, string_interner);

            Rc::get_mut(&mut tree).unwrap().index = index.clone();

            let context = TezedgeContext::new(index, parent_ref, Some(tree));
            // let context = TezedgeContext::new(index, Some(parent_hash_id.into()), Some(tree));

            // let read_ctx = index.replace_repository(Arc::new(RwLock::new(write_ctx)));

            // context.index. = index;

            // println!("PARENT={:?}", context.parent_commit_ref);

            // context.index.repository = Arc::clone(&write_ctx);
            // context.tree.index.repository = write_ctx;

            let now = std::time::Instant::now();

            let commit_context_hash = context
                .commit(commit.author, commit.message, commit.time as i64)
                .unwrap();

            println!("RESULT={:?} in {:?}", commit_context_hash, now.elapsed());

            println!("Total time {:?}", start.elapsed());

            assert_eq!(checkout_context_hash, commit_context_hash);
        }
    }
}
