use std::{
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
    working_tree::string_interner::StringId,
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

            let context = index.checkout(&context_hash).unwrap().unwrap();
            let mut stats = context.tree.traverse_working_tree().unwrap();

            let repo = context.index.repository.read().unwrap();
            let repo_stats = repo.get_read_statistics().unwrap().unwrap();

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
            let ((mut tree, storage, string_interner), commit) = {
                let ctx = reload_context(context_path);

                let context_hash = if let Some(context_hash) = context_hash.as_ref() {
                    ContextHash::from_b58check(&context_hash).unwrap()
                } else {
                    ctx.get_last_context_hash().unwrap()
                };

                let read_ctx: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(ctx));

                let index = TezedgeIndex::new(Arc::clone(&read_ctx), None);

                let context = index.checkout(&context_hash).unwrap().unwrap();

                // let parent_hash = index
                //     .fetch_parent_from_context_hash(&context_hash)
                //     .unwrap()
                //     .unwrap();

                let commit = index
                    .fetch_commit_from_context_hash(&context_hash)
                    .unwrap()
                    .unwrap();

                println!("COMMIT={:?}", commit);

                // let parent_commit_hash_id = commit.parent_commit_ref.map(|p| p.hash_id());
                // let parent_commit_hash =

                context.tree.traverse_working_tree().unwrap();
                // context.tree.forget_references().unwrap();

                (context.take_tree(), commit)
            };

            let write_ctx = Persistent::try_new(Some("/tmp/new_ctx"), false, false).unwrap();

            // WRITE PARENT
            // write_ctx.get_vacant_object_hash().unwrap().

            let write_ctx: Arc<RwLock<ContextKeyValueStore>> = Arc::new(RwLock::new(write_ctx));

            storage.borrow_mut().forget_references();
            // storage.borrow_mut().offsets_to_hash_id = Default::default();
            let index = TezedgeIndex::with_storage(write_ctx, storage, string_interner);

            tree.index = index.clone();

            // TODO: Set parent
            let context = TezedgeContext::new(index, None, Some(Rc::new(tree)));

            // let read_ctx = index.replace_repository(Arc::new(RwLock::new(write_ctx)));

            // context.index. = index;

            // println!("PARENT={:?}", context.parent_commit_ref);

            // context.index.repository = Arc::clone(&write_ctx);
            // context.tree.index.repository = write_ctx;

            context
                .commit(commit.author, commit.message, commit.time as i64)
                .unwrap();
        }
    }
}
