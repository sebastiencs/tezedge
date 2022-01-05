use std::sync::{Arc, RwLock};

use clap::{Parser, Subcommand};
use crypto::hash::{ContextHash, HashTrait};
use tezos_context::{
    kv_store::persistent::FileSizes,
    persistent::{
        file::{File, TAG_SIZES},
        KeyValueStoreBackend,
    },
    IndexApi, Persistent, TezedgeIndex,
};

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
    ContextSize {
        /// Path of the persistent context
        #[clap(short, long)]
        context_path: String,
        /// Commit to inspect, `None` to inspect the last commit
        #[clap(short, long)]
        context_hash: Option<String>,
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

            let index = TezedgeIndex::new(Arc::new(RwLock::new(ctx)), None);

            let context = index.checkout(&context_hash).unwrap().unwrap();
            context.tree.traverse_working_tree().unwrap();

            // index.
        }
    }
}
