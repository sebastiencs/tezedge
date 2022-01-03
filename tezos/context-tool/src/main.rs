use clap::{Parser, Subcommand};
use tezos_context::{
    kv_store::persistent::FileSizes,
    persistent::{
        file::{File, TAG_SIZES},
        KeyValueStoreBackend,
    },
    Persistent,
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
            println!("Reading context...");

            let sizes_file = File::<{ TAG_SIZES }>::try_new(&context_path, true).unwrap();
            let sizes = FileSizes::make_list_from_file(&sizes_file).unwrap_or(Vec::new());
            assert!(!sizes.is_empty(), "sizes.db doesn't exist or is empty");

            let mut ctx = Persistent::try_new(Some(context_path.as_str()), true, true).unwrap();
            ctx.reload_database().unwrap();

            println!("{:?}", ctx.memory_usage());

            println!("Context at {:?} is valid", context_path);
        }
    }
}
