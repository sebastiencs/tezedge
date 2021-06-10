// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::path::PathBuf;

use ocaml_interop::BoxRoot;

use crate::{PatchContextFunction, TezedgeContext, TezedgeIndex};

#[derive(Debug, Clone)]
pub enum ContextKvStoreConfiguration {
    Sled { path: PathBuf },
    InMem,
    BTreeMap,
    InMemGC,
}

// TODO: are errors not possible here? recheck that
pub fn initialize_tezedge_index(
    _context_kv_store: &ContextKvStoreConfiguration,
    patch_context: Option<BoxRoot<PatchContextFunction>>,
) -> TezedgeIndex {
    TezedgeIndex::new(patch_context)
}

pub fn initialize_tezedge_context(
    context_kv_store: &ContextKvStoreConfiguration,
) -> Result<TezedgeContext, failure::Error> {
    let index = initialize_tezedge_index(context_kv_store, None);
    Ok(TezedgeContext::new(index, None, None))
}
