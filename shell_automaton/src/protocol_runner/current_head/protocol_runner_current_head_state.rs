// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};
use tezos_protocol_ipc_client::ProtocolRunnerError;

use crate::protocol_runner::ProtocolRunnerToken;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum ProtocolRunnerCurrentHeadState {
    Init,
    Pending {
        token: ProtocolRunnerToken,
    },
    Error {
        token: ProtocolRunnerToken,
        error: ProtocolRunnerError,
    },
    Success {
        token: ProtocolRunnerToken,
    },
}
