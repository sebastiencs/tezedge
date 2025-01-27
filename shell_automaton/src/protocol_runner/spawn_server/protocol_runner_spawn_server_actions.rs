// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use serde::{Deserialize, Serialize};

use tezos_protocol_ipc_client::ProtocolRunnerError;

use crate::protocol_runner::ProtocolRunnerState;
use crate::{EnablingCondition, State};

use super::ProtocolRunnerSpawnServerState;

#[cfg_attr(feature = "fuzzing", derive(fuzzcheck::DefaultMutator))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtocolRunnerSpawnServerInitAction {}

impl EnablingCondition<State> for ProtocolRunnerSpawnServerInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.protocol_runner, ProtocolRunnerState::Idle)
    }
}

#[cfg_attr(feature = "fuzzing", derive(fuzzcheck::DefaultMutator))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtocolRunnerSpawnServerPendingAction {}

impl EnablingCondition<State> for ProtocolRunnerSpawnServerPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(
            &state.protocol_runner,
            ProtocolRunnerState::SpawnServer(ProtocolRunnerSpawnServerState::Init)
        )
    }
}

#[cfg_attr(feature = "fuzzing", derive(fuzzcheck::DefaultMutator))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtocolRunnerSpawnServerErrorAction {
    pub error: ProtocolRunnerError,
}

impl EnablingCondition<State> for ProtocolRunnerSpawnServerErrorAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(
            &state.protocol_runner,
            ProtocolRunnerState::SpawnServer(ProtocolRunnerSpawnServerState::Pending {})
        )
    }
}

#[cfg_attr(feature = "fuzzing", derive(fuzzcheck::DefaultMutator))]
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProtocolRunnerSpawnServerSuccessAction {}

impl EnablingCondition<State> for ProtocolRunnerSpawnServerSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(
            &state.protocol_runner,
            ProtocolRunnerState::SpawnServer(ProtocolRunnerSpawnServerState::Pending {})
        )
    }
}
