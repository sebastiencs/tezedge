// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::protocol_runner::current_head::ProtocolRunnerCurrentHeadState;
use crate::{Action, ActionWithMeta, State};

use super::init::ProtocolRunnerInitState;
use super::{ProtocolRunnerReadyState, ProtocolRunnerState};

pub fn protocol_runner_reducer(state: &mut State, action: &ActionWithMeta) {
    match &action.action {
        Action::ProtocolRunnerReady(_) => {
            eprintln!("OK ICI STATE={:?}", &state.protocol_runner);

            let (genesis_commit_hash, latest_context_hashes) = match &state.protocol_runner {
                ProtocolRunnerState::GetCurrentHead(ProtocolRunnerCurrentHeadState::Success {
                    genesis_commit_hash,
                    latest_context_hashes,
                    ..
                }) => (genesis_commit_hash.clone(), latest_context_hashes.clone()),
                // ProtocolRunnerState::Init(ProtocolRunnerInitState::Success {
                //     genesis_commit_hash,
                // }) => genesis_commit_hash.clone(),
                _ => return,
            };

            // let genesis_commit_hash = match &state.protocol_runner {
            //     ProtocolRunnerState::Init(ProtocolRunnerInitState::Success {
            //         genesis_commit_hash,
            //     }) => genesis_commit_hash.clone(),
            //     _ => return,
            // };

            state.protocol_runner = ProtocolRunnerReadyState {
                genesis_commit_hash,
                latest_context_hashes,
            }
            .into();
        }
        Action::ProtocolRunnerShutdownPending(_) => {
            state.protocol_runner = ProtocolRunnerState::ShutdownPending;
        }
        Action::ProtocolRunnerShutdownSuccess(_) => {
            state.protocol_runner = ProtocolRunnerState::ShutdownSuccess;
        }
        _ => return,
    };

    eprintln!("protocol_runner_reducer action={:?}", action.action);
}
