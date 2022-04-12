// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::{Action, ActionWithMeta, State};

use super::ProtocolRunnerCurrentHeadState;

pub fn protocol_runner_current_head_reducer(state: &mut State, action: &ActionWithMeta) {
    match &action.action {
        Action::ProtocolRunnerCurrentHeadInit(_) => {
            state.protocol_runner = ProtocolRunnerCurrentHeadState::Init.into();
        }
        Action::ProtocolRunnerCurrentHeadPending(content) => {
            state.protocol_runner = ProtocolRunnerCurrentHeadState::Pending {
                token: content.token,
            }
            .into();
        }
        Action::ProtocolRunnerCurrentHeadError(content) => {
            state.protocol_runner = ProtocolRunnerCurrentHeadState::Error {
                token: content.token,
                error: content.error.clone(),
            }
            .into();
        }
        Action::ProtocolRunnerCurrentHeadSuccess(content) => {
            state.protocol_runner = ProtocolRunnerCurrentHeadState::Success {
                token: content.token,
            }
            .into();
        }
        _ => {}
    }
}
