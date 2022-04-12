// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::service::ProtocolRunnerService;
use crate::{Action, ActionWithMeta, Service, Store};

use super::ProtocolRunnerCurrentHeadPendingAction;

pub fn protocol_runner_current_head_effects<S>(store: &mut Store<S>, action: &ActionWithMeta)
where
    S: Service,
{
    if let Action::ProtocolRunnerSpawnServerInit(_) = &action.action {
        let token = store.service.protocol_runner().get_current_head();
        store.dispatch(ProtocolRunnerCurrentHeadPendingAction { token });
    }
}
