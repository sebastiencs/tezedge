// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::protocol_runner::ProtocolRunnerReadyAction;
use crate::service::ProtocolRunnerService;
use crate::{Action, ActionWithMeta, Service, Store};

use super::ProtocolRunnerCurrentHeadPendingAction;

pub fn protocol_runner_current_head_effects<S>(store: &mut Store<S>, action: &ActionWithMeta)
where
    S: Service,
{
    match &action.action {
        Action::ProtocolRunnerCurrentHeadInit(_) => {
            eprintln!("GET CURRENT HEAD");
            let token = store.service.protocol_runner().get_current_head();
            store.dispatch(ProtocolRunnerCurrentHeadPendingAction { token });
        }
        Action::ProtocolRunnerCurrentHeadSuccess(_) => {
            eprintln!("GET CURRENT HEAD SUCCESS");
            store.dispatch(ProtocolRunnerReadyAction {});
        }
        _ => {}
    }

    // eprintln!("ACTION={:?}", &action.action);
    // if let Action::ProtocolRunnerCurrentHeadInit(_) = &action.action {
    // }
}
