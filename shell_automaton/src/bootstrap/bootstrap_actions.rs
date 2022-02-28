// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::net::SocketAddr;

use crypto::hash::BlockHash;
use serde::{Deserialize, Serialize};
use storage::BlockHeaderWithHash;
use tezos_messages::p2p::encoding::block_header::Level;
use tezos_messages::p2p::encoding::operations_for_blocks::OperationsForBlocksMessage;

use crate::bootstrap::BootstrapState;
use crate::current_head::CurrentHeadState;
use crate::protocol_runner::ProtocolRunnerState;
use crate::{EnablingCondition, State};

pub const MAX_PENDING_GET_OPERATIONS: usize = 128;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapInitAction {}

impl EnablingCondition<State> for BootstrapInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.current_head, CurrentHeadState::Rehydrated { .. })
            && matches!(&state.protocol_runner, ProtocolRunnerState::Ready(_))
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersConnectPendingAction {}

impl EnablingCondition<State> for BootstrapPeersConnectPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.bootstrap, BootstrapState::Init { .. })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersConnectSuccessAction {}

impl EnablingCondition<State> for BootstrapPeersConnectSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.bootstrap, BootstrapState::PeersConnectPending { .. })
            && state.peers.handshaked_len() >= state.config.peers_bootstrapped_min
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersMainBranchFindInitAction {}

impl EnablingCondition<State> for BootstrapPeersMainBranchFindInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.bootstrap, BootstrapState::PeersConnectSuccess { .. })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersMainBranchFindPendingAction {}

impl EnablingCondition<State> for BootstrapPeersMainBranchFindPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.bootstrap, BootstrapState::PeersConnectSuccess { .. })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerCurrentBranchReceivedAction {
    pub peer: SocketAddr,
    pub current_head: BlockHeaderWithHash,
    pub history: Vec<BlockHash>,
}

impl EnablingCondition<State> for BootstrapPeerCurrentBranchReceivedAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersMainBranchFindPending { peer_branches, .. } => {
                !peer_branches.contains_key(&self.peer)
            }
            BootstrapState::PeersBlockHeadersGetPending { .. } => true,
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersMainBranchFindSuccessAction {}

impl EnablingCondition<State> for BootstrapPeersMainBranchFindSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        state
            .bootstrap
            .main_block(state.config.peers_bootstrapped_min)
            .is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockHeadersGetInitAction {}

impl EnablingCondition<State> for BootstrapPeersBlockHeadersGetInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(
            &state.bootstrap,
            BootstrapState::PeersMainBranchFindSuccess { .. }
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockHeadersGetPendingAction {}

impl EnablingCondition<State> for BootstrapPeersBlockHeadersGetPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(
            &state.bootstrap,
            BootstrapState::PeersMainBranchFindSuccess { .. }
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockHeaderGetInitAction {
    pub peer: SocketAddr,
}

impl EnablingCondition<State> for BootstrapPeerBlockHeaderGetInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        state
            .bootstrap
            .peer_interval(self.peer, |p| p.current.is_pending())
            .is_none()
            && state.bootstrap.peer_next_interval(self.peer).is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockHeaderGetPendingAction {
    pub peer: SocketAddr,
}

impl EnablingCondition<State> for BootstrapPeerBlockHeaderGetPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        state
            .bootstrap
            .peer_interval(self.peer, |p| p.current.is_pending())
            .is_none()
            && state.bootstrap.peer_next_interval(self.peer).is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockHeaderGetTimeoutAction {
    pub peer: SocketAddr,
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockHeaderGetTimeoutAction {
    fn is_enabled(&self, state: &State) -> bool {
        let current_time = state.time_as_nanos();
        let timeout = state.config.bootstrap_block_header_get_timeout.as_nanos() as u64;
        state
            .bootstrap
            .peer_interval(self.peer, |p| {
                p.current.is_pending_block_hash_eq(&self.block_hash)
            })
            .map_or(false, |(_, p)| {
                p.current.is_pending_timed_out(timeout, current_time)
            })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockHeaderGetSuccessAction {
    pub peer: SocketAddr,
    pub block: BlockHeaderWithHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockHeaderGetSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        let level = self.block.header.level();
        let hash = &self.block.hash;
        state
            .bootstrap
            .peer_interval(self.peer, |p| {
                p.current.is_pending_block_level_and_hash_eq(level, hash)
            })
            .is_some()
    }
}

/// Consume downloaded block header.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockHeaderGetFinishAction {
    pub peer: SocketAddr,
}

impl EnablingCondition<State> for BootstrapPeerBlockHeaderGetFinishAction {
    fn is_enabled(&self, state: &State) -> bool {
        state
            .bootstrap
            .peer_interval(self.peer, |p| p.current.is_success())
            .is_some()
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockHeadersGetSuccessAction {}

impl EnablingCondition<State> for BootstrapPeersBlockHeadersGetSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        let current_head = match state.current_head.get() {
            Some(v) => v,
            None => return false,
        };
        match &state.bootstrap {
            BootstrapState::PeersBlockHeadersGetPending {
                main_chain,
                main_chain_last_level,
                peer_intervals,
                ..
            } => {
                peer_intervals.is_empty()
                    && main_chain_last_level - main_chain.len() as Level
                        <= current_head.header.level()
            }
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockOperationsGetInitAction {}

impl EnablingCondition<State> for BootstrapPeersBlockOperationsGetInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        // TODO(zura)
        matches!(
            &state.bootstrap,
            BootstrapState::PeersBlockHeadersGetSuccess { .. }
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockOperationsGetPendingAction {}

impl EnablingCondition<State> for BootstrapPeersBlockOperationsGetPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        // TODO(zura)
        matches!(
            &state.bootstrap,
            BootstrapState::PeersBlockHeadersGetSuccess { .. }
        )
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockOperationsGetNextAllAction {}

impl EnablingCondition<State> for BootstrapPeersBlockOperationsGetNextAllAction {
    fn is_enabled(&self, state: &State) -> bool {
        BootstrapPeersBlockOperationsGetNextAction {}.is_enabled(state)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockOperationsGetNextAction {}

impl EnablingCondition<State> for BootstrapPeersBlockOperationsGetNextAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { queue, pending, .. } => {
                pending.len() < MAX_PENDING_GET_OPERATIONS && !queue.is_empty()
            }
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockOperationsGetPendingAction {
    pub peer: SocketAddr,
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockOperationsGetPendingAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { queue, pending, .. } => {
                pending.len() < MAX_PENDING_GET_OPERATIONS
                    && pending.get(&self.block_hash).is_none()
                    && queue
                        .front()
                        .map_or(false, |v| v.block_hash == self.block_hash)
            }
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockOperationsGetTimeoutAction {
    pub peer: SocketAddr,
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockOperationsGetTimeoutAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { pending, .. } => {
                let current_time = state.time_as_nanos();
                let timeout = state
                    .config
                    .bootstrap_block_operations_get_timeout
                    .as_nanos() as u64;
                pending
                    .get(&self.block_hash)
                    .and_then(|p| p.peers.get(&self.peer))
                    .map_or(false, |p| p.is_pending_timed_out(timeout, current_time))
            }
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockOperationsGetRetryAction {
    pub peer: SocketAddr,
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockOperationsGetRetryAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { pending, .. } => pending
                .get(&self.block_hash)
                .map_or(false, |b| !b.peers.iter().any(|(_, p)| p.is_pending())),
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockOperationsReceivedAction {
    pub peer: SocketAddr,
    pub message: OperationsForBlocksMessage,
}

impl EnablingCondition<State> for BootstrapPeerBlockOperationsReceivedAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { pending, .. } => pending
                .get(self.message.operations_for_block().block_hash())
                .and_then(|v| v.peers.get(&self.peer))
                .map_or(false, |peer_state| {
                    peer_state.is_validation_pass_pending(
                        self.message.operations_for_block().validation_pass() as u8,
                    )
                }),
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeerBlockOperationsGetSuccessAction {
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapPeerBlockOperationsGetSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { pending, .. } => pending
                .get(&self.block_hash)
                .and_then(|v| v.peers.iter().find(|(_, p)| p.is_complete()))
                .is_some(),
            _ => false,
        }
    }
}

fn next_block_apply_level(state: &State) -> Option<Level> {
    let is_applying = match state.block_applier.current.is_pending() {
        true => 1,
        false => 0,
    };
    let queue_len = state.block_applier.queue.len() as Level;
    state
        .current_head
        .get()
        .map(|v| v.header.level())
        .map(|level| level + is_applying + queue_len + 1)
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapScheduleBlocksForApplyAction {}

impl EnablingCondition<State> for BootstrapScheduleBlocksForApplyAction {
    fn is_enabled(&self, state: &State) -> bool {
        !state.block_applier.current.is_pending()
            && match &state.bootstrap {
                BootstrapState::PeersBlockOperationsGetPending { pending, .. } => pending
                    .iter()
                    .min_by_key(|(_, b)| b.block_level)
                    .map_or(false, |(_, b)| b.is_success()),
                _ => false,
            }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapScheduleBlockForApplyAction {
    pub block_hash: BlockHash,
}

impl EnablingCondition<State> for BootstrapScheduleBlockForApplyAction {
    fn is_enabled(&self, state: &State) -> bool {
        !state.block_applier.current.is_pending()
            && match &state.bootstrap {
                BootstrapState::PeersBlockOperationsGetPending { pending, .. } => {
                    let next_block_level = match next_block_apply_level(state) {
                        Some(v) => v,
                        None => return false,
                    };
                    pending.get(&self.block_hash).map_or(false, |b| {
                        b.block_level <= next_block_level
                            && b.block_level >= next_block_level - 2
                            && b.is_success()
                    })
                }
                _ => false,
            }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapPeersBlockOperationsGetSuccessAction {}

impl EnablingCondition<State> for BootstrapPeersBlockOperationsGetSuccessAction {
    fn is_enabled(&self, state: &State) -> bool {
        match &state.bootstrap {
            BootstrapState::PeersBlockOperationsGetPending { queue, pending, .. } => {
                queue.is_empty() && pending.is_empty()
            }
            _ => false,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapFinishedAction {}

impl EnablingCondition<State> for BootstrapFinishedAction {
    fn is_enabled(&self, state: &State) -> bool {
        !state.block_applier.current.is_pending()
            && match &state.bootstrap {
                BootstrapState::PeersMainBranchFindSuccess { main_block, .. } => {
                    state.is_same_head(main_block.0, &main_block.1)
                }
                BootstrapState::PeersBlockOperationsGetSuccess { .. } => true,
                _ => false,
            }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapFromPeerCurrentHeadAction {
    pub peer: SocketAddr,
    pub current_head: BlockHeaderWithHash,
}

impl EnablingCondition<State> for BootstrapFromPeerCurrentHeadAction {
    fn is_enabled(&self, state: &State) -> bool {
        matches!(&state.bootstrap, BootstrapState::Finished { .. })
            && state.can_accept_new_head(&self.current_head)
            && !state.is_same_head(self.current_head.header.level(), &self.current_head.hash)
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct BootstrapCheckTimeoutsInitAction {}

impl EnablingCondition<State> for BootstrapCheckTimeoutsInitAction {
    fn is_enabled(&self, state: &State) -> bool {
        match state.bootstrap.timeouts_last_check() {
            Some(time) => {
                let check_timeouts_interval =
                    state.config.check_timeouts_interval.as_nanos() as u64;
                let current_time = state.time_as_nanos();
                current_time - time >= check_timeouts_interval
            }
            None => false,
        }
    }
}
