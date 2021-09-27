// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use redux_rs::{ActionWithId, Store};

use crate::peer::handshaking::PeerHandshakingInitAction;
use crate::peers::graylist::PeersGraylistAddressAction;
use crate::service::{MioService, RandomnessService, Service};
use crate::{Action, State};

use super::{
    PeerConnectionOutgoingErrorAction, PeerConnectionOutgoingInitAction,
    PeerConnectionOutgoingPendingAction, PeerConnectionOutgoingRandomInitAction,
};

pub fn peer_connection_outgoing_effects<S>(
    store: &mut Store<State, S, Action>,
    action: &ActionWithId<Action>,
) where
    S: Service,
{
    match &action.action {
        Action::PeerConnectionOutgoingRandomInit(_) => {
            let state = store.state.get();
            let potential_peers = state.peers.potential_iter().collect::<Vec<_>>();

            if state.peers.connected_len() >= state.config.peers_connected_max {
                return;
            }

            if let Some(address) = store.service.randomness().choose_peer(&potential_peers) {
                store.dispatch(PeerConnectionOutgoingInitAction { address }.into());
            }
        }
        Action::PeerConnectionOutgoingInit(action) => {
            let address = action.address;
            let result = store.service().mio().peer_connection_init(address);
            store.dispatch(match result {
                Ok(token) => PeerConnectionOutgoingPendingAction { address, token }.into(),
                Err(error) => PeerConnectionOutgoingErrorAction {
                    address,
                    error: error.into(),
                }
                .into(),
            });
        }
        Action::PeerConnectionOutgoingPending(_) => {
            // try to connect to next random peer.
            store.dispatch(PeerConnectionOutgoingRandomInitAction {}.into());
        }
        Action::PeerConnectionOutgoingSuccess(action) => store.dispatch(
            PeerHandshakingInitAction {
                address: action.address,
            }
            .into(),
        ),
        Action::PeerConnectionOutgoingError(action) => {
            store.dispatch(
                PeersGraylistAddressAction {
                    address: action.address,
                }
                .into(),
            );
        }
        _ => {}
    }
}