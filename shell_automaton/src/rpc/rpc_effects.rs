// Copyright (c) SimpleStaking, Viable Systems and Tezedge Contributors
// SPDX-License-Identifier: MIT

use crate::mempool::mempool_actions::{
    MempoolAskCurrentHeadAction, MempoolGetPendingOperationsAction, MempoolOperationInjectAction,
    MempoolRegisterOperationsStreamAction, MempoolRemoveAppliedOperationsAction,
    MempoolRpcEndorsementsStatusGetAction,
};
use crate::rights::{rights_actions::RightsRpcEndorsingRightsGetAction, EndorsingRightsKey};
use crate::service::rpc_service::{RpcRequest, RpcRequestStream};
use crate::service::{RpcService, Service};
use crate::{Action, ActionWithMeta, Store};

pub fn rpc_effects<S: Service>(store: &mut Store<S>, action: &ActionWithMeta) {
    match &action.action {
        Action::WakeupEvent(_) => {
            while let Ok((msg, rpc_id)) = store.service().rpc().try_recv_stream() {
                match msg {
                    RpcRequestStream::GetOperations {
                        applied,
                        refused,
                        branch_delayed,
                        branch_refused,
                    } => {
                        store.dispatch(MempoolRegisterOperationsStreamAction {
                            rpc_id,
                            applied,
                            refused,
                            branch_delayed,
                            branch_refused,
                        });
                    }
                }
            }
            while let Ok((msg, rpc_id)) = store.service().rpc().try_recv() {
                match msg {
                    RpcRequest::GetCurrentGlobalState { channel } => {
                        let _ = channel.send(store.state.get().clone());
                    }
                    RpcRequest::GetMempoolOperationStats { channel } => {
                        let stats = store.state().mempool.operation_stats.clone();
                        let _ = channel.send(stats);
                    }
                    RpcRequest::InjectOperation {
                        operation,
                        operation_hash,
                    } => {
                        store.dispatch(MempoolOperationInjectAction {
                            operation,
                            operation_hash,
                            rpc_id,
                        });
                    }
                    RpcRequest::RequestCurrentHeadFromConnectedPeers => {
                        store.dispatch(MempoolAskCurrentHeadAction {});
                    }
                    RpcRequest::RemoveOperations { operation_hashes } => {
                        store.dispatch(MempoolRemoveAppliedOperationsAction { operation_hashes });
                    }
                    RpcRequest::MempoolStatus => match &store.state().mempool.running_since {
                        None => store
                            .service()
                            .rpc()
                            .respond(rpc_id, serde_json::Value::Null),
                        Some(()) => store
                            .service()
                            .rpc()
                            .respond(rpc_id, serde_json::Value::String("".to_string())),
                    },
                    RpcRequest::GetPendingOperations => {
                        store.dispatch(MempoolGetPendingOperationsAction { rpc_id });
                    }
                    RpcRequest::GetEndorsingRights { block_hash, level } => {
                        store.dispatch(RightsRpcEndorsingRightsGetAction {
                            key: EndorsingRightsKey {
                                current_block_hash: block_hash,
                                level,
                            },
                            rpc_id,
                        });
                    }
                    RpcRequest::GetEndorsementsStatus { block_hash } => {
                        store
                            .dispatch(MempoolRpcEndorsementsStatusGetAction { rpc_id, block_hash });
                    }
                }
            }
        }
        _ => {}
    }
}
