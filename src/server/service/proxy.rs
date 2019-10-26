use std::sync::{Arc, RwLock};

use futures::{future, stream, Future, Stream};
use grpcio::{
    ChannelBuilder, Environment, Error as GrpcError, RpcContext, RpcStatus, RpcStatusCode,
    UnarySink,
};
use kvproto::errorpb;
use kvproto::kvrpcpb::*;
use kvproto::tikvpb::{self, TikvClient};
use tokio_threadpool::Builder as ThreadPoolBuilder;

use crate::server::StoreAddrResolver;
use pd_client::PdClient;
use tikv_util::collections::HashMap;
use tikv_util::future::paired_future_callback;

pub struct Service<S: StoreAddrResolver, P: PdClient> {
    store_resolver: S,
    pool: Arc<tokio_threadpool::ThreadPool>,
    clients: Arc<RwLock<HashMap<u64, TikvClient>>>,

    #[allow(dead_code)]
    pd: Arc<P>,
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> std::clone::Clone for Service<S, P> {
    fn clone(&self) -> Self {
        Service {
            store_resolver: self.store_resolver.clone(),
            pd: self.pd.clone(),
            pool: Arc::clone(&self.pool),
            clients: Arc::clone(&self.clients),
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> Service<S, P> {
    pub fn new(store_resolver: S, pd: Arc<P>) -> Self {
        Service {
            store_resolver,
            pd,
            pool: Arc::new(
                ThreadPoolBuilder::new()
                    .pool_size(4)
                    .name_prefix("dcproxy")
                    .build(),
            ),
            clients: Arc::new(RwLock::new(HashMap::default())),
        }
    }

    fn get_client(&self, store_id: u64) -> Option<TikvClient> {
        let clients = self.clients.read().unwrap();
        match clients.get(&store_id) {
            Some(c) => Some(c.clone()),
            None => None,
        }
    }
}

impl<S: StoreAddrResolver + 'static, P: PdClient + 'static> tikvpb::DcProxy for Service<S, P> {
    fn get_committed_index_and_ts(
        &mut self,
        ctx: RpcContext<'_>,
        mut req: GetCommittedIndexAndTsRequest,
        sink: UnarySink<GetCommittedIndexAndTsResponse>,
    ) {
        enum Res {
            GrpcError((GrpcError, u64)),
            RegionError(errorpb::Error),
            Index(u64),
        }

        info!("GetCommittedIndexAndTs is called");
        if req.get_ts_required() {
            panic!("GetCommittedIndexAndTs ts_required currently not supported");
        }

        let mut indices = Vec::new();
        for region_ctx in req.take_contexts().into_iter() {
            let store_id = region_ctx.get_peer().get_store_id();
            info!("getting connection for {}", store_id);
            let client = match self.get_client(store_id) {
                Some(c) => c,
                None => {
                    let (cb, f) = paired_future_callback();
                    let addr = match self
                        .store_resolver
                        .resolve(store_id, cb)
                        .and_then(|_| f.wait().unwrap())
                    {
                        Ok(a) => a,
                        Err(e) => {
                            warn!("GetCommittedIndexAndTs resolve {} fail: {:?}", store_id, e);
                            let code = RpcStatusCode::INTERNAL;
                            let status = RpcStatus::new(code, Some(format!("{}", e)));
                            ctx.spawn(sink.fail(status).map_err(|_| ()));
                            return;
                        }
                    };
                    info!("GetCommittedIndexAndTs resolved {} to {}", store_id, addr);
                    let env = Arc::new(Environment::new(4));
                    let channel = ChannelBuilder::new(env).connect(&addr);
                    let c = TikvClient::new(channel);
                    info!(
                        "GetCommittedIndexAndTs connected to [{}, {}]",
                        store_id, addr
                    );
                    let mut clients = self.clients.write().unwrap();
                    clients.insert(store_id, c.clone());
                    drop(clients);
                    c
                }
            };

            let mut req = ReadIndexRequest::default();
            req.set_context(region_ctx);
            let f = future::result(client.read_index_async(&req))
                .flatten()
                .map(|mut resp| {
                    let idx = resp.get_read_index();
                    if idx != 0 {
                        return Res::Index(idx);
                    }
                    return Res::RegionError(resp.take_region_error());
                })
                .or_else(move |e| future::ok::<_, ()>(Res::GrpcError((e, store_id))));
            indices.push(f);
        }

        let f = stream::futures_ordered(indices)
            .collect()
            .and_then(|res_vec| {
                let mut resp = GetCommittedIndexAndTsResponse::new();
                for res in res_vec {
                    match res {
                        Res::Index(i) => resp.mut_committed_indices().push(i),
                        Res::RegionError(e) => {
                            warn!("GetCommittedIndexAndTs region error: {:?}", e);
                            resp.mut_committed_indices().push(0);
                            let mut proxy_e = ProxyError::new();
                            proxy_e.set_region_error(e);
                            resp.mut_errors().push(proxy_e);
                        }
                        Res::GrpcError(e) => {
                            // TODO: update peer cache.
                            warn!("GetCommittedIndexAndTs grpc error: {:?}", e);
                            resp.mut_committed_indices().push(0);
                            let mut proxy_e = ProxyError::new();
                            proxy_e.set_grpc_error(format!("{:?}", e));
                            resp.mut_errors().push(proxy_e);
                        }
                    }
                }
                Ok(resp)
            });
        let h = self.pool.spawn_handle(f);
        ctx.spawn(h.and_then(|resp| {
            sink.success(resp)
                .map_err(|e| warn!("GetCommittedIndexAndTs sends response fail: {:?}", e))
        }));
    }

    fn transaction_write(
        &mut self,
        ctx: RpcContext<'_>,
        req: TransactionWriteRequest,
        sink: UnarySink<TransactionWriteResponse>,
    ) {
        enum Res {
            GrpcError((GrpcError, u64)),
            RegionError(errorpb::Error),
            NoError(),
        }
        info!("Start transaction write!");
        // unimplemented!();
            let mut prewrite_futures = Vec::new();
            let mut commit_futures = Vec::new();
            let mut transaction_write_resp = TransactionWriteResponse::default();
            let pre_writes = req.get_pre_writes().clone().to_vec();
            for pre_write_req in pre_writes.clone() {
                let proxy_ctx = pre_write_req.get_context();
                let store_id = proxy_ctx.get_peer().get_store_id();
                let need_prewrite = pre_write_req.get_need();
                if !need_prewrite {
                    continue;
                }
                info!("getting connection for {}", store_id);
                // get cache client
                let client = match self.get_client(store_id) {
                    Some(c) => c,
                    None => {
                        let (cb, f) = paired_future_callback();
                        let addr = match self
                            .store_resolver
                            .resolve(store_id, cb)
                            .and_then(|_| f.wait().unwrap())
                        {
                            Ok(a) => a,
                            Err(e) => {
                                warn!("proxy pre write resolve {} fail: {:?}", store_id, e);
                                let code = RpcStatusCode::INTERNAL;
                                let status = RpcStatus::new(code, Some(format!("{}", e)));
                                ctx.spawn(sink.fail(status).map_err(|_| ()));
                                return;
                            }
                        };
                        info!("proxy pre write resolved {} to {}", store_id, addr);
                        let env = Arc::new(Environment::new(4));
                        let channel = ChannelBuilder::new(env).connect(&addr);
                        let c = TikvClient::new(channel);
                        info!(
                            "proxy pre write connected to [{}, {}]",
                            store_id, addr
                        );
                        let mut clients = self.clients.write().unwrap();
                        clients.insert(store_id, c.clone());
                        drop(clients);
                        c
                    }
                };

                let f = future::result(client.kv_prewrite_async(&pre_write_req))
                        .flatten()
                        .map(|resp| {
                            return resp;
                        })
                        .or_else(move |e| future::ok::<_, ()>(PrewriteResponse::default()));
                prewrite_futures.push(f);
            }
            let f = stream::futures_ordered(prewrite_futures)
                    .collect()
                    .and_then(|resp_vec| {
                        let mut pre_write_resp_vec = Vec::new();
                        for resp in resp_vec {
                            if resp.has_region_error() || resp.get_errors().is_empty() {
                                pre_write_resp_vec.push(resp);
                            }
                        }
                        transaction_write_resp.set_pre_write_resps(protobuf::RepeatedField::from_vec(pre_write_resp_vec));
                        Ok(transaction_write_resp)
                    });
            let h = self.pool.spawn_handle(f);
            ctx.spawn(h.and_then(|resp| {
                sink.success(resp)
                    .map_err(|e| {
                        warn!("proxy pre write sends response fail: {:?}", e);
                    })
                    .map(|_| {
                        info!("proxy pre write sends response success");
                    })
                }));
            
            // commit
            let len = pre_writes.len();
            for i in 0..len {
                let pre_write_req = pre_writes[i];
                let proxy_ctx = pre_write_req.get_context().clone();
                let start_version = pre_write_req.get_start_version().clone();
                // TODO get commit version from pd
                let mutations = pre_write_req.get_mutations().clone();
                let mut keys: Vec<Vec<u8>> = Vec::new();
                for m in mutations {
                    let k = m.get_key().clone().to_vec();
                    keys.push(k);
                }
                let store_id = proxy_ctx.get_peer().get_store_id();
                // get cache client
                let client = match self.get_client(store_id) {
                    Some(c) => c,
                    None => {
                        let (cb, f) = paired_future_callback();
                        let addr = match self
                            .store_resolver
                            .resolve(store_id, cb)
                            .and_then(|_| f.wait().unwrap()){
                                Ok(a) => a,
                                Err(e) => {
                                    warn!("proxy commit resolve {} fail: {:?}", store_id, e);
                                    let code = RpcStatusCode::INTERNAL;
                                    let status = RpcStatus::new(code, Some(format!("{}", e)));
                                    ctx.spawn(sink.fail(status).map_err(|_| ()));
                                    return;
                                }
                            };
                        info!("proxy commit resolved {} to {}", store_id, addr);
                        let env = Arc::new(Environment::new(4));
                        let channel = ChannelBuilder::new(env).connect(&addr);
                        let c = TikvClient::new(channel);
                        info!(
                            "proxy commit connected to [{}, {}]",
                            store_id, addr
                        );
                        let mut clients = self.clients.write().unwrap();
                        clients.insert(store_id, c.clone());
                        drop(clients);
                        c
                    }
                };
                let commit_req = CommitRequest::new();
                commit_req.set_context(proxy_ctx.clone());
                commit_req.set_start_version(start_version);
                commit_req.set_keys(protobuf::RepeatedField::from_vec(keys));
                let f = future::result(client.kv_commit_async(&commit_req))
                    .flatten()
                    .map(|resp| {
                       return resp;
                    })
                    .or_else(move |e| future::ok::<_, ()>(CommitResponse::default()));
                if i == 0 {
                    // only return first commit resp
                    f.wait()
                    .and_then(|resp| {
                        transaction_write_resp.set_commit_resp(resp.clone());
                        if resp.has_region_error() || resp.has_error() {
                            // TODO return
                        }
                        Ok(transaction_write_resp)
                    });
                } else {
                    commit_futures.push(f);
                }
            }
            ctx.spawn(sink.success(transaction_write_resp)
                    .map_err(|e| {
                        warn!("proxy commit sends response fail: {:?}", e);
                    })
                    .map(|_| {
                        info!("proxy commit sends response success");
                    }));
    }
}
