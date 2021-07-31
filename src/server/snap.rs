// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Display, Formatter};
use std::fs::{self, File};
use std::io::{Read, Write};
use std::marker::Unpin;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::{Future, TryFutureExt};
use futures::sink::{Sink, SinkExt};
use futures::stream::TryStreamExt;
use grpcio::{
    ChannelBuilder, ClientStreamingSink, Environment, RequestStream, RpcStatus, RpcStatusCode,
    WriteFlags,
};
use kvproto::raft_serverpb::{Done, RaftMessage, RaftSnapshotData, SnapshotChunk};
use kvproto::tikvpb::TikvClient;
use protobuf::Message;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

use engine_rocks::RocksEngine;
use raftstore::router::RaftStoreRouter;
use raftstore::store::{SnapEntry, SnapKey, SnapManager};
use security::SecurityManager;
use tikv_util::worker::Runnable;
use tikv_util::DeferContext;

use super::metrics::*;
use super::{Config, Error, Result};

pub type Callback = Box<dyn FnOnce(Result<()>) + Send>;

const DEFAULT_POOL_SIZE: usize = 4;

/// A task for either receiving Snapshot or sending Snapshot
pub enum Task {
    Recv {
        stream: RequestStream<SnapshotChunk>,
        sink: ClientStreamingSink<Done>,
    },
    Send {
        addr: String,
        msg: RaftMessage,
        cb: Callback,
    },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match *self {
            Task::Recv { .. } => write!(f, "Recv"),
            Task::Send {
                ref addr, ref msg, ..
            } => write!(f, "Send Snap[to: {}, snap: {:?}]", addr, msg),
        }
    }
}

async fn send_snap_files(
    mgr: &SnapManager,
    mut sender: impl Sink<(SnapshotChunk, WriteFlags), Error = Error> + Unpin,
    msg: RaftMessage,
    files: Vec<PathBuf>,
) -> Result<u64> {
    let limiter = mgr.io_limiter();
    let mut total_sent = msg.compute_size() as u64;
    let mut chunk = SnapshotChunk::default();
    debug!("sending metadata"; "metadata" => ?msg);
    chunk.set_message(msg);
    sender
        .feed((chunk, WriteFlags::default().buffer_hint(true)))
        .await?;
    for path in files {
        debug!("sending snap file"; "file" => %path.display());
        let name = path.file_name().unwrap().to_str().unwrap();
        assert!(name.len() < 255);
        let mut buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
        buffer.push(name.len() as u8);
        buffer.extend_from_slice(name.as_bytes());
        let mut f = File::open(&path)?;
        let file_size = f.metadata()?.len();
        let mut size = 0;
        let mut off = buffer.len();
        loop {
            unsafe { buffer.set_len(SNAP_CHUNK_LEN) };
            let readed = f.read(&mut buffer[off..])?;
            // The file can be in cache, so read doesn't always trigger IO.
            limiter.consume(readed / 2);
            let new_len = readed + off;
            total_sent += new_len as u64;
            unsafe {
                buffer.set_len(new_len);
            }
            let mut chunk = SnapshotChunk::default();
            chunk.set_data(buffer);
            sender
                .feed((chunk, WriteFlags::default().buffer_hint(true)))
                .await?;
            size += readed;
            if new_len < SNAP_CHUNK_LEN {
                break;
            }
            buffer = Vec::with_capacity(SNAP_CHUNK_LEN);
            off = 0;
        }
        info!("sent snap file"; "file" => %path.display(), "size" => file_size, "sent" => size);
    }
    sender.close().await?;
    Ok(total_sent)
}

const SNAP_CHUNK_LEN: usize = 1024 * 1024;

struct SendStat {
    key: SnapKey,
    total_size: u64,
    elapsed: Duration,
}

/// Send the snapshot to specified address.
///
/// It will first send the normal raft snapshot message and then send the snapshot file.
fn send_snap(
    env: Arc<Environment>,
    mgr: SnapManager,
    security_mgr: Arc<SecurityManager>,
    cfg: &Config,
    addr: &str,
    msg: RaftMessage,
) -> Result<impl Future<Output = Result<SendStat>>> {
    assert!(msg.get_message().has_snapshot());
    let timer = Instant::now();

    let send_timer = SEND_SNAP_HISTOGRAM.start_coarse_timer();

    let key = {
        let snap = msg.get_message().get_snapshot();
        SnapKey::from_snap(snap)?
    };

    mgr.register(key.clone(), SnapEntry::Sending);
    let deregister = {
        let (mgr, key) = (mgr.clone(), key.clone());
        DeferContext::new(move || mgr.deregister(&key, &SnapEntry::Sending))
    };

    let path = mgr.get_final_name_for_build(&key);
    if !path.exists() {
        return Err(box_err!("missing snap file: {}", path.display()));
    }
    let files = fs::read_dir(&path)?
        .map(|d| Ok(d?.path()))
        .collect::<Result<Vec<_>>>()?;

    let cb = ChannelBuilder::new(env)
        .stream_initial_window_size(cfg.grpc_stream_initial_window_size.0 as i32)
        .keepalive_time(cfg.grpc_keepalive_time.0)
        .keepalive_timeout(cfg.grpc_keepalive_timeout.0)
        .default_compression_algorithm(cfg.grpc_compression_algorithm());

    let channel = security_mgr.connect(cb, addr);
    let client = TikvClient::new(channel);
    let (sink, receiver) = client.snapshot()?;

    let send_task = async move {
        let sink = sink.sink_map_err(Error::from);
        let total_size = send_snap_files(&mgr, sink, msg, files).await?;
        let recv_result = receiver.map_err(Error::from).await;
        send_timer.observe_duration();
        drop(deregister);
        drop(client);
        match recv_result {
            Ok(_) => {
                fail_point!("snapshot_delete_after_send");
                let _ = fs::remove_dir_all(path);
                // TODO: improve it after rustc resolves the bug.
                // Call `info` in the closure directly will cause rustc
                // panic with `Cannot create local mono-item for DefId`.
                Ok(SendStat {
                    key,
                    total_size,
                    elapsed: timer.elapsed(),
                })
            }
            Err(e) => Err(e),
        }
    };
    Ok(send_task)
}

fn recv_snap<R: RaftStoreRouter<RocksEngine> + 'static>(
    stream: RequestStream<SnapshotChunk>,
    sink: ClientStreamingSink<Done>,
    snap_mgr: SnapManager,
    raft_router: R,
) -> impl Future<Output = Result<()>> {
    let timer = Instant::now();
    let recv_task = async move {
        let mut stream = stream.map_err(Error::from);
        let meta = match stream.try_next().await? {
            Some(mut head) if head.has_message() => head.take_message(),
            Some(_) => return Err(box_err!("no raft message in the first chunk")),
            None => return Err(box_err!("empty grpc stream")),
        };
        debug!("received snap metadata"; "metadata" => ?meta);
        let key = match SnapKey::from_snap(meta.get_message().get_snapshot()) {
            Ok(k) => k,
            Err(e) => return Err(box_err!("failed to create snap key: {:?}", e)),
        };
        let data = meta.get_message().get_snapshot().get_data();
        let mut snapshot = RaftSnapshotData::default();
        snapshot.merge_from_bytes(data)?;

        snap_mgr.register(key.clone(), SnapEntry::Receiving);
        defer!({
            snap_mgr.deregister(&key, &SnapEntry::Receiving);
        });

        let path = snap_mgr.get_temp_path_for_build(key.region_id);
        let limiter = snap_mgr.io_limiter();
        fs::create_dir_all(&path)?;
        loop {
            let mut chunk = match stream.try_next().await? {
                Some(mut c) if !c.has_message() => c.take_data(),
                Some(_) => return Err(box_err!("duplicated metadata")),
                None => break,
            };
            let len = chunk[0] as usize;
            let file_name = box_try!(std::str::from_utf8(&chunk[1..len + 1]));
            let p = path.join(file_name);
            debug!("receiving snap file"; "file" => %p.display());
            let mut f = File::create(&p)?;
            let mut size = chunk.len() - len - 1;
            limiter.consume(size).await;
            f.write_all(&chunk[len + 1..])?;
            while chunk.len() >= SNAP_CHUNK_LEN {
                chunk = match stream.try_next().await? {
                    Some(mut c) if !c.has_message() => c.take_data(),
                    Some(_) => return Err(box_err!("duplicated metadata")),
                    None => return Err(box_err!("missing chunk")),
                };
                limiter.consume(chunk.len()).await;
                f.write_all(&chunk)?;
                size += chunk.len();
            }
            info!("received snap file"; "file" => %p.display(), "size" => size);
            f.sync_data()?;
        }
        // TODO: should sync directory.
        let final_path = snap_mgr.get_final_name_for_recv(&key);
        fs::rename(&path, snap_mgr.get_final_name_for_recv(&key))?;
        if let Err(e) = raft_router.send_raft_msg(meta) {
            return Err(box_err!("{} failed to send snapshot to raft: {}", key, e));
        }
        Ok(final_path)
    };

    async move {
        let res: Result<PathBuf> = recv_task.await;
        match res {
            Ok(p) => {
                sink.success(Done::default()).await?;
                info!("receive snap {} takes {:?}", p.display(), timer.elapsed());
                Ok(())
            }
            Err(e) => {
                // If sink can't send back error, the error is lost, so print it here.
                error!("failed to receive snapshot"; "error" => ?e);
                let status = RpcStatus::new(RpcStatusCode::UNKNOWN, Some(format!("{:?}", e)));
                sink.fail(status).await.map_err(Error::from)
            }
        }
    }
}

pub struct Runner<R: RaftStoreRouter<RocksEngine> + 'static> {
    env: Arc<Environment>,
    snap_mgr: SnapManager,
    pool: Runtime,
    raft_router: R,
    security_mgr: Arc<SecurityManager>,
    cfg: Arc<Config>,
    sending_count: Arc<AtomicUsize>,
    recving_count: Arc<AtomicUsize>,
}

impl<R: RaftStoreRouter<RocksEngine> + 'static> Runner<R> {
    pub fn new(
        env: Arc<Environment>,
        snap_mgr: SnapManager,
        r: R,
        security_mgr: Arc<SecurityManager>,
        cfg: Arc<Config>,
    ) -> Runner<R> {
        Runner {
            env,
            snap_mgr,
            pool: RuntimeBuilder::new()
                .threaded_scheduler()
                .thread_name(thd_name!("snap-sender"))
                .core_threads(DEFAULT_POOL_SIZE)
                .on_thread_start(tikv_alloc::add_thread_memory_accessor)
                .on_thread_stop(tikv_alloc::remove_thread_memory_accessor)
                .build()
                .unwrap(),
            raft_router: r,
            security_mgr,
            cfg,
            sending_count: Arc::new(AtomicUsize::new(0)),
            recving_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl<R: RaftStoreRouter<RocksEngine> + 'static> Runnable for Runner<R> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::Recv { stream, sink } => {
                let task_num = self.recving_count.load(Ordering::SeqCst);
                if task_num >= self.cfg.concurrent_recv_snap_limit {
                    warn!("too many recving snapshot tasks, ignore");
                    let status = RpcStatus::new(
                        RpcStatusCode::RESOURCE_EXHAUSTED,
                        Some(format!(
                            "the number of received snapshot tasks {} exceeded the limitation {}",
                            task_num, self.cfg.concurrent_recv_snap_limit
                        )),
                    );
                    self.pool.spawn(sink.fail(status));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.recv.inc();

                let snap_mgr = self.snap_mgr.clone();
                let raft_router = self.raft_router.clone();
                let recving_count = Arc::clone(&self.recving_count);
                recving_count.fetch_add(1, Ordering::SeqCst);
                let task = async move {
                    let result = recv_snap(stream, sink, snap_mgr, raft_router).await;
                    recving_count.fetch_sub(1, Ordering::SeqCst);
                    if let Err(e) = result {
                        error!("failed to recv snapshot"; "err" => %e);
                    }
                };
                self.pool.spawn(task);
            }
            Task::Send { addr, msg, cb } => {
                fail_point!("send_snapshot");
                if self.sending_count.load(Ordering::SeqCst) >= self.cfg.concurrent_send_snap_limit
                {
                    warn!(
                        "too many sending snapshot tasks, drop Send Snap[to: {}, snap: {:?}]",
                        addr, msg
                    );
                    cb(Err(Error::Other("Too many sending snapshot tasks".into())));
                    return;
                }
                SNAP_TASK_COUNTER_STATIC.send.inc();

                let env = Arc::clone(&self.env);
                let mgr = self.snap_mgr.clone();
                let security_mgr = Arc::clone(&self.security_mgr);
                let sending_count = Arc::clone(&self.sending_count);
                sending_count.fetch_add(1, Ordering::SeqCst);

                let send_task = send_snap(env, mgr, security_mgr, &self.cfg, &addr, msg);
                let task = async move {
                    let res = match send_task {
                        Err(e) => Err(e),
                        Ok(f) => f.await,
                    };
                    match res {
                        Ok(stat) => {
                            info!(
                                "sent snapshot";
                                "region_id" => stat.key.region_id,
                                "snap_key" => %stat.key,
                                "size" => stat.total_size,
                                "duration" => ?stat.elapsed
                            );
                            cb(Ok(()));
                        }
                        Err(e) => {
                            error!("failed to send snap"; "to_addr" => addr, "err" => ?e);
                            cb(Err(e));
                        }
                    };
                    sending_count.fetch_sub(1, Ordering::SeqCst);
                };

                self.pool.spawn(task);
            }
        }
    }
}
