use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, RwLock};
use tokio::time::{sleep, timeout};

use api::request::QubicApiPacket;
use store::get_db_path;
use store::sqlite::peer::set_peer_disconnected;

use crate::connection_pool::ConnectionPool;
use crate::error::{NetworkError, NetworkResult};
use crate::peer::AsyncPeer;
use crate::tcp_recv::async_qubic_tcp_receive_data;

pub struct WorkerTask {
    peer_id: String,
    task_handle: tokio::task::JoinHandle<()>,
}

pub struct AsyncWorkerManager {
    workers: Arc<DashMap<String, WorkerTask>>,
    connection_pool: Arc<ConnectionPool>,
    request_matcher: Arc<RwLock<HashMap<u32, QubicApiPacket>>>,
}

impl AsyncWorkerManager {
    pub fn new(connection_pool: Arc<ConnectionPool>) -> Self {
        Self {
            workers: Arc::new(DashMap::new()),
            connection_pool,
            request_matcher: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn start_worker(
        &self,
        peer_id: &str,
        peer: AsyncPeer,
        request_receiver: broadcast::Receiver<QubicApiPacket>,
    ) {
        // Stop existing worker if any
        self.stop_worker(peer_id).await;

        let peer_id_clone = peer_id.to_string();
        let workers = Arc::clone(&self.workers);
        let request_matcher = Arc::clone(&self.request_matcher);

        let task_handle = tokio::spawn(async move {
            Self::worker_loop(peer, request_receiver, request_matcher).await;

            // Clean up when worker exits
            workers.remove(&peer_id_clone);
        });

        let worker_task = WorkerTask {
            peer_id: peer_id.to_string(),
            task_handle,
        };

        self.workers.insert(peer_id.to_string(), worker_task);
        logger::debug(&format!("Started async worker for peer: {}", peer_id));
    }

    pub async fn stop_worker(&self, peer_id: &str) {
        if let Some((_, worker_task)) = self.workers.remove(peer_id) {
            worker_task.task_handle.abort();
            logger::debug(&format!("Stopped worker for peer: {}", peer_id));
        }
    }

    async fn worker_loop(
        peer: AsyncPeer,
        mut request_receiver: broadcast::Receiver<QubicApiPacket>,
        request_matcher: Arc<RwLock<HashMap<u32, QubicApiPacket>>>,
    ) {
        let peer_id = peer.get_id().to_string();
        let mut consecutive_failures = 0;
        const MAX_CONSECUTIVE_FAILURES: u32 = 5;
        const BACKOFF_BASE_DURATION: Duration = Duration::from_millis(100);

        loop {
            // Receive request with timeout to allow periodic health checks
            let request_result = timeout(Duration::from_secs(30), request_receiver.recv()).await;

            match request_result {
                Ok(Ok(request)) => {
                    // Check if this request is targeted to this peer or is a broadcast
                    if let Some(ref target_peer) = request.peer {
                        if target_peer != &peer_id {
                            continue; // Skip requests not for this peer
                        }
                    }

                    let start_time = Instant::now();

                    // Store request for response matching
                    {
                        let mut matcher = request_matcher.write().await;
                        matcher.insert(request.header._dejavu, request.clone());
                    }

                    // Process the request
                    let result = Self::handle_request(&peer, request).await;
                    let processing_time = start_time.elapsed();

                    // Update peer statistics
                    peer.update_stats(|stats| {
                        stats.total_requests += 1;
                        match result {
                            Ok(_) => {
                                stats.successful_requests += 1;
                                consecutive_failures = 0;
                            }
                            Err(_) => {
                                stats.failed_requests += 1;
                                consecutive_failures += 1;
                            }
                        }

                        // Update average response time using exponential moving average
                        let alpha = 0.1; // Smoothing factor
                        stats.avg_response_time_ms = alpha * processing_time.as_millis() as f64
                            + (1.0 - alpha) * stats.avg_response_time_ms;
                    })
                    .await;

                    // Handle consecutive failures with exponential backoff
                    if consecutive_failures > 0 {
                        let backoff_duration =
                            BACKOFF_BASE_DURATION * 2_u32.pow(consecutive_failures.min(10)); // Cap at 2^10
                        sleep(backoff_duration).await;
                    }

                    // Disconnect peer if too many consecutive failures
                    if consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
                        logger::error!(
                            "Peer {} has {} consecutive failures, disconnecting",
                            peer_id,
                            consecutive_failures
                        );
                        peer.set_connected(false);
                        let _ = set_peer_disconnected(get_db_path().as_str(), &peer_id);
                        break;
                    }
                }
                Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                    // Channel lagged, continue processing
                    logger::debug!("Worker for peer {} lagged, continuing", peer_id);
                    continue;
                }
                Ok(Err(broadcast::error::RecvError::Closed)) => {
                    // Channel closed, exit worker
                    logger::debug!(
                        "Request channel closed, stopping worker for peer {}",
                        peer_id
                    );
                    break;
                }
                Err(_) => {
                    // Timeout occurred, perform health check
                    if !peer.health_check().await {
                        logger::debug!("Health check failed for peer {}, stopping worker", peer_id);
                        break;
                    }
                }
            }
        }

        logger::debug!("Worker loop exited for peer: {}", peer_id);
    }

    async fn handle_request(peer: &AsyncPeer, mut request: QubicApiPacket) -> NetworkResult<()> {
        let mut connection = peer.get_connection().await?;
        let request_bytes = request.as_bytes();

        // Send request
        if let Err(e) = Self::send_data(&mut connection.stream, &request_bytes).await {
            // Don't return connection on error
            return Err(e);
        }

        // Receive and process response
        let response_result = async_qubic_tcp_receive_data(peer, &mut connection.stream).await;

        // Return connection to pool
        peer.return_connection(connection).await;

        // Update last responded time
        peer.update_last_responded().await;

        response_result
    }

    async fn send_data(stream: &mut tokio::net::TcpStream, data: &[u8]) -> NetworkResult<()> {
        use tokio::io::AsyncWriteExt;

        stream
            .write_all(data)
            .await
            .map_err(|e| NetworkError::Io(e))?;

        stream.flush().await.map_err(|e| NetworkError::Io(e))?;

        Ok(())
    }

    pub fn get_worker_count(&self) -> usize {
        self.workers.len()
    }

    pub fn get_worker_ids(&self) -> Vec<String> {
        self.workers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub async fn stop_all_workers(&self) {
        let worker_ids: Vec<String> = self.get_worker_ids();

        for worker_id in worker_ids {
            self.stop_worker(&worker_id).await;
        }

        logger::debug!("Stopped all workers");
    }

    pub async fn restart_worker(
        &self,
        peer_id: &str,
        peer: AsyncPeer,
        request_receiver: broadcast::Receiver<QubicApiPacket>,
    ) {
        self.stop_worker(peer_id).await;
        self.start_worker(peer_id, peer, request_receiver).await;
        logger::debug!("Restarted worker for peer: {}", peer_id);
    }

    // Cleanup old request matchers periodically
    pub fn start_matcher_cleanup(&self) -> tokio::task::JoinHandle<()> {
        let request_matcher = Arc::clone(&self.request_matcher);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300)); // 5 minutes

            loop {
                interval.tick().await;

                let mut matcher = request_matcher.write().await;
                // Keep only recent requests (this is a simple cleanup,
                // in production you'd want timestamp-based cleanup)
                if matcher.len() > 10000 {
                    matcher.clear();
                    logger::debug!("Cleaned up request matcher cache");
                }
            }
        })
    }
}
