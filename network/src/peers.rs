use dashmap::DashMap;
use rand::prelude::IteratorRandom;
use rand::thread_rng;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::interval;

use api::header::EntityType;
use api::request::QubicApiPacket;
use logger::debug;
use store;

use crate::connection_pool::{ConnectionPool, PoolConfig};
use crate::error::{NetworkError, NetworkResult};
use crate::peer::{AsyncPeer, PeerStats};
use crate::worker::AsyncWorkerManager;

#[derive(Debug, Clone)]
pub enum LoadBalancingStrategy {
    RoundRobin,
    Random,
    LeastConnections,
    ScoreBased,
}

#[derive(Debug, Clone)]
pub struct PeerSetConfig {
    pub max_peers: usize,
    pub connection_timeout: Duration,
    pub health_check_interval: Duration,
    pub load_balancing: LoadBalancingStrategy,
    pub enable_connection_pooling: bool,
    pub pool_config: PoolConfig,
}

impl Default for PeerSetConfig {
    fn default() -> Self {
        Self {
            max_peers: 50,
            connection_timeout: Duration::from_secs(5),
            health_check_interval: Duration::from_secs(30),
            load_balancing: LoadBalancingStrategy::ScoreBased,
            enable_connection_pooling: true,
            pool_config: PoolConfig::default(),
        }
    }
}

pub struct AsyncPeerSet {
    peers: Arc<DashMap<String, AsyncPeer>>,
    addr_to_id: Arc<DashMap<SocketAddr, String>>,
    connection_pool: Arc<ConnectionPool>,
    worker_manager: Arc<AsyncWorkerManager>,
    config: PeerSetConfig,
    round_robin_counter: AtomicU32,
    request_sender: broadcast::Sender<QubicApiPacket>,
    _request_receiver: broadcast::Receiver<QubicApiPacket>,
}

impl AsyncPeerSet {
    pub fn new(config: PeerSetConfig) -> Self {
        let connection_pool = Arc::new(ConnectionPool::new(config.pool_config.clone()));
        let worker_manager = Arc::new(AsyncWorkerManager::new(Arc::clone(&connection_pool)));
        let (request_sender, request_receiver) = broadcast::channel(1000);

        Self {
            peers: Arc::new(DashMap::new()),
            addr_to_id: Arc::new(DashMap::new()),
            connection_pool,
            worker_manager,
            config,
            round_robin_counter: AtomicU32::new(0),
            request_sender,
            _request_receiver: request_receiver,
        }
    }

    pub async fn add_peer(&self, addr: SocketAddr, nick: &str) -> NetworkResult<String> {
        // Check peer limit
        if self.peers.len() >= self.config.max_peers {
            return Err(NetworkError::PeerLimitReached);
        }

        // Check if peer already exists
        if self.addr_to_id.contains_key(&addr) {
            return Err(NetworkError::ConnectionFailed(
                "Peer already exists".to_string(),
            ));
        }

        // Create new async peer
        let peer = AsyncPeer::new(addr, nick, Arc::clone(&self.connection_pool)).await?;
        let peer_id = peer.get_id().to_string();

        // Test connection
        if !peer.health_check().await {
            return Err(NetworkError::ConnectionFailed(format!(
                "Failed to connect to {}",
                addr
            )));
        }

        // Store peer
        self.addr_to_id.insert(addr, peer_id.clone());
        self.peers.insert(peer_id.clone(), peer.clone());

        // Start worker for this peer
        self.worker_manager
            .start_worker(&peer_id, peer, self.request_sender.subscribe())
            .await;

        // Update database
        match store::sqlite::peer::set_peer_connected(store::get_db_path().as_str(), &peer_id) {
            Ok(_) => debug(&format!("Added and connected peer: {}", peer_id)),
            Err(err) => debug(&format!("Error updating peer status: {}", err)),
        }

        Ok(peer_id)
    }

    pub async fn get_peer_ids(&self) -> Vec<String> {
        self.peers.iter().map(|entry| entry.key().clone()).collect()
    }

    pub async fn remove_peer(&self, peer_id: &str) -> bool {
        if let Some((_, peer)) = self.peers.remove(peer_id) {
            // Remove from address mapping
            self.addr_to_id.remove(&peer.get_addr());

            // Stop worker
            self.worker_manager.stop_worker(peer_id).await;

            // Update database
            let _ =
                store::sqlite::peer::set_peer_disconnected(store::get_db_path().as_str(), peer_id);

            debug(&format!("Removed peer: {}", peer_id));
            true
        } else {
            false
        }
    }

    pub async fn remove_peer_by_addr(&self, addr: SocketAddr) -> bool {
        if let Some((_, peer_id)) = self.addr_to_id.remove(&addr) {
            self.remove_peer(&peer_id).await
        } else {
            false
        }
    }

    pub async fn make_request(&self, request: QubicApiPacket) -> NetworkResult<()> {
        if self.peers.is_empty() {
            return Err(NetworkError::ConnectionFailed(
                "No peers available".to_string(),
            ));
        }

        // Determine if we should broadcast to all peers or use load balancing
        let broadcast_all = matches!(
            request.api_type,
            EntityType::RequestCurrentTickInfo
                | EntityType::RequestedQuorumTick
                | EntityType::RequestTickData
                | EntityType::RequestContractFunction
                | EntityType::RequestAssets
        );

        if broadcast_all {
            // Broadcast to all connected peers
            let _ = self.request_sender.send(request);
        } else {
            // Use load balancing to select optimal peer
            if let Some(selected_peer) = self.select_peer_for_request().await {
                let mut targeted_request = request;
                targeted_request.peer = Some(selected_peer.get_id().to_string());
                let _ = self.request_sender.send(targeted_request);
            } else {
                return Err(NetworkError::ConnectionFailed(
                    "No healthy peers available".to_string(),
                ));
            }
        }

        Ok(())
    }

    async fn select_peer_for_request(&self) -> Option<AsyncPeer> {
        let connected_peers: Vec<AsyncPeer> = self
            .peers
            .iter()
            .filter(|entry| entry.value().is_connected())
            .map(|entry| entry.value().clone())
            .collect();

        if connected_peers.is_empty() {
            return None;
        }

        match self.config.load_balancing {
            LoadBalancingStrategy::Random => connected_peers.into_iter().choose(&mut thread_rng()),
            LoadBalancingStrategy::RoundRobin => {
                let index = self.round_robin_counter.fetch_add(1, Ordering::Relaxed) as usize;
                connected_peers.get(index % connected_peers.len()).cloned()
            }
            LoadBalancingStrategy::LeastConnections => {
                // For now, just return random - would need connection count tracking
                connected_peers.into_iter().choose(&mut thread_rng())
            }
            LoadBalancingStrategy::ScoreBased => {
                let mut best_peer: Option<AsyncPeer> = None;
                let mut best_score = 0.0;

                for peer in connected_peers {
                    let score = peer.calculate_score().await;
                    if score > best_score {
                        best_score = score;
                        best_peer = Some(peer);
                    }
                }

                best_peer
            }
        }
    }

    pub fn get_peer_count(&self) -> usize {
        self.peers.len()
    }

    pub fn get_connected_peer_count(&self) -> usize {
        self.peers
            .iter()
            .filter(|entry| entry.value().is_connected())
            .count()
    }

    pub async fn get_peer_stats(&self) -> HashMap<String, PeerStats> {
        let mut stats = HashMap::new();
        for entry in self.peers.iter() {
            let peer_id = entry.key().clone();
            let peer_stats = entry.value().get_stats().await;
            stats.insert(peer_id, peer_stats);
        }
        stats
    }

    pub async fn health_check_all_peers(&self) {
        let mut unhealthy_peers = Vec::new();

        for entry in self.peers.iter() {
            let peer_id = entry.key().clone();
            let peer = entry.value();

            if !peer.health_check().await {
                unhealthy_peers.push(peer_id);
            }
        }

        // Remove unhealthy peers
        for peer_id in unhealthy_peers {
            self.remove_peer(&peer_id).await;
        }
    }

    pub fn start_health_checker(&self) -> tokio::task::JoinHandle<()> {
        let peer_set = Arc::new(self.peers.clone());
        let worker_manager = Arc::clone(&self.worker_manager);
        let health_check_interval = self.config.health_check_interval;

        tokio::spawn(async move {
            let mut interval = interval(health_check_interval);

            loop {
                interval.tick().await;

                let mut unhealthy_peers = Vec::new();

                for entry in peer_set.iter() {
                    let peer_id = entry.key().clone();
                    let peer = entry.value();

                    if !peer.health_check().await {
                        unhealthy_peers.push(peer_id);
                    }
                }

                // Remove unhealthy peers
                for peer_id in unhealthy_peers {
                    peer_set.remove(&peer_id);
                    worker_manager.stop_worker(&peer_id).await;

                    let _ = store::sqlite::peer::set_peer_disconnected(
                        store::get_db_path().as_str(),
                        &peer_id,
                    );
                }
            }
        })
    }

    pub async fn get_best_peers(&self, count: usize) -> Vec<AsyncPeer> {
        let mut peers_with_scores = Vec::new();

        for entry in self.peers.iter() {
            if entry.value().is_connected() {
                let score = entry.value().calculate_score().await;
                peers_with_scores.push((entry.value().clone(), score));
            }
        }

        // Sort by score (descending)
        peers_with_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());

        peers_with_scores
            .into_iter()
            .take(count)
            .map(|(peer, _)| peer)
            .collect()
    }

    pub async fn reconnect_peer(&self, peer_id: &str) -> NetworkResult<()> {
        if let Some(entry) = self.peers.get(peer_id) {
            let peer = entry.value().clone();

            if peer.health_check().await {
                // Restart worker if health check passes
                self.worker_manager
                    .start_worker(peer_id, peer, self.request_sender.subscribe())
                    .await;
                Ok(())
            } else {
                Err(NetworkError::ConnectionFailed(
                    "Health check failed".to_string(),
                ))
            }
        } else {
            Err(NetworkError::ConnectionFailed("Peer not found".to_string()))
        }
    }
}

// Compatibility wrapper for the old synchronous PeerSet
use std::sync::Mutex;

pub struct PeerSet {
    async_peer_set: Arc<AsyncPeerSet>,
    runtime: Arc<tokio::runtime::Runtime>,
    request_matcher: Arc<Mutex<HashMap<u32, QubicApiPacket>>>,
    req_channel: (spmc::Sender<QubicApiPacket>, spmc::Receiver<QubicApiPacket>),
    threads: HashMap<String, std::thread::JoinHandle<()>>,
}

impl PeerSet {
    pub fn new() -> Self {
        let runtime = Arc::new(tokio::runtime::Runtime::new().expect("Failed to create Tokio runtime"));
        let config = PeerSetConfig::default();
        let async_peer_set = Arc::new(AsyncPeerSet::new(config));

        Self {
            async_peer_set,
            runtime,
            request_matcher: Arc::new(Mutex::new(HashMap::new())),
            req_channel: spmc::channel::<QubicApiPacket>(),
            threads: HashMap::new(),
        }
    }

    pub fn get_peers(&self) -> Vec<String> {
        // Return peer IDs as strings since we can't return references to async peers
        self.runtime.block_on(async {
            self.async_peer_set.get_peer_ids().await
        })
    }

    pub fn get_peer_ids(&self) -> Vec<String> {
        self.runtime.block_on(async {
            self.async_peer_set.get_peer_ids().await
        })
    }

    pub fn num_peers(&self) -> usize {
        self.async_peer_set.peers.len()
    }

    pub fn add_peer(&mut self, ip: &str) -> Result<(), String> {
        let addr: std::net::SocketAddr = ip.parse()
            .map_err(|_| "Invalid IP address format".to_string())?;

        self.runtime.block_on(async {
            match self.async_peer_set.add_peer(addr, "").await {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        })
    }

    pub fn delete_peer_by_id(&mut self, id: &str) -> bool {
        self.runtime.block_on(async {
            self.async_peer_set.remove_peer(id).await
        })
    }

    pub fn make_request(&mut self, request: QubicApiPacket) -> Result<(), String> {
        self.runtime.block_on(async {
            match self.async_peer_set.make_request(request).await {
                Ok(_) => Ok(()),
                Err(e) => Err(e.to_string()),
            }
        })
    }
}
