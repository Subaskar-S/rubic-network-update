use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::Instant;
use uuid::Uuid;

use crate::connection_pool::{ConnectionPool, PooledConnection};
use crate::error::{NetworkError, NetworkResult};
use store::get_db_path;
use store::sqlite::peer::{create_peer, remove_blacklist};

#[derive(Debug, Clone)]
pub struct PeerStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub avg_response_time_ms: f64,
    pub last_seen: SystemTime,
    pub uptime_percentage: f64,
}

impl Default for PeerStats {
    fn default() -> Self {
        Self {
            total_requests: 0,
            successful_requests: 0,
            failed_requests: 0,
            avg_response_time_ms: 0.0,
            last_seen: UNIX_EPOCH,
            uptime_percentage: 100.0,
        }
    }
}

#[derive(Debug)]
pub struct AsyncPeer {
    id: String,
    addr: SocketAddr,
    nick: Arc<RwLock<String>>,
    whitelisted: AtomicBool,
    ping_time: AtomicU32,
    last_responded: Arc<RwLock<SystemTime>>,
    connected: AtomicBool,
    connection_pool: Arc<ConnectionPool>,
    stats: Arc<RwLock<PeerStats>>,
    created_at: Instant,
}

impl Clone for AsyncPeer {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            addr: self.addr,
            nick: Arc::clone(&self.nick),
            whitelisted: AtomicBool::new(self.whitelisted.load(Ordering::Relaxed)),
            ping_time: AtomicU32::new(self.ping_time.load(Ordering::Relaxed)),
            last_responded: Arc::clone(&self.last_responded),
            connected: AtomicBool::new(self.connected.load(Ordering::Relaxed)),
            connection_pool: Arc::clone(&self.connection_pool),
            stats: Arc::clone(&self.stats),
            created_at: self.created_at,
        }
    }
}

impl AsyncPeer {
    pub async fn new(
        addr: SocketAddr,
        nick: &str,
        connection_pool: Arc<ConnectionPool>,
    ) -> NetworkResult<Self> {
        let id = Uuid::new_v4().to_string();
        let addr_str = addr.to_string();

        // Create peer in database
        create_peer(
            get_db_path().as_str(),
            &id,
            &addr_str,
            nick,
            9999,
            false,
            UNIX_EPOCH,
        )
        .map_err(|e| NetworkError::Database(e))?;

        // Remove from blacklist if present
        let _ = remove_blacklist(get_db_path().as_str(), &addr_str);

        let peer = Self {
            id,
            addr,
            nick: Arc::new(RwLock::new(nick.to_string())),
            whitelisted: AtomicBool::new(false),
            ping_time: AtomicU32::new(9999),
            last_responded: Arc::new(RwLock::new(UNIX_EPOCH)),
            connected: AtomicBool::new(false),
            connection_pool,
            stats: Arc::new(RwLock::new(PeerStats::default())),
            created_at: Instant::now(),
        };

        Ok(peer)
    }

    pub fn get_id(&self) -> &str {
        &self.id
    }

    pub fn get_addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn get_nick(&self) -> String {
        self.nick.read().await.clone()
    }

    pub async fn set_nick(&self, nick: String) {
        *self.nick.write().await = nick;
    }

    pub fn get_ping_time(&self) -> u32 {
        self.ping_time.load(Ordering::Relaxed)
    }

    pub fn set_ping_time(&self, ping: u32) {
        self.ping_time.store(ping, Ordering::Relaxed);
    }

    pub fn is_whitelisted(&self) -> bool {
        self.whitelisted.load(Ordering::Relaxed)
    }

    pub fn set_whitelisted(&self, whitelisted: bool) {
        self.whitelisted.store(whitelisted, Ordering::Relaxed);
    }

    pub fn is_connected(&self) -> bool {
        self.connected.load(Ordering::Relaxed)
    }

    pub fn set_connected(&self, connected: bool) {
        self.connected.store(connected, Ordering::Relaxed);
    }

    pub async fn get_last_responded(&self) -> SystemTime {
        *self.last_responded.read().await
    }

    pub async fn update_last_responded(&self) {
        *self.last_responded.write().await = SystemTime::now();
    }

    pub async fn get_connection(&self) -> NetworkResult<PooledConnection> {
        self.connection_pool.get_connection(self.addr).await
    }

    pub async fn return_connection(&self, connection: PooledConnection) {
        self.connection_pool.return_connection(connection).await;
    }

    pub async fn health_check(&self) -> bool {
        match self.get_connection().await {
            Ok(connection) => {
                self.return_connection(connection).await;
                self.set_connected(true);
                true
            }
            Err(_) => {
                self.set_connected(false);
                false
            }
        }
    }

    pub async fn get_stats(&self) -> PeerStats {
        self.stats.read().await.clone()
    }

    pub async fn update_stats<F>(&self, updater: F)
    where
        F: FnOnce(&mut PeerStats),
    {
        let mut stats = self.stats.write().await;
        updater(&mut *stats);
    }

    pub fn get_uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub async fn calculate_score(&self) -> f64 {
        let stats = self.get_stats().await;
        let ping = self.get_ping_time() as f64;
        let uptime = stats.uptime_percentage;
        let success_rate = if stats.total_requests > 0 {
            (stats.successful_requests as f64) / (stats.total_requests as f64) * 100.0
        } else {
            100.0
        };

        // Simple scoring algorithm: lower ping is better, higher uptime and success rate is better
        let ping_score = if ping > 0.0 { 1000.0 / ping } else { 0.0 };
        let combined_score = (ping_score * 0.3) + (uptime * 0.4) + (success_rate * 0.3);

        combined_score
    }
}