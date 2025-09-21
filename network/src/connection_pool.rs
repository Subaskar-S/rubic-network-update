use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, Semaphore};
use tokio::time::timeout;

use crate::error::{NetworkError, NetworkResult};

#[derive(Debug, Clone)]
pub struct PoolConfig {
    pub max_connections_per_peer: usize,
    pub connection_timeout: Duration,
    pub idle_timeout: Duration,
    pub max_idle_connections: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_connections_per_peer: 5,
            connection_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(60),
            max_idle_connections: 10,
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    pub stream: TcpStream,
    pub created_at: Instant,
    pub last_used: Instant,
    pub addr: SocketAddr,
}

impl PooledConnection {
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        let now = Instant::now();
        Self {
            stream,
            created_at: now,
            last_used: now,
            addr,
        }
    }
    
    pub fn update_last_used(&mut self) {
        self.last_used = Instant::now();
    }
    
    pub fn is_expired(&self, idle_timeout: Duration) -> bool {
        self.last_used.elapsed() > idle_timeout
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    config: PoolConfig,
    connections: Arc<Mutex<VecDeque<PooledConnection>>>,
    active_connections: AtomicUsize,
    semaphore: Arc<Semaphore>,
}

impl ConnectionPool {
    pub fn new(config: PoolConfig) -> Self {
        let semaphore = Arc::new(Semaphore::new(config.max_connections_per_peer));
        
        Self {
            config,
            connections: Arc::new(Mutex::new(VecDeque::new())),
            active_connections: AtomicUsize::new(0),
            semaphore,
        }
    }
    
    pub async fn get_connection(&self, addr: SocketAddr) -> NetworkResult<PooledConnection> {
        // Acquire semaphore permit
        let _permit = self.semaphore
            .acquire()
            .await
            .map_err(|_| NetworkError::PoolExhausted)?;
        
        // Try to get an existing connection from the pool
        if let Some(mut connection) = self.get_pooled_connection(addr).await {
            connection.update_last_used();
            self.active_connections.fetch_add(1, Ordering::Relaxed);
            return Ok(connection);
        }
        
        // Create a new connection
        self.create_new_connection(addr).await
    }
    
    async fn get_pooled_connection(&self, addr: SocketAddr) -> Option<PooledConnection> {
        let mut connections = self.connections.lock().await;
        
        // Remove expired connections
        connections.retain(|conn| !conn.is_expired(self.config.idle_timeout));
        
        // Find a connection for the specific address
        if let Some(pos) = connections.iter().position(|conn| conn.addr == addr) {
            connections.remove(pos)
        } else {
            None
        }
    }
    
    async fn create_new_connection(&self, addr: SocketAddr) -> NetworkResult<PooledConnection> {
        let stream = timeout(
            self.config.connection_timeout,
            TcpStream::connect(addr)
        )
        .await
        .map_err(|_| NetworkError::Timeout)?
        .map_err(NetworkError::Io)?;
        
        self.active_connections.fetch_add(1, Ordering::Relaxed);
        Ok(PooledConnection::new(stream, addr))
    }
    
    pub async fn return_connection(&self, mut connection: PooledConnection) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
        
        // Check if connection is still valid and not expired
        if !connection.is_expired(self.config.idle_timeout) {
            connection.update_last_used();
            
            let mut connections = self.connections.lock().await;
            
            // Only keep up to max_idle_connections
            if connections.len() < self.config.max_idle_connections {
                connections.push_back(connection);
            }
        }
        // Connection is dropped if expired or pool is full
    }
    
    pub async fn cleanup_expired_connections(&self) {
        let mut connections = self.connections.lock().await;
        let initial_count = connections.len();
        connections.retain(|conn| !conn.is_expired(self.config.idle_timeout));
        let removed_count = initial_count - connections.len();
        
        if removed_count > 0 {
            logger::debug(&format!("Cleaned up {} expired connections", removed_count));
        }
    }
    
    pub fn get_active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Relaxed)
    }
    
    pub async fn get_pooled_connections(&self) -> usize {
        self.connections.lock().await.len()
    }
    
    pub fn get_config(&self) -> &PoolConfig {
        &self.config
    }
}

// Cleanup task that runs periodically
impl ConnectionPool {
    pub async fn start_cleanup_task(self: Arc<Self>) {
        let pool = Arc::clone(&self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            
            loop {
                interval.tick().await;
                pool.cleanup_expired_connections().await;
            }
        });
    }
}
