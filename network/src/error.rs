use thiserror::Error;

pub type NetworkResult<T> = Result<T, NetworkError>;

#[derive(Error, Debug)]
pub enum NetworkError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),
    
    #[error("Connection timeout")]
    Timeout,
    
    #[error("Invalid response")]
    InvalidResponse,
    
    #[error("Peer limit reached")]
    PeerLimitReached,
    
    #[error("Database error: {0}")]
    Database(String),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Network error: {0}")]
    Network(String),
    
    #[error("Peer not found")]
    PeerNotFound,
    
    #[error("Connection pool exhausted")]
    PoolExhausted,
    
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

impl NetworkError {
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            NetworkError::Timeout
                | NetworkError::ConnectionFailed(_)
                | NetworkError::PoolExhausted
                | NetworkError::Network(_)
        )
    }
    
    pub fn is_permanent(&self) -> bool {
        matches!(
            self,
            NetworkError::PeerLimitReached
                | NetworkError::InvalidConfig(_)
                | NetworkError::PeerNotFound
        )
    }
}
