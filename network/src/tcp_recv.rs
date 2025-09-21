use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;
use tokio::time::timeout;

use api::header::{EntityType, RequestResponseHeader};
use api::request::QubicApiPacket;
use store::get_db_path;
use store::sqlite::peer::set_peer_disconnected;

use crate::error::{NetworkError, NetworkResult};
use crate::peer::AsyncPeer;

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_PACKET_SIZE: usize = 1024 * 1024; // 1MB max packet size for safety
const MAX_PACKETS_PER_RESPONSE: usize = 1000; // Prevent infinite loops

pub async fn async_qubic_tcp_receive_data(
    peer: &AsyncPeer,
    stream: &mut TcpStream,
) -> NetworkResult<()> {
    // Try to read the first packet to determine if it's a multi-packet response
    match recv_single_packet(peer, stream).await {
        Ok(Some(packet)) => {
            // Check if this indicates multiple packets are coming
            if packet.header.recv_multiple_packets() {
                // We already got the first packet, now get the rest
                let mut packets = vec![packet];
                let mut additional_packets = recv_multiple_packets(peer, stream).await?;
                packets.append(&mut additional_packets);

                // Process multiple packets
                if !packets.is_empty() {
                    logger::debug(&format!(
                        "Received {} packets from peer {}",
                        packets.len(),
                        peer.get_id()
                    ));
                }
            } else {
                // Single packet response
                logger::debug(&format!(
                    "Received single packet from peer {}",
                    peer.get_id()
                ));
            }
        }
        Ok(None) => {
            // No valid packet received
            logger::debug("No valid packet received");
        }
        Err(e) => {
            // Handle connection errors
            handle_connection_error(peer, &e).await;
            return Err(e);
        }
    }

    Ok(())
}

async fn recv_single_packet(
    peer: &AsyncPeer,
    stream: &mut TcpStream,
) -> NetworkResult<Option<QubicApiPacket>> {
    // First read the header
    let mut header_bytes = [0u8; 8];

    timeout(READ_TIMEOUT, async {
        stream
            .read_exact(&mut header_bytes)
            .await
            .map_err(|e| NetworkError::Io(e))
    })
    .await
    .map_err(|_| NetworkError::Timeout)??;

    let header = RequestResponseHeader::from_vec(&header_bytes.to_vec());
    let packet_size = header.get_size();

    // Validate packet size
    if packet_size < 8 || packet_size > MAX_PACKET_SIZE {
        return Err(NetworkError::InvalidResponse);
    }

    // Read the full packet (including the header we already read)
    let mut buffer = vec![0u8; packet_size];
    buffer[0..8].copy_from_slice(&header_bytes);

    if packet_size > 8 {
        timeout(READ_TIMEOUT, async {
            stream
                .read_exact(&mut buffer[8..])
                .await
                .map_err(|e| NetworkError::Io(e))
        })
        .await
        .map_err(|_| NetworkError::Timeout)??;
    }

    match QubicApiPacket::format_response_from_bytes(&peer.get_id().to_string(), buffer) {
        Some(packet) => Ok(Some(packet)),
        None => {
            logger::debug("Failed to parse packet from peer");
            Err(NetworkError::InvalidResponse)
        }
    }
}

async fn recv_multiple_packets(
    peer: &AsyncPeer,
    stream: &mut TcpStream,
) -> NetworkResult<Vec<QubicApiPacket>> {
    let mut packets = Vec::new();
    let mut packet_count = 0;

    loop {
        // Safety check to prevent infinite loops
        if packet_count >= MAX_PACKETS_PER_RESPONSE {
            logger::debug(&format!(
                "Reached maximum packet limit ({}) for peer {}",
                MAX_PACKETS_PER_RESPONSE,
                peer.get_id()
            ));
            break;
        }

        // Try to read next packet
        match recv_single_packet(peer, stream).await {
            Ok(Some(packet)) => {
                // Check if this is an end response packet
                if packet.header.get_type().to_byte() == EntityType::ResponseEnd.to_byte() {
                    break;
                }
                packets.push(packet);
                packet_count += 1;
            }
            Ok(None) => {
                // Invalid packet, but continue trying
                packet_count += 1;
                continue;
            }
            Err(_) => {
                // Error reading packet or end of stream, stop
                break;
            }
        }
    }

    logger::debug(&format!(
        "Received {} packets from peer {}",
        packets.len(),
        peer.get_id()
    ));

    Ok(packets)
}

// Enhanced error handling with automatic peer disconnection
pub async fn handle_connection_error(peer: &AsyncPeer, error: &NetworkError) {
    match error {
        NetworkError::ConnectionFailed(_) | NetworkError::Timeout | NetworkError::Io(_) => {
            // Mark peer as disconnected
            peer.set_connected(false);

            // Update database
            if let Err(db_err) = set_peer_disconnected(get_db_path().as_str(), peer.get_id()) {
                logger::error!(
                    "Failed to update peer {} disconnect status: {}",
                    peer.get_id(),
                    db_err
                );
            }

            logger::debug(&format!(
                "Peer {} disconnected due to: {}",
                peer.get_id(),
                error
            ));
        }
        _ => {
            // Non-connection errors, just log
            logger::debug(&format!("Peer {} error: {}", peer.get_id(), error));
        }
    }
}

// Utility function for testing connection health
pub async fn test_connection_health(stream: &mut TcpStream) -> bool {
    // Try to read 1 byte to test if connection is alive
    let mut test_byte = [0u8; 1];
    match timeout(Duration::from_millis(100), async {
        stream.read(&mut test_byte).await
    })
    .await
    {
        Ok(Ok(_)) => true,
        _ => false,
    }
}

// Enhanced packet validation
fn validate_packet_header(header: &RequestResponseHeader) -> NetworkResult<()> {
    let size = header.get_size();

    // Basic size validation
    if size < 8 || size > MAX_PACKET_SIZE {
        return Err(NetworkError::InvalidResponse);
    }

    // Validate entity type
    let entity_type = header.get_type();
    if !is_valid_entity_type(&entity_type) {
        return Err(NetworkError::InvalidResponse);
    }

    Ok(())
}

fn is_valid_entity_type(entity_type: &EntityType) -> bool {
    matches!(
        entity_type,
        EntityType::RequestCurrentTickInfo
            | EntityType::RespondCurrentTickInfo
            | EntityType::RequestedQuorumTick
            | EntityType::RequestTickData
            | EntityType::RequestContractFunction
            | EntityType::RequestAssets
            | EntityType::ResponseEnd // Add other valid types as needed
    )
}
