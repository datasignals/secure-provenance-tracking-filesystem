use crate::context::RPCContext;
use crate::rpcwire::*;
use crate::vfs::NFSFileSystem;
use anyhow;
use async_trait::async_trait;
use std::net::SocketAddr;
use std::sync::Arc;
use std::{io, net::IpAddr};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info};
use moka::future::Cache;
// use std::time::Duration;
use tokio::sync::RwLock;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::{Instant, Duration};
use tokio::sync::Mutex;

/// A NFS Tcp Connection Handler
/// 
struct ClientExecution {
    address: String,
    timestamp: Instant,
}

pub struct NFSTcpListener<T: NFSFileSystem + Send + Sync + 'static> {
    listener: TcpListener,
    port: u16,
    arcfs: Arc<T>,
    mount_signal: Option<mpsc::Sender<bool>>,
}

pub fn generate_host_ip(hostnum: u16) -> String {
    format!(
        "127.88.{}.{}",
        ((hostnum >> 8) & 0xFF) as u8,
        (hostnum & 0xFF) as u8
    )
}

/// processes an established socket
async fn process_socket(
    mut socket: tokio::net::TcpStream,
    context: RPCContext,
) -> Result<(), anyhow::Error> {
    
    let (mut message_handler, mut socksend, mut msgrecvchan) = SocketMessageHandler::new(&context);
    let _ = socket.set_nodelay(true);

    tokio::spawn(async move {
        loop {
            if let Err(e) = message_handler.read().await {
                info!("Message loop broken due to {:?}", e);
                break;
            }
        }
    });
    loop {
        tokio::select! {
            _ = socket.readable() => {
                let mut buf = [0; 128000];

                match socket.try_read(&mut buf) {
                    Ok(0) => {
                        return Ok(());
                    }
                    Ok(n) => {
                        let _ = socksend.write_all(&buf[..n]).await;
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                        continue;
                    }
                    Err(e) => {
                        info!("Message handling closed : {:?}", e);
                        return Err(e.into());
                    }
                }

            },
            reply = msgrecvchan.recv() => {
                match reply {
                    Some(Err(e)) => {
                        info!("Message handling closed : {:?}", e);
                        return Err(e);
                    }
                    Some(Ok(msg)) => {
                        if let Err(e) = write_fragment(&mut socket, &msg).await {
                            error!("Write error {:?}", e);
                        }
                    }
                    None => {
                        return Err(anyhow::anyhow!("Unexpected socket context termination"));
                    }
                }
            }
        }
    }
}

#[async_trait]
pub trait NFSTcp: Send + Sync {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16;

    /// Gets the true listening IP. Useful on windows when the IP may be random
    fn get_listen_ip(&self) -> IpAddr;

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>);

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()>;
}

impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcpListener<T> {
    /// Binds to a ipstr of the form [ip address]:port. For instance
    /// "127.0.0.1:12000". fs is an instance of an implementation
    /// of NFSFileSystem.
    pub async fn bind(ipstr: &str, fs: T) -> io::Result<NFSTcpListener<T>> {
        let (ip, port) = ipstr.split_once(':').ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "IP Address must be of form ip:port",
            )
        })?;
        let port = port.parse::<u16>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "Port not in range 0..=65535",
            )
        })?;

        let arcfs: Arc<T> = Arc::new(fs);

        if ip == "auto" {
            let mut num_tries_left = 32;

            for try_ip in 1u16.. {
                let ip = generate_host_ip(try_ip);

                let result = NFSTcpListener::bind_internal(&ip, port, arcfs.clone()).await;

                match &result {
                    Err(_) => {
                        if num_tries_left == 0 {
                            return result;
                        } else {
                            num_tries_left -= 1;
                            continue;
                        }
                    }
                    Ok(_) => {
                        return result;
                    }
                }
            }
            unreachable!(); // Does not detect automatically that loop above never terminates.
        } else {
            // Otherwise, try this.
            NFSTcpListener::bind_internal(ip, port, arcfs).await
        }
    }

    async fn bind_internal(ip: &str, port: u16, arcfs: Arc<T>) -> io::Result<NFSTcpListener<T>> {
        let ipstr = format!("{ip}:{port}");
        let listener = TcpListener::bind(&ipstr).await?;
        info!("Listening on {:?}", &ipstr);

        let port = match listener.local_addr().unwrap() {
            SocketAddr::V4(s) => s.port(),
            SocketAddr::V6(s) => s.port(),
        };
        Ok(NFSTcpListener {
            listener,
            port,
            arcfs,
            mount_signal: None,
        })
    }
}

#[async_trait]
impl<T: NFSFileSystem + Send + Sync + 'static> NFSTcp for NFSTcpListener<T> {
    /// Gets the true listening port. Useful if the bound port number is 0
    fn get_listen_port(&self) -> u16 {
        let addr = self.listener.local_addr().unwrap();
        addr.port()
    }

    fn get_listen_ip(&self) -> IpAddr {
        let addr = self.listener.local_addr().unwrap();
        addr.ip()
    }

    /// Sets a mount listener. A "true" signal will be sent on a mount
    /// and a "false" will be sent on an unmount
    fn set_mount_listener(&mut self, signal: mpsc::Sender<bool>) {
        self.mount_signal = Some(signal);
    }

    /// Loops forever and never returns handling all incoming connections.
    async fn handle_forever(&self) -> io::Result<()> {
        let user_mount_info = Arc::new(RwLock::new(HashMap::new()));
        let connection_map: Cache<String, String> = Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(1))
            .build();
        let connection_map = Arc::new(connection_map);
        let recent_executions: Arc<Mutex<VecDeque<ClientExecution>>> = Arc::new(Mutex::new(VecDeque::new()));
        
        loop {
            let (socket, _) = self.listener.accept().await?;
            let client_address = socket.peer_addr().unwrap().to_string();
    
            let now = Instant::now();

            let mut executions = recent_executions.lock().await;
            
            // Remove any executions older than 10ms
            while let Some(front) = executions.front() {
                if now.duration_since(front.timestamp) > Duration::from_millis(10) {
                    executions.pop_front();
                } else {
                    break;
                }
            }

            // Check for mapping with the most recent execution
            if let Some(last_execution) = executions.back() {
                let duration = now.duration_since(last_execution.timestamp);
                if duration <= Duration::from_millis(10) && last_execution.address != client_address {
                    info!("Client address mapped: Previous: {}, Current: {}, Duration: {:?}",
                          last_execution.address, client_address, duration);
                    
                    // Store the mapping in the Cache
                    // connection_map.insert(, client_address.clone());
                    connection_map.insert(client_address.clone(), last_execution.address.clone()).await;
    
                }
            }

            // Add the current execution to the queue
            executions.push_back(ClientExecution {
                address: client_address.clone(),
                timestamp: now,
            });

            // Limit the queue size to prevent unbounded growth
            if executions.len() > 100 {
                executions.pop_front();
            }
            
            let context = RPCContext {
                local_port: self.port,
                client_addr: client_address.clone(),
                auth: crate::rpc::auth_unix::default(),
                vfs: self.arcfs.clone(),
                mount_signal: self.mount_signal.clone(),
                connection_map: connection_map.clone(),
                user_mount_info: user_mount_info.clone(),
            };
    
            info!("Accepting socket {:?} {:?}", socket, context);
            tokio::spawn(async move {
                let _ = process_socket(socket, context).await;
            });
        }
    }
    // async fn handle_forever(&self) -> io::Result<()> {

    //     let user_mount_info = Arc::new(RwLock::new(HashMap::new()));
        
    //     // Create a Moka cache with a time-to-live
    //     let connection_map: Cache<String, String> = Cache::builder()
    //         .max_capacity(1000)
    //         .time_to_live(Duration::from_secs(1))  
    //         .build();
    //     let connection_map = Arc::new(connection_map);

    //     let mut first_client_addr = None;

    //     loop {
    //         let (socket, _) = self.listener.accept().await?;


    //         let client_addr = socket.peer_addr().unwrap().to_string();
            
    //         if first_client_addr.is_none() {
    //             first_client_addr = Some(client_addr.clone());
    //             // println!("First Client Addr: {}", client_addr);
    //         } else {
    //             let second_client_addr = client_addr.clone();
    //             let first_addr = first_client_addr.clone().unwrap();
                
    //             // Insert into Moka cache
    //             connection_map.insert(second_client_addr.clone(), first_addr.clone()).await;
                
    //             // println!(
    //             //     "Mapped: Second Client Addr {} -> First Client Addr {}",
    //             //     second_client_addr, first_addr
    //             // );

    //             // Reset for the next set of connections
    //             first_client_addr = None;
    //         }

            
            
    //         let context = RPCContext {
    //             local_port: self.port,
    //             client_addr: socket.peer_addr().unwrap().to_string(),
    //             auth: crate::rpc::auth_unix::default(),
    //             vfs: self.arcfs.clone(),
    //             mount_signal: self.mount_signal.clone(),
    //             connection_map: connection_map.clone(),
    //             user_mount_info: user_mount_info.clone(),
                
    //         };
    //         info!("Accepting socket {:?} {:?}", socket, context);
    //         tokio::spawn(async move {
    //             let _ = process_socket(socket, context).await;
    //         });
    //     }
    // }
}
