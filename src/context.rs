use crate::vfs::NFSFileSystem;
use std::fmt;
use std::sync::Arc;
use tokio::sync::mpsc;

use tokio::sync::RwLock;
use std::collections::HashMap;
use moka::future::Cache;

#[derive(Clone)]
pub struct RPCContext {
    pub local_port: u16,
    pub client_addr: String,
    pub auth: crate::rpc::auth_unix,
    pub vfs: Arc<dyn NFSFileSystem + Send + Sync>,
    pub mount_signal: Option<mpsc::Sender<bool>>,
    pub connection_map: Arc<Cache<String, String>>,
    pub user_mount_info: Arc<RwLock<HashMap<String, String>>>,  // Store the user's mount information
}

impl fmt::Debug for RPCContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("RPCContext")
            .field("local_port", &self.local_port)
            .field("client_addr", &self.client_addr)
            .field("auth", &self.auth)
            .finish()
    }
    
}

// impl RPCContext {
//     pub async fn set_mount_point(&self, mount_point: String) {
//         let mut mount_point_lock = self.mount_point.write().await;
//         // println!("Setting mount point to: {}", mount_point); 
//         *mount_point_lock = mount_point;
//     }
// }
