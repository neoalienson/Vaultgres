use super::connection::Connection;
use crate::catalog::Catalog;
use std::io;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

pub struct Server {
    listener: TcpListener,
    catalog: Arc<Catalog>,
    data_dir: String,
}

impl Server {
    pub fn bind(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let catalog = Arc::new(Catalog::new());
        Ok(Self { listener, catalog, data_dir: "./data".to_string() })
    }

    pub fn bind_with_data_dir(addr: &str, data_dir: String) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let catalog = Arc::new(Catalog::new_with_data_dir(&data_dir));
        Ok(Self { listener, catalog, data_dir })
    }

    pub fn accept(&self) -> io::Result<Connection<TcpStream>> {
        let (stream, _) = self.listener.accept()?;
        Ok(Connection::new(stream, self.catalog.clone()))
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
    }

    pub fn shutdown(&self) -> Result<(), String> {
        log::info!("💾 Saving catalog to disk...");
        (&*self.catalog).save_to_disk(&self.data_dir)?;
        log::info!("✅ Catalog saved successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_bind() {
        let server = Server::bind("127.0.0.1:0").unwrap();
        assert!(server.local_addr().is_ok());
    }
}
