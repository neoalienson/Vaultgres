use super::connection::Connection;
use crate::catalog::Catalog;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::io;

pub struct Server {
    listener: TcpListener,
    catalog: Arc<Catalog>,
}

impl Server {
    pub fn bind(addr: &str) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let catalog = Arc::new(Catalog::new());
        Ok(Self { listener, catalog })
    }

    pub fn accept(&self) -> io::Result<Connection<TcpStream>> {
        let (stream, _) = self.listener.accept()?;
        Ok(Connection::new(stream, self.catalog.clone()))
    }

    pub fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.listener.local_addr()
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
