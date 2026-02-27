use rustgres::protocol::Server;
use rustgres::config::Config;
use std::thread;
use std::env;
use std::path::Path;
use std::fs;

fn main() -> std::io::Result<()> {
    // Load config from file or use default
    let config_path = env::var("RUSTGRES_CONFIG").unwrap_or_else(|_| "config.yaml".to_string());
    let config = if Path::new(&config_path).exists() {
        Config::from_file(&config_path).unwrap_or_else(|e| {
            eprintln!("Warning: Failed to load config from {}: {}", config_path, e);
            eprintln!("Using default configuration");
            Config::default()
        })
    } else {
        Config::default()
    };
    
    // Create data directories
    fs::create_dir_all(&config.storage.data_dir)?;
    fs::create_dir_all(&config.storage.wal_dir)?;
    
    // Initialize logger from config
    let filter = if config.logging.scope == "*" {
        format!("rustgres={}", config.logging.level)
    } else {
        config.logging.scope.split(',')
            .map(|s| format!("rustgres::{}={}", s.trim(), config.logging.level))
            .collect::<Vec<_>>()
            .join(",")
    };
    
    env::set_var("RUST_LOG", filter);
    env_logger::init();
    
    let addr = format!("{}:{}", config.server.host, config.server.port);
    log::info!("🚀 RustGres v0.1.0 starting...");
    log::info!("📡 Listening on {}", addr);
    log::info!("📁 Data directory: {}", config.storage.data_dir);
    log::info!("📝 WAL directory: {}", config.storage.wal_dir);
    log::info!("💾 Buffer pool: {} pages ({} MB)", 
        config.storage.buffer_pool_size,
        (config.storage.buffer_pool_size * config.storage.page_size) / (1024 * 1024)
    );
    log::info!("✅ Ready for connections");
    log::info!("\nConnect with: psql -h {} -p {} -U postgres -d testdb\n", config.server.host, config.server.port);
    
    let server = Server::bind(&addr)?;
    
    loop {
        match server.accept() {
            Ok(mut conn) => {
                thread::spawn(move || {
                    if let Err(e) = conn.run() {
                        log::error!("Connection error: {}", e);
                    }
                });
            }
            Err(e) => {
                log::error!("Accept error: {}", e);
            }
        }
    }
}
