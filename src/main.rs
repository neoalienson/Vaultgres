#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]

use std::env;
use std::fs;
use std::path::Path;
use std::thread;
use vaultgres::config::Config;
use vaultgres::protocol::Server;

fn main() -> std::io::Result<()> {
    // Load config from file or use default
    let config_path = env::var("VAULTGRES_CONFIG").unwrap_or_else(|_| "config.yaml".to_string());
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
        format!("vaultgres={}", config.logging.level)
    } else {
        config
            .logging
            .scope
            .split(',')
            .map(|s| format!("vaultgres::{}={}", s.trim(), config.logging.level))
            .collect::<Vec<_>>()
            .join(",")
    };

    // SAFETY: set_var is not thread-safe, but we are in the main thread and no other threads have been spawned yet.
    unsafe {
        env::set_var("RUST_LOG", filter);
    }
    env_logger::init();

    let addr = format!("{}:{}", config.server.host, config.server.port);
    log::info!("🚀 VaultGres v0.1.0 starting...");
    log::info!("📡 Listening on {}", addr);
    log::info!("📁 Data directory: {}", config.storage.data_dir);
    log::info!("📝 WAL directory: {}", config.storage.wal_dir);
    log::info!(
        "💾 Buffer pool: {} pages ({} MB)",
        config.storage.buffer_pool_size,
        (config.storage.buffer_pool_size * config.storage.page_size) / (1024 * 1024)
    );
    log::info!("✅ Ready for connections");
    log::info!(
        "\nConnect with: psql -h {} -p {} -U postgres -d testdb\n",
        config.server.host,
        config.server.port
    );

    let server = Server::bind_with_data_dir(&addr, config.storage.data_dir.clone())?;

    // Setup signal handler for graceful shutdown
    let server_clone = std::sync::Arc::new(server);
    let server_for_handler = server_clone.clone();

    ctrlc::set_handler(move || {
        log::info!("\n🛑 ctrlc: Shutdown signal received (SIGTERM/SIGINT)");
        log::info!("💾 ctrlc: Forcing immediate save before shutdown...");
        if let Err(e) = server_for_handler.shutdown() {
            log::error!("❌ ctrlc: Failed to save catalog: {}", e);
        } else {
            log::info!("✅ ctrlc: Catalog saved successfully before shutdown");
        }
        log::info!("👋 ctrlc: Exiting...");
        std::process::exit(0);
    })
    .expect("Error setting Ctrl-C handler");

    // Also handle SIGTERM explicitly for Docker
    #[cfg(unix)]
    {
        use signal_hook::consts::signal::*;
        use signal_hook::iterator::Signals;
        use std::thread;

        let server_for_signal = server_clone.clone();

        thread::spawn(move || {
            log::info!("📡 Signal handler thread started, waiting for SIGTERM/SIGINT");
            let mut signals = Signals::new([SIGTERM, SIGINT]).unwrap();
            for signal in &mut signals {
                match signal {
                    SIGTERM => {
                        log::info!("\n🛑 signal_hook: Received SIGTERM");
                        log::info!("💾 signal_hook: Forcing immediate save...");
                        if let Err(e) = server_for_signal.shutdown() {
                            log::error!("❌ signal_hook: Failed to save catalog: {}", e);
                        } else {
                            log::info!("✅ signal_hook: Catalog saved successfully");
                        }
                        log::info!("👋 signal_hook: Exiting...");
                        std::process::exit(0);
                    }
                    SIGINT => {
                        log::info!("\n🛑 signal_hook: Received SIGINT");
                        log::info!("💾 signal_hook: Forcing immediate save...");
                        if let Err(e) = server_for_signal.shutdown() {
                            log::error!("❌ signal_hook: Failed to save catalog: {}", e);
                        } else {
                            log::info!("✅ signal_hook: Catalog saved successfully");
                        }
                        log::info!("👋 signal_hook: Exiting...");
                        std::process::exit(0);
                    }
                    _ => {}
                }
            }
        });
    }

    loop {
        match server_clone.accept() {
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
