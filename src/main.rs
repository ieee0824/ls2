use std::net::SocketAddr;

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}

#[tokio::main]
async fn main() {
    let bind = env("LS2_BIND_ADDR", "127.0.0.1:9326");
    let addr: SocketAddr = bind.parse().unwrap_or_else(|e| {
        eprintln!("invalid LS2_BIND_ADDR {bind}: {e}");
        std::process::exit(2);
    });
    let allow_remote = matches!(
        env("LS2_ALLOW_UNAUTHENTICATED_REMOTE", "false").as_str(),
        "true" | "1"
    );
    if !addr.ip().is_loopback() && !allow_remote {
        eprintln!(
            "refusing to bind {addr}: ls2 has no authentication. \
             Set LS2_ALLOW_UNAUTHENTICATED_REMOTE=true to accept non-loopback connections."
        );
        std::process::exit(2);
    }
    let path = env("LS2_DATABASE_PATH", "ls2.sqlite");
    let ls2 = ls2::Ls2::open(&path).unwrap_or_else(|e| {
        eprintln!("cannot open database {path}: {e}");
        std::process::exit(1);
    });
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .unwrap_or_else(|e| {
            eprintln!("cannot bind {addr}: {e}");
            std::process::exit(1);
        });
    let config = ls2::server::Config {
        virtual_host_domains: env("LS2_VIRTUAL_HOST_DOMAINS", "localhost")
            .split(',')
            .map(str::to_owned)
            .collect(),
        sqs_endpoint: std::env::var("LS2_SQS_ENDPOINT")
            .ok()
            .filter(|v| !v.is_empty()),
        lifecycle_interval: match env("LS2_LIFECYCLE_INTERVAL_SECONDS", "60").parse::<u64>() {
            Ok(0) => None,
            Ok(secs) => Some(std::time::Duration::from_secs(secs)),
            Err(e) => {
                eprintln!("invalid LS2_LIFECYCLE_INTERVAL_SECONDS: {e}");
                std::process::exit(2);
            }
        },
    };
    if let Some(sqs) = &config.sqs_endpoint {
        eprintln!("event notifications: {sqs}");
    }
    let router = ls2::server::try_router_with_config(ls2, config).unwrap_or_else(|e| {
        eprintln!("{e}");
        std::process::exit(2);
    });
    eprintln!("ls2 listening on http://{addr} (database: {path})");
    eprintln!("browse: http://{addr}/_ls2/browse");
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server error");
}
