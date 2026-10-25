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
    eprintln!("ls2 listening on http://{addr} (database: {path})");
    axum::serve(listener, ls2::server::router(ls2))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server error");
}
