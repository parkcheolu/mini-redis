//! mini-redis 서버.
//! 
//! 이 파일은 이 라이브러리에 구현된 서버의 진입점이다. 커맨드라인을 파싱하고
//! 아규먼트를 'mini_redis::server'에 전달한다.
//! 
//! 아규먼트 파싱에는 'clap' crate를 사용한다.

use mini_redis::{server, DEFAULT_PORT};

use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::signal;

#[tokio::main]
pub async fn main() -> mini_redis::Result<()> {
    // 로깅을 활성화한다.
    // 자세한 내용: https://docs.rs/tracing
    tracing_subscriber::fmt::try_init()?;

    let cli = Cli::from_args();
    let port = cli.port.as_deref().unwrap_or(DEFAULT_PORT);

    let listener = TcpListener::bind(&format!("127.0.0.1:{}", port)).await?;

    server::run(listener, signal::ctrl_c()).await
}
#[derive(StructOpt)]
#[structopt(name = "mini-redis-sever", version = env!("CARGO_PKG_VERSION"), author = env!("CARGO_PKG_AUTHORS"), about = "A Redis server")]
struct Cli {
    port: Option<String>,
}