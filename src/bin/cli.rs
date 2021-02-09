use mini_redis::{client, DEFAULT_PORT};

use bytes::Bytes;
use std::{num::ParseIntError, str, time::Duration};
use structopt::StructOpt;

#[derive(StructOpt)]
struct Cli {
    #[structopt(subcommand)]
    command: Command,

    #[structopt(name = "hostname", long = "--host", default_value = "127.0.0.1")]
    host: String,

    #[structopt(name = "port", long = "--port", default_value = DEFAULT_PORT)]
    port: String,
}
#[derive(StructOpt)]
enum Command {
    /// 키의 값을 가져온다.
    Get {
        /// 가져올 값의 키
        key: String,
    },
    /// 키와 값을 묶어 저장한다.
    Set {
        /// 저장할 키 이름
        key: String,

        /// 저장할 값
        #[structopt(parse(from_str = bytes_from_str))]
        value: Bytes,

        /// 값 만료 시간 값
        #[structopt(parse(try_from_str = duration_from_ms_str))]
        expires: Option<Duration>,
    },
}

/// CLI 툴의 진입점.
/// 
/// '[tokio::main]' 어노테이션은 이 함수의 호출 시 Tokio 런타임을 시작하도록 하는
/// 시그널을 보낸다. 함수 본문은 새롭게 가동되는 런타임 안에서 실행된다.
/// 
/// 여기서 사용하는 'flavoer = "current_thread"'은 백그라운드 쓰레드를 가동하기 
/// 위함이다. 멀티쓰레드를 사용하는 대신 가벼움을 취함으로써 CLI 툴의 유즈케이스의
/// 이점을 더욱 살리도록 한다.
#[tokio::main(flavor = "current_thread")]
async fn main() -> mini_redis::Result<()> {
    // 로깅을 활성화한다.
    tracing_subscriber::fmt::try_init()?;

    // 커맨드라인 아규먼트를 파싱한다.
    let cli = Cli::from_args();

    // 연결할 원격 주소를 가져온다.
    let addr = format!("{}:{}", cli.host, cli.port);

    // 연결을 수립한다.
    let mut client = client::connect(&addr).await?;

    // 요청 커맨드를 수행한다.
    match cli.command {
        Command::Get { key } => {
            if let Some(value) = client.get(&key).await? {
                if let Ok(string) = str::from_utf8(&value) {
                    println!("\"{}\"", string);
                } else {
                    println!("{:?}", value);
                }
            } else {
                println!("(nil)");
            }
        }
        Command::Set {
            key,
            value,
            expires: None,
        } => {
            client.set(&key, value).await?;
            println!("OK");
        }
        Command::Set {
            key,
            value,
            expires: Some(expires),
        } => {
            client.set_expires(&key, value, expires).await?;
            println!("OK");
        }
    }

    Ok(())
}

fn duration_from_ms_str(src: &str) -> Result<Duration, ParseIntError> {
    let ms = src.parse::<u64>()?;
    Ok(Duration::from_millis(ms))
}

fn bytes_from_str(src: &str) -> Bytes {
    Bytes::from(src.to_string())
}