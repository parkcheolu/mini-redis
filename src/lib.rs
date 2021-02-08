///! Redis 서버와 클라이언트의 미니멀(i.e. 매우 불완전한)한 구현.
///!
///! 이 프로젝트의 목적은 Tokio로 구현된 비동기 Rust 프로젝트의 규모 있는 예시를
///! 제공하는 데에 있다. 이 프로그램을 운영 환경에 사용하지 않을 것을 권한다.
///! 
///! #Latout
///! 
///! 이 라이브러리는 가이드와 함께 사용하도록 구성되어 있다. 여기에는 "실제" Redis
///! 클라이언트 라이브러리에서는 public이 아닐 public 모듈들이 존재한다.
///! 
///! 주 요소는:
///! 
///!///'server': Redis 서버 구현체. 한 'TcpListener'를 취하여 레디스 클라이언트 
///!    커넥션 요청을 핸들링하는 단일 'run' 함수를 포함한다.
///! 
///!///'client': 비동기 Redis 클라이언트 구현체. Tokio로 어떻게 클라이언트를 만드는지
///!    보여준다.
///! 
///!///'cmd': 지원하는 Redis 커맨드 구현체
///! 
///!///'frame': 단일 Redis 프로토콜 프레임. 한 프레임은 "command"와 바이트 표현의 중간
///!    표현을 위해 사용된다.

pub mod client;

pub mod cmd;
pub use cmd::Command;

mod connection;
pub use connection::Connection;

pub mod frame;
pub use frame::Frame;

mod db;
use db::Db;

mod parse;
use parse::{Parse, ParseError};

pub mod server;

mod buffer;
pub use buffer::{buffer, Buffer};

mod shutdown;
use shutdown::Shutdown;


/// 레디스 서버가 수신할 기본 포트.
/// 
/// 포트가 지정되지 않을 경우 사용된다.
pub const DEFAULT_PORT: &str = "6379";

/// 대부분의 함수에서 반환하는 에러.
///
/// 실제 어플리케이션을 작성할 때는 에러 핸들링을 위한 특화된 crate 혹은 원본 에러를 담고 
/// 있는 'enum' 에러 타입의 사용이 필요할 수 있다. 하지만 여기에서는 박싱된 'std::error::Error'로
/// 충분하다.
///
/// 성능상의 이유로, 중요한 부분에서는 박싱을 사용하지 않는다. 예를 들어, 'parse'에는 'enum'으로 
/// 커스텀 에러가 정의되어 있다. 이는 그 에러가 일반적인 커맨드 실행 중 소켓에 프레임의 일부가 
/// 수신되었을 때 발생하고 핸들링되기 때문이다. 'parse::Error'는 'std::error::Error'를 구현하며, 
/// 에러를 'Box<dyn std::error::Error>'로 변환한다.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// mini-redis 연산에 특화된 'Result'
///
/// 편의를 위해 정의되었다.
pub type Result<T> = std::result::Result<T, Error>;


 