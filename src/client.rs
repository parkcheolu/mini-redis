
use crate::cmd::{Get, Publish, Set, Subscribe, Unsubscribe};
use crate::{Connection, Frame};

use async_stream::try_stream;
use std::io::{Error, ErrorKind};
use bytes::Bytes;
use tokio::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;
use tokio_stream::Stream;
use tracing::{debug, instrument};

/// Redis 서버와 커넥션을 수립한다.
/// 
/// 'Client'는 'TcpStream' 하나를 기반으로 기본적인 네트워크 클라이언트 기능(no pooling, 재시도, ...)
/// 을 제공한다. 커넥션은 ['connect'](fn@connect) 함수를 통해 수립한다.
/// 
/// 요청(requests)은 'Client'의 다양한 메서드를 통해 이루어진다.
pub struct Client {
    /// 레디스 프로토콜 인코더/디코더를 갖춘 TCP 커넥션
    /// 인코더/디코더는 버퍼링을 사용하는 'TcpStream'으로 구현되어 있다.
    /// 
    /// 'Listener'가 인바운드 커넥션을 수신하면, 'TcpStream'을 'Connection::new'로 전달하고,
    /// 'Connection::new'에서는 넘겨받은 'TcpStream'과 연결되는 버퍼를 초기화한다.
    /// 'Connection'은 핸들러로 하여금 "프레임" 수준의 연산을 가능하게 하고, 바이트 레벨 프로토콜
    /// 파싱의 세부 내용을 'Connection' 안에 캡슐화한다.    
    connection: Connection,
}

/// pub/sub 모드로 진입한 클라이언트
/// 
/// 한 번 채널을 구독한 클라이언트는 pub/sub 관련 커맨드만을 수행할 가능성이 있다. 'Client' 타입은
/// 'Subscriber'로 전이되어 pub/sub과 무관한 메서드가 호출됨을 방지한다.
pub struct Subscriber {
    /// 구독자 클라이언트
    client: Client,
    
    /// 현재 'Subscriber'를 통해 구독하는 채널의 모음
    subscribed_channels: Vec<String>,
}

/// 구독 중인 채널을 통해 수신되는 메시지
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

/// 'addr'에 위치한 Redis 서버와의 연결을 수립한다.
/// 
/// 'addr'은 'SocketAddr'으로 비동기적 변환이 가능한 어떠한 타입이든 될 수 있다.
/// 여기에는 'SocketAddr'과 문자열이 포함된다. 'ToSocketAddrs' trait은 'std'가 아닌 Tokio의 버전이다.
/// 
/// # Example
/// 
/// ```no_run
/// use mini_redis::client;
/// 
/// #[tokio::main]
/// async fn main() {
///     let client = match client::connect("localhost:6379").await {
///         Ok(client) => client,
///         Err(_) => panic!("failed to establish connection"),
///     };
/// # drop(client);
/// }
/// ```
pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
    // 'addr' 아규먼트는 곧바로 'TcpStream::connect'에 전달된다. 이는 비동기 DNS 룩업
    // 을 수행하고, TCP 커넥션 수립을 시도한다. 이 두 단계 중 하나에서 발생하는 에러는 
    // 'mini-redis' 에 연결하는 호출자에게 전달된다.
    let socket = TcpStream::connect(addr).await?;

    // 연결 상태를 초기화한다. 이 작업은 레디스 프로토콜 프레임 파싱을 위한 읽기/쓰기
    // 버퍼를 할당한다.
    let connection = Connection::new(socket);

    Ok(Client { connection })
}

impl Client {
    /// 키에 해당하는 값을 얻는다.
    /// 
    /// 존재하지 않는 키라면, 특별한 값인 'None'을 반환한다.
    /// 
    /// # Examples
    /// 
    /// 기본적인 사용 예
    /// 
    /// ```no_run
    /// use mini_redis::client;
    /// 
    /// async fn main() {
    ///     let mut client = client::connect("localhost:6379").await.unwrap();
    /// 
    ///     let val = client.get("foo").await.unwrap();
    ///     println!("Got = {:?}", val);
    /// }
    /// ```
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 'key'의 'Get' 커맨드를 생성하고, 이를 프레임으로 변환한다.
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        // 프레임을 소켓에 쓴다(write). 완전한 프레임을 소켓에 쓰며, 필요할 경우 대기한다. 
        self.connection.write_frame(&frame).await?;

        // 서버로부터 응답을 기다린다.
        // 
        // 'Simple', 'Bulk' 프레임을 받는다. 'Null'은 키가 없음을 의미하며, 'None'을
        // 반환한다.
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    /// 'key'를 주어진 'value'에 묶어 세팅한다.
    /// 
    /// 'value'와 'key'의 연결은 'key'가 다른 'set' 호출로 덮어씌어지거나, 삭제될 때까지 
    /// 유지된다.
    /// 
    /// 이미 키에 연결된 값이 있으면 값을 덮어쓴다. SET 연산이 성공하면 이전에 키에 연결된 
    /// 값은 폐기된다.
    /// 
    /// # Examples
    /// 
    /// 기본적인 사용 예
    /// 
    /// ```no_run
    /// use mini_redis::client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///      let mut client = client::connect("localhost:6379").await.unwrap();
    ///      
    ///      client.set("foo", "bar".into()).await.unwrap();
    ///      
    ///      // 곧바로 값을 가져온다.
    ///      let val = client.get("foo").await.unwrap().unwrap();
    ///      assert_eq!(val, "bar");
    /// }
    /// ```
    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {
        //  'Set' 커맨드를 생성하고 'set_cmd'에 전달한다. 값과 만료 시간을 함께 설정
        //  하기 위한 메서드가 따로 분리되어 있다. 두 함수의 공통부는 'set_cmd'에 구현
        //  되어 있다.
        self.set_cmd(Set::new(key, value, None)).await
    }

    /// 'key'를 주어진 'value'에 묶어 세팅한다. 'expiration'로 지정한 시간이 지나면
    /// 값은 삭제된다.
    /// 
    /// 'value'와 'key'의 연결은 다음 중 하나를 만족할 때까지 유지된다:
    /// - 시간 만료.
    /// - 이후 다른 'set' 호출에 의해 값이 덮어씌워짐.
    /// - 삭제.
    /// 
    /// 이미 키에 연결된 값이 있으면 값을 덮어쓴다. SET 연산이 성공하면 이전에 키에 연결된 
    /// 값은 폐기된다.
    /// 
    /// # Examples
    /// 
    /// 기본적인 사용 예시. 시간에 기반한 로직이기 때문에, 이 예시가 항상 그대로 작동하기는
    /// 않으며, 클라이언트와 서버의 시간이 동기화되어 있음을 전제로 한다. 현실 세계에서는 
    /// 그렇지 않은 경향이 있다.
    /// 
    /// ```no_run
    /// use mini_redis::client;
    /// use tokio::time;
    /// use std::time::Duration;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///      let ttl = Duration::from_millis(500);
    ///      let mut client = client::connect("localhost:6379").await.unwrap();
    ///      
    ///      client.set_expires("foo", "bar".into(), ttl).await.unwrap();
    /// 
    ///      // 곧바로 값을 가져온다.
    ///      let val = client.get("foo").await.unwrap().unwrap();
    ///      assert_eq!(val, "bar");
    /// 
    ///      // TTL 만료까지 기다린다.
    ///      time::sleep(ttl).await;
    /// 
    ///      let val = client.get("foo").await.unwrap();
    ///      assert!(val.is_some());
    /// }
    /// ```
    pub async fn set_expires(
        &mut self,
        key: &str,
        value: Bytes,
        expiration: Duration,
    ) -> crate::Result<()> {
        // 'Set' 커맨드를 생성하고 'set_cmd'에 전달한다. 값과 만료 시간을 함께 설정
        // 하기 위한 메서드가 따로 분리되어 있다. 두 함수의 공통부는 'set_cmd'에 구현
        // 되어 있다.
        self.set_cmd(Set::new(key, value, Some(expiration))).await
    }

    // 'SET'의 핵심 로직. 'set', 'set_expires'에서 사용한다.
    async fn set_cmd(&mut self, cmd: Set) -> crate::Result<()> {
        // 'Set' 커맨드를 프레임으로 변환한다.
        let frame = cmd.into_frame();

        debug!(request = ?frame);

        // 프레임을 소켓에 쓴다. 이 쓰기 작업은 완전한 프레임을 소켓에 쓴다.
        // 필요에 따라 대기한다.
        self.connection.write_frame(&frame).await?;

        // 서버로부터 응답을 기다린다. 응답이 성공일 경우 서버는 간단히 "OK"로
        // 응답한다. 이 외에 다른 응답은 에러를 나타낸다.
        match self.read_response().await? {
            Frame::Simple(response) if response == "OK" => Ok(()),
            frame => Err(frame.to_error()),
        }
    }

    /// 'message'를 주어진 'channel'에 발행(전송)한다.
    /// 
    /// 현재 채널에 구독 중인 구독자 수를 반환한다. 이 모든 구독자가 실제로 메시지를
    /// 수신했음을 보장하지 않는다. 구독자는 언제든 연결을 끊을 수 있다.
    /// 
    /// # Examples
    /// 
    /// 기본적인 사용 예시.
    /// 
    /// ```no_run
    /// use mini_redis::client;
    /// 
    /// #[tokio::main]
    /// async fn main() {
    ///      let mut client = client::connect("localhost:6379").await.unwrap();
    /// 
    ///      let val = client.publish("foo").await.unwrap().unwrap();
    ///      assert!("Got = {:?}", val);
    /// }
    /// ```
    pub async fn publish(&mut self, channel: &str, message: Bytes) -> crate::Result<u64> {
        // 'Publish' 커맨드를 프레임으로 변환한다.
        let frame = Publish::new(channel, message).into_frame();

        debug!(request = ?frame);

        // 프레임을 소켓에 쓴다.
        self.connection.write_frame(&frame).await?;

        // 응답을 읽는다.
        match self.read_response().await? {
            Frame::Integer(response) => Ok(response),
            frame => Err(frame.to_error()),
        }
    }

    /**
     * 클라이언트가 특정 채널을 구독한다.
     * 
     * 한 번 구독 커맨드를 수행한 클라이언트는 더이상 non-pub/sub 커맨드를 수행할 수
     * 없다. 이 함수는 'self'를 소비하여 'Subscriber'를 반환한다.
     * 
     * 'Subscriber' 값을 사용하여 메시지를 수신하고 클라이언트가 구독 중인 채널 목록을
     * 관리한다.
     */
    pub async fn subscribe(mut self, channels: Vec<String>) -> crate::Result<Subscriber> {
        // 서버에 구독 커맨드를 수행하고 확인을 기다린다. 클라이언트는 "구독자" 상태로
        // 변하고, 이 시점부터 pub/sub 커맨드만 수행할 수 있다.
        self.subscribe_cmd(&channels).await?;

        // 'Subscriber' 타입을 반환한다.
        Ok(Subscriber {
            client: self,
            subscribed_channels: channels,
        })
    }

    // 'SUBSCRIBE'의 핵심 로직. 구독 함수들이 사용한다.
    async fn subscribe_cmd(&mut self, channels: &[String]) -> crate::Result<()> {
        // 'Subscribe' 커맨드를 프레임으로 변환한다.
        let frame = Subscribe::new(&channels).into_frame();

        debug!(request = ?frame);

        // 프레임을 소켓에 쓴다.
        self.connection.write_frame(&frame).await?;

        // 서버는 구독 중인 각 채널에 대해 구독이 확인되었음을 메시지로 응답한다.
        for channel in channels {
            // 응답을 읽는다.
            let response = self.read_response().await?;

            // 구독 확인 응답인지 검증한다.
            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    // 서버는 다음 형태의 배열 프레임으로 응답한다:
                    // 
                    // ```
                    // [ "subscribe", channel, num-subscribed ]
                    // ```
                    // 
                    // channel은 채널의 이름이며, num-subscribed는 클라이언트가 현재
                    // 구독 중인 채널의 수이다.
                    [subscribe, schannel, ..]
                        if *subscribe == "subscribe" && *schannel == channel => {}
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }

    /// 소켓으로부터 응답을 읽는다.
    /// 
    /// 'Error' 프레임을 수신하면 'Err'로 변환한다.
    async fn read_response(&mut self) -> crate::Result<Frame> {
        let response = self.connection.read_frame().await?;

        debug!(?response);

        match response {
            // 에러 프레임은 'Err'로 변환한다.
            Some(Frame::Error(msg)) => Err(msg.into()),
            Some(frame) => Ok(frame),
            None => {
                // 여기서 'None'을 수신한다는 것은 서버가 프레임을 전송하지 않고
                // 연결을 종료했음을 나타낸다. 이는 예상치 못한 동작이며, "connection reset by server"
                // 에러로 표시한다.
                let err = Error::new(ErrorKind::ConnectionReset, "connection reset by server");

                Err(err.into())
            }
        }
    }
}

impl Subscriber {

    // 현재 구독 중인 채널 목록을 반환한다.
    pub fn get_subscribed(&self) -> &[String] {
        &self.subscribed_channels
    }

    /// 구독 채널에 발행된 다음 메시지를 수신한다. 필요에 따라 대기한다.
    /// 
    /// 'None'은 구독이 중단되었음을 나타낸다.
    pub async fn next_message(&mut self) -> crate::Result<Option<Message>> {
        match self.client.connection.read_frame().await? {
            Some(mframe) => {
                debug!(?mframe);

                match mframe {
                    Frame::Array(ref frame) => match frame.as_slice() {
                        [message, channel, content] if *message == "message" => Ok(Some(Message {
                            channel: channel.to_string(),
                            content: Bytes::from(content.to_string()),
                        })),
                        _ => Err(mframe.to_error()),
                    },
                    frame => Err(frame.to_error()),
                }
            }
            None => Ok(None),
        }
    }

    /// subscriber를 'Stream'으로 변환한다. 이 'Stream'은 구독 채널에 발행된 
    /// 메시지를 생산한다.
    /// 
    /// 'Subscriber'는 자체적으로 스트림을 구현하지 않는다. safe 코드로 스트림을
    /// 구현하는 일은 가볍지 않기 때문이다. async/await의 사용은 Stream 구현체의
    /// 'unsafe' 코드 사용을 요구한다. 이 대신, 변환 함수를 제공하고 여기서 'async-stream'
    /// crate의 도움을 받아 구현된 스트림을 반환하도록 한다.
    pub fn into_stream(mut self) -> impl Stream<Item = crate::Result<Message>> {
        // 'async-stream' crate의 'try_stream' 매크로를 사용한다. Rust의 제너레이터는
        // 안정적이지 않다. 이 crate은 매크로를 사용하여 async/await을 기반으로 작동하는
        // 유사 제너레이터를 생성한다. 제한 사항이 있으므로 문서를 읽기를 권한다.
        try_stream! {
            while let Some(message) = self.next_message().await? {
                yield message;
            }
        }
    }

    /// 채널 목록을 구독한다.
    pub async fn subscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        // 구독 커맨드를 수행한다.
        self.client.subscribe_cmd(channels).await?;

        // 구독 채널 목록을 갱신한다.
        self.subscribed_channels
            .extend(channels.iter().map(Clone::clone));
        
        Ok(())
    }

    /// 채널 목록으로 구독을 해지한다.
    pub async fn unsubscribe(&mut self, channels: &[String]) -> crate::Result<()> {
        let frame = Unsubscribe::new(&channels).into_frame();

        debug!(request = ?frame);

        // 프레임을 소켓에 쓴다.
        self.client.connection.write_frame(&frame).await?;

        // 인풋 채널 목록이 비어있다면 서버는 모든 구독 채널로부터의 구독을 해지한다.
        // 때문에 수신한 해지 목록과 클라이언트의 구독 채널 목록을 비교한다.
        let num = if channels.is_empty() {
            self.subscribed_channels.len()
        } else {
            channels.len()
        };

        // 응답을 읽는다.
        for _ in 0..num {
            let response = self.client.read_response().await?;

            match response {
                Frame::Array(ref frame) => match frame.as_slice() {
                    [unsubscribe, channel, ..] if *unsubscribe == "unsubscribe" => {
                        let len = self.subscribed_channels.len();

                        if len == 0 {
                            // 최소 1개의 채널이 있어야 한다.
                            return Err(response.to_error());
                        }

                        // 이 시점에는 해지된 채널이 아직 구독 목록에 남아있다.
                        // 해지된 채널을 목록에서 제거한다.
                        self.subscribed_channels.retain(|c| *channel != &c[..]);

                        // 구독 채널 목록에서 삭제된 채널은 단 하나여야 한다.
                        if self.subscribed_channels.len() != len - 1 {
                            return Err(response.to_error());
                        }
                    }
                    _ => return Err(response.to_error()),
                },
                frame => return Err(frame.to_error()),
            };
        }

        Ok(())
    }
}