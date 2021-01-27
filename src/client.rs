
/**
Redis 서버와 커넥션을 수립한다.

'Client'는 'TcpStream' 하나를 기반으로 기본적인 네트워크 클라이언트 기능(no pooling, 재시도, ...)
을 제공한다. 커넥션은 ['connect'](fn@connect) 함수를 통해 수립한다.

요청(requests)은 'Client'의 다양한 메서드를 통해 이루어진다.
*/
pub struct Client {
    /**
    레디스 프로토콜 인코더/디코더를 갖춘 TCP 커넥션
    인코더/디코더는 버퍼링을 사용하는 'TcpStream'으로 구현되어 있다.

    'Listener'가 인바운드 커넥션을 수신하면, 'TcpStream'을 'Connection::new'로 전달하고,
    'Connection::new'에서는 넘겨진 'TcpStream'과 연결되는 버퍼를 초기화한다.
    'Connection'은 핸들러로 하여금 "프레임" 수준의 연산을 가능하게 하고, 바이트 레벨 프로토콜
    파싱의 세부 내용을 'Connection' 안에 캡슐화한다.    
    */
    connection: Connection,
}

/**
pub/sub 모드로 진입한 클라이언트

한 번 채널을 구독한 클라이언트는 pub/sub 관련 커맨드만을 수행할 가능성이 있다. 'Client' 타입은
'Subscriber'로 전이되어 pub/sub과 무관한 메서드가 호출됨을 방지한다.
*/
pub struct Subscriber {
    // 구독자 클라이언트
    client: Client,
    
    // 현재 'Subscriber'를 통해 구독하는 채널의 모음
    subscribed_channels: Vec<String>,
}

// 구독 중인 채널을 통해 수신되는 메시지
pub struct Message {
    pub channel: String,
    pub content: Bytes,
}

/**
'addr'에 위치한 Redis 서버와의 연결을 수립한다.

'addr'은 'SocketAddr'으로 비동기적 변환이 가능한 어떠한 타입이든 될 수 있다.
여기에는 'SocketAddr'과 문자열이 포함된다. 'ToSocketAddrs' trait은 'std'가 아닌 Tokio의 버전이다.

# Example

```no_run
use mini_redis::client;

#[tokio::main]
async fn main() {
    let client = match client::connect("localhost:6379").await {
        Ok(client) => client,
        Err(_) => panic!("failed to establish connection"),
    };
# drop(client);
}
```
 */
pub async fn connect<T: ToSocketAddrs>(addr: T) -> crate::Result<Client> {
    /**
    'addr' 아규먼트는 곧바로 'TcpStream::connect'에 전달된다. 이는 비동기 DNS 룩업
    을 수행하고, TCP 커넥션 수립을 시도한다. 이 두 단계 중 하나에서 발생하는 에러는 
    'mini-redis' 에 연결하는 호출자에게 전달된다.
    */
    let socket = TcpStream::connect(addr).await?;

    /**
    연결 상태를 초기화한다. 이 작업은 레디스 프로토콜 프레임 파싱을 위한 읽기/쓰기
    버퍼를 할당한다.
    */
    let connection = Connection::new(socket);

    Ok(Client { connection })
}

impl Client {
    /**
    키에 해당하는 값을 얻는다.

    존재하지 않는 키라면, 특별한 값인 'None'을 반환한다.

    # Examples

    기본적인 사용 예

    ```no_run
    use mini_redis::client;

    async fn main() {
        let mut client = client::connect("localhost:6379").await.unwrap();

        let val = client.get("foo").await.unwrap();
        println!("Got = {:?}", val);
    }
    ```
    */
    pub async fn get(&mut self, key: &str) -> crate::Result<Option<Bytes>> {
        // 'key'의 'Get' 커맨드를 생성하고, 이를 프레임으로 변환한다.
        let frame = Get::new(key).into_frame();

        debug!(request = ?frame);

        /**
        프레임을 소켓에 쓴다(write). 완전한 프레임을 소켓에 쓰며, 필요할 경우 대기한다. 
        */
        self.connection.write_frame(&frame).await?;

        /**
        서버로부터 응답을 기다린다.

        'Simple', 'Bulk' 프레임을 받는다. 'Null'은 키가 없음을 의미하며, 'None'을
        반환한다.
        */
        match self.read_response().await? {
            Frame::Simple(value) => Ok(Some(value.into())),
            Frame::Bulk(value) => Ok(Some(value)),
            Frame::Null => Ok(None),
            frame => Err(frame.to_error()),
        }
    }

    pub async fn set(&mut self, key: &str, value: Bytes) -> crate::Result<()> {

    }

    
}

impl Subscriber {

}