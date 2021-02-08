use crate::client::Client;
use crate::Result;

use bytes::Bytes;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::sync::oneshot;

/**
 * 클라이언트의 새 요청 버퍼를 생성한다.
 * 
 * 'Client'는 Redis 커맨드를 TCP 커넥션에 직접 수행한다. 한 시점에 반드시 하나의
 * 요청만이 전송될 수 있고, 연산은 'Client' 핸들에의 뮤터블한 접근을 필요로 한다.
 * 이런 방식으로, 다수의 Tokio 태스크가 단일 Redis 커넥션을 사용하지 않도록 한다.
 * 
 * 이런 수준의 문제를 다루기 위한 전략은, Redis 커넥션을 관리하기 위한 전용 Tokio 
 * 태스크(커넥션 태스크) 하나를 가동하고, 커넥션에 연산을 수행하기 위해 "메시지 전달"
 * 을 사용하는 것이다. 커맨드는 채널에 들어간다. 커넥션 태스크는 채널에서 커맨드를 
 * 꺼내서 Redis 커넥션에 수행한다. 커맨드에 대한 응답을 수신하면, 이를 원 요청자에게 
 * 전달한다.
 * 
 * 응답 'Buffer' 핸들은 새 핸들을 개별 태스크에 전달하기 전에 clone될 수 있다.
 */
pub fn buffer(client: Client) -> Buffer {
    /**
     * 메시지 수 제한을 32로 하드 코딩한다. 실제 어플리케이션에서는 이 크기를
     * 설정할 수 있도록 해야하지만, 여기서는 필요치 않다.
     */
    let (tx, rx) = channel(32);

    // 커넥션에 대한 요청 처리 태스크를 가동한다.
    tokio::spawn(async move { run (client, rx).await });

    // 'Buffer' 핸들을 반환한다.
    Buffer { tx }
}

// 요청된 커맨드를 'Buffer' 핸들로부터 메시지로 전달하기 위한 enum
enum Command {
    Get(String),
    Set(String, Bytes),
}

/**
 * 채널을 통해 커넥션 태스크에 전송된 메시지 타입
 * 
 * 'Command'는 커넥션에 전달하는 커맨드이다.
 * 
 * 'oneshot::Sender'는 **단일**값을 전송하는 채널으로, 여기서는 커넥션으로부터
 * 수신한 응답을 원 요청자에게 전달하기 위해 사용한다.
 */
type Message = (Command, oneshot::Sender<Result<Option<Bytes>>>);

/**
 * 채널을 통해 전송된 커맨드를 수신하고, 이를 Client(커넥션)에 전달한다. 커맨드 
 * 응답은 'oneshot'을 통해 다시 호출자에게 반환한다.
 */
async fn run(mut client: Client, mut rx: Receiver<Message>) {
    /**
     * 채널에서 메시지를 반복적으로 꺼낸다. 반환값 'None'은 모든 'Buffer' 핸들이
     * drop되었고 채널에 메시지가 더이상 남아있지 않음을 나타낸다.
     */
    while let Some((cmd, tx)) = rx.recv().await {
        // 커맨드를 커넥션에 전달한다.
        let response = match cmd {
            Command::Get(key) => client.get(&key).await,
            Command::Set(key, value) => client.set(&key, value).await.map(|_| None),
        };

        /**
         * 응답을 호출자에게 전송한다.
         * 
         * 메시지 전송 실패는 'rx'가 메시지를 수신하기 전에 drop된 것이며, 이는 
         * 런타임에 일반적으로 발생할 수 있다.
         */
        let _ = tx.send(response);
    }
}

pub struct Buffer {
    tx: Sender<Message>,
}

impl Buffer {

    /**
     * 키의 값을 꺼낸다.
     * 
     * 'Client::get'과 같지만, 요청이 자신과 연결된 커넥션에 전송 가능할 때까지
     * **버퍼링**된다.
     */
    pub async fn get(&mut self, key: &str) -> Result<Option<Bytes>> {
        // 채널을 통해 전송할 새로운 'Get'커맨드를 초기화한다.
        let get = Command::Get(key.into());

        // 커넥션으로부터 응답을 수신하기 위한 새로운 oneshot을 초기화한다.
        let (tx, rx) = oneshot::channel();

        // 요청을 전송한다.
        self.tx.send((get, tx)).await?;

        // 응답을 기다린다.
        match rx.await {
            Ok(res) => res,
            Err(err) => Err(err.into()),
        }
    }

    /**
     * 키와 값을 연결하여 세팅한다.
     * 
     * 'Client::set'과 같지만, 요청이 자신과 연결된 커넥션에 전송 가능할 때까지
     * **버퍼링**된다.
     */
    pub async fn set(&mut self, key: &str, value: Bytes) -> Result<()> {
        // 채널을 통해 전송할 새로운 'Set'커맨드를 초기화한다.
        let set = Command::Set(key.into(), value);

        // 커넥션으로부터 응답을 수신하기 위한 새로운 oneshot을 초기화한다.
        let (tx, rx) = oneshot::channel();

        // 요청을 전송한다.
        self.tx.send((set, tx)).await?;

        // 응답을 기다린다.
        match rx.await {
            Ok(res) => res.map(|_| ()),
            Err(err) => Err(err.into()),
        }
    }
}