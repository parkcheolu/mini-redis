/**
 * ! 미니멀 Redis 서버 구현
 * !
 * ! 인바운드 커넥션을 수신하는 비동기 'run'함수를 제공한다.
 * ! 커넥션마다 태스크를 가동한다.
 */

use crate::{Command, Connection, Db, Shutdown};

use std::future::Future;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};
use tracing::{debug, error, info, instrument};


/**
 * 서버 리스너 상태. 'run'호출 안에서 생성된다. TCP 리스닝을 수행하고 per-connection
 * 상태를 초기화하는 'run' 메서드를 포함하며, per-connection 상태를 초기화한다.
 */
struct Listener {
    /**
     * 공유 데이터베이스 핸들.
     * 
     * 키/값 저장소와 pub/sub 브로드캐스트 채널을 가진다.
     * 
     * 'Arc'로 감싼 래퍼(wrapper)이다. 'db'를 clone할 수 있도록 하여 각 커넥션의
     *  상태로 전달된다 ('Handler').
     */
    db: Db,

    // 'run' 호출자가 제공하는 TCP 리스너.
    listener: TcpListener,

    /**
     * 최대 커넥션 수를 제한한다.
     * 
     * 커넥션의 최대 개수를 제한하기 위해 'Semaphore(이하 세마포어)'를 사용한다.
     * 새로운 커넥션을 수락하기 전에 이 세마포어의 permit을 획득해야 한다. 유효한
     * permit을 획득하지 못하면 리스너는 대기한다.
     * 
     * 핸들러가 커넥션 처리를 마치면 permit을 세마포어에 반환한다.
     */
    limit_connections: Arc<Semaphore>,

    /**
     * 셧다운 시그널을 모든 유효한 커넥션에게 브로드캐스팅한다.
     * 
     * 초기 'shutdown' 트리거는 'run' 호출자가 제공한다. 유효한 커넥션들을 graceful하게
     * 종료하는 역할은 서버의 몫이다. 하나의 커넥션 태스크가 가동되면 이 태스크는 브로드캐스트
     * 수신자 핸들에게 전달된다. graceful 셧다운이 초기화되면 broadcast::Sender를 통해 '()'
     * 값을 전송한다. 각 커넥션은 이 값을 수신하고 안전한 종료 상태가 되며 태스크를 완료한다.
     */
    notify_shutdown: broadcast::Sender<()>,

    /**
     * graceful 셧다운 중 클라이언트 커넥션의 처리 완료를 기다리는 과정에 사용된다.
     * 
     * 모든 'Sender' 핸들이 한 번 범위를 벗어나면 Tokio 채널이 닫힌다. 한 채널이 닫히면
     * 채널의 수신자는 'None'을 수신한다. 이 방식으로 모든 커넥션 핸들이 작업을 마쳤음을
     * 감지한다. 한 커넥션 핸들러가 초기화되면 'shutdown_complete_tx'의 clone에 할당되며,
     * 리스너 셧다운 시 'shutdown_complete_tx'가 보유한 sender 필드를 drop한다. 모든 핸들러
     * 태스크가 완료되면 모든 'Sender'의 clone도 drop된다. 이는 결과적으로 'shutdown_complete_tx.recv()'
     * 가 'None'으로 완료되도록 한다. 이 시점에 서버 프로세스를 안전하게 종료할 수 있다.
     */
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

/**
 * 각 커넥션의 핸들러. 'connection'으로부터 요청을 읽어 그 커맨드를 'db'에 수행한다.
 */
struct Handler {
    /**
     * 공유 데이터베이스 핸들.
     * 
     * 'connection'으로부터 커맨드를 수신하면 이를 'db'에 수행한다. 커맨드는 'cmd' 모듈의
     * 구현체를 사용한다. 각 커맨드는 그 수행을 완료하기 위해 'db' 인스턴스와의 상호작용을
     * 필요로 한다.
     */
    db: Db,

    /**
     * 레디스 프로토콜 인코더/디코더를 갖춘 TCP 커넥션.
     * 인코더/디코더는 버퍼링된 'TcpStream'을 사용하여 구현되어 있다.
     * 
     * 'Listener'가 인바운드 커넥션을 수신하면 'TcpStream'을 'Connection::new'에 전달한다.
     * 'Connection::new'는 'TcpStream'과 연결된 버퍼를 초기화한다. 'Connection'은 핸들러에게
     * "frame" 수준의 연산을 가능하게 하며, 바이트 레벨 프로토콜 파싱의 세부사항은 'Connection'에
     * 캡슙화한다.
     */
    connection: Connection,

    /**
     * 최대 커넥션 세마포어.
     * 
     * 핸들러 drop 시 이 세마포어에 permit을 반환한다. 리스너가 커넥션 중단을 기다리는 중이라면
     * 유효한 새 permit 알림을 받아서 커넥션을 수락할 것이다.
     */
    limit_connections: Arc<Semaphore>,

    /**
     * 셧다운 알림을 받는다.
     * 
     * 'Listener'에서 sender와 페어링된 'broadcast::Receiver' 래퍼이다. 커넥션 핸들러는 통신
     * 상대측이 연결을 끊거나 **혹은** 셧다운 알림을 받을 때까지 커넥션으로부터의 요청을 처리한다.
     * 후자의 경우, 상대측을 향해 전송 동작 중에 있는 통신은 '안전한 상태'에 도달할 때까지 지속된다.
     * 안전한 상태란 커넥션이 종료되는 시점이다.
     */
    shutdown: Shutdown,

    // 직접 사용하지 않는다. 'Handler' drop 시 사용...?
    _shutdown_complete: mpsc::Sender<()>,
}

/**
 * 레디스 서버가 수용하는 최대 동시 커넥션 수.
 * 
 * 동시 커넥션 수가 여기에 도달하면 서버는 유효한 커넥션이 종료될 때까지 새로운 커넥션을 수립하지 않는다.
 * 
 * 실제 어플리케이션에서는 이 값을 따로 설정할 수 있어야 하겠지만, 이 예시에서는 하드코딩으로 둔다.
 * 
 * 이 값은 이 프로그램을 운영 환경에서 사용하지 않을 것을 권하도록 하는 아주 낮은 값이다 (당신은 이런 모든 
 * 고지사항들이 이 프로젝트가 진지한 프로젝트가 아님을 확실히 보여준다고 생각하겠지만, 이는 mini-http에서도 
 * 마찬가지라고 생각합니다.)
 */
const MAX_CONNECTIONS: usize = 250;

/**
 * mini-redis 서버를 가동한다.
 * 
 * 리스너로부터 커넥션을 수락한다. 커넥션 핸들링 태스크를 각 커넥션 당 하나씩 가동한다. 서버는 'shutdown'
 * future가 완료될 때까지 가동한다. 즉 서버의 graceful 셧다운까지이다.
 * 
 * 'tokio::signal::ctrl_c()'를 'shutdown' 아규먼트로 사용할 수 있다. 이것은 SIGINT 시그널이 될 것이다.
 */
pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    /**
     * 제공된 'shutdown' future가 완료되면, 반드시 셧다운 메시디를 모든 유효 커넥션들에게 전송해야 한다.
     * 이 작업에는 브로드캐스트 채널을 사용한다. 아래 코드의 호출은 브로드캐스트 페어의 수신자를 무시하고,
     * 수신자가 필요하면 sender에 subscribe() 메서드를 사용하여 하나를 생성한다.
     */
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);

    // 리스너 상태를 초기화한다.
    let mut server = Listener {
        listener,
        db: Db::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
    };

    /**
     * 서버 가동과 'shutdown' 시그널 수신을 동시에 수행한다. 서버 태스크는 에러를 만날때까지 실행된다.
     * 때문에 일반적인 환경에서는 이 'select!'문의 실행은 'shutdown' 시그널 수신 전까지 계속된다.
     * 
     * 'select!'문은 다음의 형태로 작성한다.
     *
     * ```
     * <result of async op> = <async op> => <step to perform with result>
     * ```
     * 
     * 모든 '<async op>' 문은 동시에 실행된다. op가 **처음**완료되면 이에 연결된 '<step to perform with result>'
     * 가 실행된다.
     */
    //  'select!' 매크로는 비동기 Rust 작성을 위해 기본이 되는 빌딩 블록이다. 자세한 내용은 아래를 본다:
    //  https://docs.rs/tokio/*/tokio/macro.select.html
    tokio::select! {
        res = server.run() => {
            /**
             * 여기서 에러를 수신하면 TCP 리스너로부터의 커넥션 수락이 여러번 실패했다는 의미이며, 서버는 실행을
             * 그만두고 셧다운한다.
             * 
             * 각 커넥션 핸들링에서 만나는 에러는 여기까지 올라오지 않는다. (...do not bubble up to this point.)
             */
            if let Err(err) = res {
                error!(cause = %err, "failed to accept");
            }
        }
        _ = shutdown => {
            // 셧다운 시그널을 수신했다.
            info!("shutting down");
        }
    }

    /**
     * 'shutdown_complete' 수신자와 전송기를 추출하여 'shutdown_transmitter'를 명시적으로 drop한다.
     * 이렇게 하지 않으면 아래의 '.await'는 영원히 완료되지 않기 때문에, 이는 중요한 작업이 된다.
     */
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;
    /**
     * 'notify_shutdown'이 drop되면 '구독 중'에 있는 모든 태스크는 셧다운 시그널을 수신하고 종료한다.
     */
    drop(notify_shutdown);
    // 아래의 'Receiver'를 완료하기 위해 마지막 'Sender'를 drop한다.
    drop(shutdown_complete_tx);

    /**
     * 모든 유효한 커넥션이 처리를 마칠때까지 기다린다. 리스너가 잡고있는 'Sender' 핸들은 위에서 drop
     * 되었기 때문에, 커넥션 핸들러 태스크가 잡고 있는 'Sender'만이 남아있다. 이 drop 작업들을 수행할 때
     * 'mpsc' 채널이 닫히고 'recv()'는 'None'을 반환할 것이다.
     */
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}

impl Listener {
    /**
     * 서버를 가동한다.
     * 
     * 인바운드 커넥션을 수신한다. 각 인바운드 커넥션마다 그 커넥션을 핸들링할 태스크를 시작한다.
     * 
     * # Errors
     * 
     * 커넥션 수락에서 에러가 발생하면 'Err'를 반환한다. 여기에는 시간이 지남에 따라 해결될 수 있는
     * 여러가지 원인이 있을 수 있다. 예를 들어, os가 내부적으로 제한하는 가용 소켓 수에 도달하는 경우,
     * 수락에 실패할 것이다.
     * 
     * 이 프로세스는 일시적인 에러가 스스로 해결되었음을 감지할 수 없다. 이를 핸들링하는 전략 중 하나는
     * 백오프 전략을 구현하는 것이다. 여기서는 이 방법을 취한다.
     */
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");

        loop {
            /**
             * 가용 permit을 기다린다.
             * 
             * 'acquire'는 세마포어에 라이프타임으로 연결된 permit을 반환한다. permit 값은 drop 시 
             * 자동으로 세마포어로 반환된다. 이 방식은 많은 경우에 편리하지만, 여기에서는 permit은 다른
             * 태스크에 반환되어야 한다 (핸들러 태스크). 이를 위해 우리는 "forget"을 사용한다. "forget"은
             * permit을 세마포어의 **permit값 증가 없이** drop한다. 다음으로, 핸들러 태스크에서는 작업을
             * 마쳤을 때 직접 새로운 permit을 추가한다.
             * 
             * 세마포어가 닫히면 'acquire()'는 'Err'을 반환한다. 우리는 절대 세마포어를 닫지 않으므로, 
             * 'unwrap()'은 안전하다.
             */
            self.limit_connections.acquire().await.unwrap().forget();

            /**
             * 새 소켓을 수락한다. 이는 에러 핸들링을 시도한다. 'accept' 메서드는 내부적으로 에러 복구를 
             * 시도하므로, 여기서 나오는 에러는 복구 불가능한 에러이다.
             */
            let socket = self.accept().await?;

            // 한 커넥션에 대한 핸들러 상태를 생성한다.
            let mut handler = Handler {
                /**
                 * 공유 데이터베이스로의 핸들을 가져온다. db는 내부적으로 'Arc'이므로 clone은 ref count
                 * 만을 증가시킨다.
                 */
                db: self.db.clone(),

                /**
                 * 커넥션 상태를 초기화한다. 이 동작은 레디스 프로토콜 프레임 파싱을 수행하기 위한 읽기/쓰기
                 *  버퍼를 초기화한다.
                 */
                connection: Connection::new(socket),

                /**
                 * 커넥션 상태는 커넥션 최대치를 제한하는 세마포어를 필요로 한다. 핸들러가 커넥션에 대한 처리를
                 * 마치면, permit은 세마포어로 반환된다.
                 */
                limit_connections: self.limit_connections.clone(),

                // 셧다운 알림을 수신한다.
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),

                /**
                 * 모든 clone이 drop되면 수신자에게 이를 알린다.
                 */
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            /**
             * 커넥션 처리를 위한 태스크를 가동한다. Tokio 태스크는 비동기 그린 쓰레드에 가까우며, 동시에 실행된다.
             */
            tokio::spawn(async move {
                // 커넥션을 처리한다. 에러를 만나면 로깅한다.
                if let Err(err) = handler.run().await {
                    error!(cause = ?err, "connection error");
                }
            });
        }
    }

    /**
     * 인바운드 커넥션을 수락한다.
     * 
     * 에러는 백오프 & 재시도로 핸들링한다. 지수 백오프 전략을 사용한다. 태스크는 첫 실패 후 1초를 
     * 기다린다. 두 번째 실패에서는 2초 기다린다. 이어지는 실패에 대해서는 대기 시간을 2배씩 늘린다.
     * 64초 대기 후인 6번째 시도에서 실패하면 이 함수는 에러를 반환한다. 
     */
    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        // 수락을 몇 번 시도한다.
        loop {
            /**
             * 수락 연산을 수행한다. 소켓을 성공적으로 수락하면 이 소켓을 반환한다.
             * 성공하지 못하면 에러를 저장한다.
             */
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        // 너무 많이 실패했다. 에러를 반환한다.
                        return Err(err.into());
                    }
                }
            }

            // 백오프 시간에 도달할 때까지 실행을 멈춘다.
            time::sleep(Duration::from_secs(backoff)).await;

            // 백오프 시간을 두 배로 늘린다.
            backoff *= 2;
        }
    }
}

impl Handler {
    /**
     * 단일 커넥션을 핸들링한다.
     * 
     * 소켓으로부터 요청 프레임을 읽어 처리한다. 응답은 다시 소켓에 쓴다.
     * 
     * 현재 파이프라이닝은 구현되어있지않다. 파이프라이닝은 각 커넥션이 프레임을 interleaving 없이도
     * 동시에 둘 이상의 요청을 처리할 수 있도록 하는 기능이다. 자세한 내용은 여기에 있다:
     * https://redis.io/topics/pipelining
     * 
     * 셧다운 시그널을 수신하면 커넥션은 안전 상태에 도달할 때까지 처리를 지속한다. 안전 상태는 커넥션을
     * 종료하는 시점이다.
     */
    async fn run(&mut self) -> crate::Result<()> {
        /**
         * 셧다운 시그널을 수신하지 전까지 계속해서 새 요청 프레임을 읽는다.
         */
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    /**
                     * 셧다운 시그널을 수신하면 'run'함수를 종료한다.
                     * 이는 태스크를 종료하는 결과가 된다.
                     */
                    return Ok(());
                }
            };

            /**
             * read_frame()에서 'None'을 반환하면 상대측은 소켓을 닫는다.
             * 더이상 처리할 내용은 없고, 태스크를 종료할 수 있다.
             */
            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(())
            };

            /**
             * 레디스 프레임을 커맨드 struct로 변환한다. 프레임이 유효하지 않거나 
             * 지원하지 않는 커맨드라면 에러를 반환한다.
             */
            let cmd = Command::from_frame(frame)?;

            /**
             * 'cmd' 객체를 로깅한다. 이 문법은 'tracing' crate이 제공하는 축약된
             * 형태이다. 이는 아래와 유사한 것으로 간주할 수 있다:
             * 
             * ```
             * debug!(cmd = format!("{:?}", cmd))
             * ```
             */
            debug!(?cmd);

            /**
             * 커맨드 수행에 필요한 작업을 수행한다. 이는 데이터베이스 상태를 변경할 수 있다.
             * 
             * 커넥션을 apply 함수에 전달하여 커맨드가 그 응답 프레임을 커넥션에 직접 쓸 수 있도록
             * 한다. pub/sub의 경우, 다수의 프레임을 상대측으로 전송할 수 있다.
             */
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }

        Ok(())
    }
}

impl Drop for Handler {
    fn drop(&mut self) {
        /**
         * 세마포어에 permit 하나를 반환한다.
         * 
         * 이 작업은 커넥션 수가 최대치에 도달하여 중지된 리스너를 재개한다.
         * 
         * 이 작업을 'Drop' 구현에 두어, 만일 태스크를 핸들링하는 커넥션이 panic된 상황이라도
         *  permit 반환을 보장한다. 만약 'add_permit'을 'run'함수의 끝에서 호출할 경우, 어떤
         * 버그는 panic을 유발하고, permit은 세마포어로 반환되지 못한다.
         */
        self.limit_connections.add_permits(1);
    }
}