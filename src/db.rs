use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/*
모든 커넥션이 공유하는 서버 상태.

'Db'는 키/값 데이터와, 활동중인 pub/sub 체널에 대한 모든 'broadcast::Sender' 값들을 'HashMap'에 저장한다.

한 'Db' 인스턴스는 공유 상태에 대한 핸들이다. 'Db'의 cloning은 shallow이며, atomic 레퍼런스 카운드를 증가시키기만 한다.

'Db' 값이 하나 생성되면 백그라운드 작업 하나가 시작된다. 이 작업은 요청된 만료 시간이 도래했을 때 값을 expiring 한다.
작업은 모든 'Db' 인스턴스의 dropped 까지 계속된다.
 */
pub(crate) struct Db {
     /*
     공유 상태의 핸들. 백그라운드 작업 또한 'Arc<Shared>'를 갖는다.
     */
    shared: Arc<Shared>,
}


struct Shared {
    /*
    공유 상태는 mutex로 보호된다. mutex는 'std::sync::Mutex' 이다. Tokio의 mutex가 아니다.
    이는 mutex를 획득한 상태에서 취하는 비동기 연산이 없기 때문이다. 그리고, 크리티컬 섹션이 아주 작다.

    Tokio mutex는 주로 '.await' 이 값을 반환하는 시점에 락이 유지되어야 할 때 사용된다. 이를 제외한 대부분의 상황에서는 
    std mutex가 최선의 선택이다. 만일 크리티컬 섹션에 비동기 연산이 존재하지 않지만 동작 시간이 긴 경우 (CPU 인텐시브한 작업 or 블로킹 연산),
    mutex 대기 연산을 포함한 전체 연산은 'blocking' 연산으로 간주되며, 'tokio::task:spawn_blocking'이 사용되어야 한다.
    */
    state: Mutex<State>,

    /*
    앤트리 만료를 핸들링하는 백그라운드 작업에게 신호를 보낸다. 백그라운드 작업은 대기하다가 이 신호가 오면 신호가 만료값을 체크인지, 셧다운 시그널인지 확인한다.
    */
    background_task: Notify,
}

struct State {
    /*
    key-value 데이터. 값을 저장할 뿐이기에, std HashMap으로 충분하다.
    */
    entries: HashMap<String, Entry>,

    /*
    pub/sub key-space. 레디스는 pub/sub과 key-value의 키 공간을 분리하여 사용한다.
    'mini-redis'는 이를 별도의 'HashMap'을 두어 구현한다.
     */
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /*
    키 TTL을 추적한다.

    키 만료 정보를 졍렬하여 보관하기 위해 'BTreeMap'을 사용한다.
    때문에 백그라운드 작업이 다음 만료 값을 찾을 때, 이 맵을 iterate하기만 하면 된다.

    가능성은 거의 없지만, 정확히 같은 순간에 둘 이상의 만료값이 생성될 수 있다.
    때문에 이 맵에서 Instant는 키로 사용하기에 충분하지 않다. 유니크 만료 식별자 ('u64')를 사용하여
    만료값을 구분하도록 한다.
    */
    expirations: BTreeMap<(Instant, u64), String>,

    /*
    다음 만료를 위한 식별자. 각 만료는 유니크 식별자와 연결되어 있다.
    여기서의 '식별자'는 위에서 언급된 '식별자'와 같은 것을 칭한다.
    */
    next_id: u64,

    /*
    Db 인스턴스가 셧다운되면 true가 된다. 'Db'인스턴스는 내부의 모든 값이 drop될 때 셧다운된다.
    이 값을 true로 세팅하면 백그라운드 태스크에게도 종료를 알린다. 
     */
    shutdown: bool,
}

// key-value 저장소에 저장될 항목.
struct Entry {
    // 항목을 찾기 위한 유니크한 값.
    id: u64,
    // 저장되는 실제 데이터.
    data: Bytes,
    // 항목이 만료되어 Db에서 삭제되어야 하는 시간.
    expires_at: Option<Instant>,
}

impl Db {
    // 비어있는 새로운 'Db' 인스턴스를 생성한다. 공유 상태를 할당하고, 백그라운드 작업이 키 만료를 관리하도록 한다.
    pub(crate) fn new() -> Db {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                entries: HashMap::new(),
                pub_sub: HashMap::new(),
                expirations: BTreeMap::new(),
                next_id: 0,
                shutdown: false,
            }),
            background_task: Notify::new(),
        });

        // 백그라운드 작업 시작.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Db {shared}
    }

    /*
    키에 해당하는 값을 꺼낸다.

    해당하는 값이 없으면 None을 반환한다. None을 반환하는 경우는 키에 대한 값이 할당되지 않았거나,
    할당되었던 값이 만료된 경우이다.
    */
    pub(crate) fn get(&self, key: &str) -> Option<Bytes> {
        /*
        락을 획득하고, 값을 꺼내고, 꺼낸 값을 clone한다.

        데이터는 Bytes로 저장되기 때문에, shallow clone이 된다. 실제 데이터는 복사되지 않는다.
        */
        let state = self.shared.state.lock().unwrap();
        state.entries.get(key).map(|entry| entry.data.clone())
    }

    /*
    키-값을 저장한다. 선택적으로 만료시간도 설정한다.

    이미 키에 해당하는 값이 있다면 삭제한다.
    */
    pub(crate) fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        /*
        다음 저장 ID를 증가시킨다.
        락으로 보호함으로써, 이 과정은 각 'set' 연산에 대해 한 유니크 식별자가 생성됨을 보장한다.
        */
        let id = state.next_id;
        state.next_id += 1;

        /*
        이 'set' 동작이 다음 만료가 되거든, 백그라운드 태스크는 상태를 변경하기 위해 알림을 받아야 한다.

        백그라운드 태스크가 알림을 받아야할지 여부는 'set' 동작 중 결정된다.
        */
        let mut notify = false;
        let expires_at = expire.map(|duration| {
            // 새로운 값이 만료될 시간.
            let when = Instant::now() + duration;

            /*
            오직 새로운 입력 항목의 만료가 다음 만료 항목일 때만 백그라운드 워커(태스크)에게 알린다.
            이 경우, 워커는 깨어나서(woken up) 이 상태를 업데이트해야한다.
            */
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);
            
            // 만료를 추적한다.
            state.expirations.insert((when, id), key.clone());
            when
        });

        // 새 항목을 'HashMap'에 넣는다.
        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            }
        );

        /*
        이 키로 저장된 기존 항목에 만료 시간이 있을 경우, 이 만료 정보는 삭제되어야 한다.
        */
        if let some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }

        /*
        백그라운드 태스크에게 알리기 전에 뮤택스를 해제한다. 이 작업은 이 함수가 뮤택스를 아직 잡고 있는 동안
        백그라운드 태스크가 깨어나서 뮤택스를 획득하려는 불필요한 시도를 방지하여 경합을 줄이도록 한다.
        */
        drop(state);

        if notify {
            /*
            마지막으로, 새로운 만료 정보를 업데이트해야 하는 경우에 한하여 백그라운드 태스크에게 알림을 보낸다.
            */
            self.shared.background_task.notify_one();
        }
    }

    /*
    요청된 채널에 대한 'Receiver'를 반환한다.

    반환되는 'Receiver'는 'PUBLISH' 커맨드로 값을 수신하는 경우에 사용된다.
    */
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // 뮤택스를 획득한다.
        let mut state = self.shared.state.lock().unwrap();

        /*
        요청된 채널에 대한 앤트리가 없을 경우, 새로운 브로드캐스트 채널을 생성하여 키와 연결한다.
        앤트리가 있다면 연결된 리시버를 반환한다.
        */
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                /*
                브로드캐스트가 없으면 새로 만든다.

                채널은 '1024'개의 메시지를 담을 수 있도록 생성한다.
                한 메시지는 모든 수신자들에게 전송될 때까지 보유된다. 
                이는 한 구독자의 수신 속도가 늦는다면 메시지가 사라지지 않고 계속 남아있을 수 있음을 의미한다.

                채널이 가득차면 메시지 발행은 오래된 메시지를 우선으로 drop한다. 
                이렇게 함으로써 느린 메시지 수신자로 인해 전체 시스템이 정지되는 경우를 방지한다.
                */
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /*
    채널에 메시지를 발행하고, 채널의 수신자의 수를 반환한다.
    */
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            /*
            브로드캐스트 채널을 통한 메시지 전송이 성공하면 수신자의 수를 반환한다.
            에러는 수신자가 없음을 의미한다. 이 경우 '0'을 반환해야 한다.
            */
            .map(|tx| tx.send(value).unwrap_or(0))
            /*
            키에 연결된 채널이 없다면 이는 수신자가 없는 것이다. 따라서 '0'을 반환한다.
            */
            .unwrap_or(0)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        /*
        마지막 'Db' 인스턴스인 경우 백그라운드 태스크는 반드시 셧다운 시그널을 받아야 한다.

        먼저, 이 인스턴스가 마지막 'Db' 인스턴스인지 판단한다. 이 판단은 'strong_count'를 확인함으로써 이루어진다.
        마지막 인스턴스인 경우, 카운트는 2가 되어야 한다. 하나는 이 'Db' 인스턴스이고, 다른 하나는 백그라운드 태스크가 잡고있는
        핸들이다.
        */
        if Arc::strong_count(&self.shared) == 2 {
            /*
            백그라운드 태스크는 반드시 셧다운 시그널을 받아야 한다.
            'State::shutdown'을 true로 세팅하고 태스크에게 시그널을 보낸다.
            */
            let mut state = self.shared.state.lock().unwrap();
            state.shutdown = true;

            /*
            백그라운드 태스크에게 시그널을 보내기 전에 락을 drop한다.
            이 작업은 불필요하게 백그라운드 태스크가 깨어나 뮤택스 획득을 시도하지 않도록 하여 락 경합을 줄인다.
            */
            drop(state);
            self.shared.background_task.notify_one();
        }
    }
}

impl Shared {
    /*
    모든 만료된 키를 퍼지하고, 다음 키 만료 시간을 가리키는 'Instant'를 반환한다.
    */
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            /*
            데이터베이스는 셧다운되고, 공유 상태에 대한 모든 핸들은 drop되었다.
            백그라운드 태스크는 정지되어야 한다.
            */
            return None;
        }

        /*
        이 코드는 borrow checker를 만족시키기 위한 것이다. 간단히 말하자면, 'lock()'은 'MutexGuard'를 반환한다.
        '&mut State'를 반환하지 않는다. borrow checker는 뮤택스 가드를 뚫고 안의 값을 볼 수 없어, 'state.expirations'와 'state.entries'에
        mutably하게 접근하는 일이 안전한지 판단할 수 없다. 때문에 루프 밖에서 'State'에 대한 "진짜" mutable 레퍼런스를 얻도록 한다.
        */
        let state = &mut *state;

        // '지금' 전에 만료되도록 스케쥴된 모든 키를 찾는다.
        let now = Instant::now();
        
        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                /*
                퍼지를 마치면 'when'은 다음 키 만료 시간을 가리키는 Instant가 된다.
                백그라운드 태스크는 이 시간까지 대기할 것이다.
                */
                return Some(when);
            }

            // 만료된 키는 삭제한다.
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }
        None
    }

    /*
    데이터베이크가 셧다운 중이라면 'true'를 반환한다.

    'shutdown'플래그는 'Db'의 모든 값이 drop되었을 때 설정된다. 이는 공유 상태에 더이상 접근할 수 없음을 나타낸다.
    */
    fn is_shutdown(&self) -> bool {
        self.state.lock().unwrap().shutdown
    }
}

impl State {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }
}

/*
백그라운드 태스크의 실행 루틴.

알림을 기다린다. 알림이 오면 공유 상태 핸들로부터 모든 만료 키를 퍼지한다.
'shutdown'이 설정되면 태스크를 종료한다.
*/
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // 셧다운 플래그가 설정되면 태스크는 종료되어야 한다.
    while !shared.is_shutdown() {
        /*
        만료된 모든 키를 퍼지한다. 이 함수는 다음 만료될 키의 만료 시간을 반환한다.
        워커는 다음 만료 시간이 지나 다시 퍼지를 수행할 때까지 기다려야 한다.
        */
        if let Some(when) = shared.purge_expired_keys() {
            /*
            다음 키가 만료되거나, 백그라운드 태스크가 알림을 받을 때까지 기다린다.
            알림을 받으면 반드시 상태를 리로드하여 더 빨리 만료되도록 설정된 새로운 키를 인식해야 한다.
            이 작업은 루프를 통해 수행한다.
            */
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            /*
            만료될 키가 없다. 알림을 기다린다.
            */
            shared.background_task.notified().await;
        }
    }
}