use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

use bytes::Bytes;
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

/*
모든 커넥션이 공유하는 서버 상태.

'Db'는 키/값 데이터와, 활동중인 pub/sub 체널에 대한 모든 'broadcast::Sender' 값들을 'HashMap'에 저장한다.

한 'Db' 인스턴스는 공유 상태에 대한 handle이다. 'Db'의 cloning은 shallow이며, atomic 레퍼런스 카운드를 증가시키기만 한다.

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

    키 만료 시, 만료 정보를 졍렬하여 보관하기 위해 'BTreeMap'을 사용한다.
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
    할당되었던 값이 만료된 경우가 있을 수 있다.
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
            let when = Instant::now() + duration;

            notify = state.next_expiration()
                        .map(|expiration| expiration > when)
                        .unwrap_or(true);
            
            state.expirations.insert((when, id), key.clone());
            when
        });

        let prev = state.entries.insert(
            key,
            Entry {
                id,
                data: value,
                expires_at,
            }
        );

        if let some(prev) = prev {
            if let Some(when) = prev.expires_at {
                state.expirations.remove(&(when, prev.id));
            }
        }

        drop(state);

        if notify {
            self.shared.background_task.notify_one();
        }
    }


    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        let mut state = self.shared.state.lock().unwrap();

        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.lock().unwrap();

        state
            .pub_sub
            .get(key)
            .map(|tx| tx.send(value).unwrap_or(0))
            .unwrap_or(0)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 2 {
            let mut state = self.shared.state.lock().unwrap();
            state.shutdown = true;

            drop(state);
            self.shared.background_task.notify_one();
        }
    }
}

impl Shared {
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.lock().unwrap();

        if state.shutdown {
            return None;
        }

        let state = &mut *state;

        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                return Some(when);
            }
            state.entries.remove(key);
            state.expirations.remove(&(when, id));
        }
        None
    }

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

async fn purge_expired_tasks(shared: Arc<Shared>) {
    while !shared.is_shutdown() {
        if let Some(when) = shared.purge_expired_keys() {
            tokio::select! {
                _ = time::sleep_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            shared.background_task.notified().await;
        }
    }
}
