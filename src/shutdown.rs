use tokio::sync::broadcast;

/// 서버 셧다운 시그널을 수신한다.
/// 
/// 셧다운 시그널은 'broadcast::Receiver'를 통해 이루어진다. 오직 단일 값만 전송될 수 있다.
/// 값이 한 번 broadcast 채널을 통해 전송되면, 서버는 정지되어야 한다.
/// 
/// 'Shutdown' struct는 시그널을 대기하고 시그널을 수신했는지 확인한다. 호출자는 셧다운 시그널
/// 수신 여부를 확인할 수 있다. 
pub(crate) struct Shutdown {
    /// 셧다운 시그널을 수신했다면 'true'를 반환한다.
    shutdown: bool,

    /// 셧다운을 수신하기 위한 채널의 절반을 수신한다.
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    /// 주어진 'broadcast::Receiver'를 기반으로 하는 새로운 'Shutdown'을 생성한다.
    pub(crate) fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Shutdown {
            shutdown: false,
            notify,
        }
    }

    /// 셧다운 시그널을 수신했다면 'true'를 반환한다.
    pub(crate) fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    /// 셧다운 알림을 수신한다. 필요에 따라 대기한다.
    pub(crate) async fn recv(&mut self) {
        // 이미 셧다운 시그널을 수신했다면 즉시 반환한다.
        if self.shutdown {
            return;
        }

        // 단 하나의 값만 전송되기 때문에, "lag error"를 수신하는 일은 없다.
        let _ = self.notify.recv().await;

        // 셧다운 시그널을 수신했음을 세팅한다.
        self.shutdown = true;
    }
}