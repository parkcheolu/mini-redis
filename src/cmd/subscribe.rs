
/**
 * 클라이언트를 하나 혹은 둘 이상의 채널에 구독자로 등록한다.
 * 
 * 클라이언트가 한 번 구독 상태가 되면, 그 클라이언트는 SUBSCRIBE, PSUBSCRIBE,
 * UNSUBSCRIBE, PUNSUBSCRIBE, PING, QUIT 커맨드를 제외한 다른 커맨드는 수행하지
 * 못한다.
 */
pub struct Subscribe {
    channels: Vec<String>,
}

/**
 * 클라이언트를 하나 혹은 둘 이상의 채널로부터 구독 해지한다.
 * 
 * 구독 해지 채널이 지정되지 않으면, 이전까지 구독되었던 모든 채널로부터 클라이언트를
 * 구독 해지한다.
 */
pub struct Unsubscribe {
    channels: Vec<String>,
}

/**
 * 메시지의 스트림
 * 스트림은 'broadcast::Receiver'로부터 메시지를 수신한다. 'stream!'을 사용하여 메시지를
 * 소비하는 'Stream'을 생성한다. 'stream!'에는 이름을 지정할 수 없기 때문에, 여기서는 trait object를
 * 사용하여 스트림을 박싱한다.
 */
type Messages = Pin<Box<dyn Stream<Item = Bytes> + Send>>;

impl Subscribe {
    // 특정 채널을 수신하기 위한 새로운 'Subscribe'를 생성한다.
    pub(crate) fn new(channels: &[String]) -> Subscribe {
        Subscribe {
            channels: channels.to_vec(),
        }
    }

    /**
     * 수신한 프레임으로부터 'Subscribe' 인스턴스를 파싱한다.
     * 
     * 'Parse' 아규먼트는 'Frame'의 필드를 읽기 위한 커서 방식의 API를 제공한다.
     * 이 함수의 호출 시점에는 프레임은 소켓으로부터 수신한 하나의 완전한 프레임이다.
     * 
     * 'SUBSCRIBE' 문자열은 이미 소비되었다.
     * 
     * # Returns
     * 
     * 성공의 경우 'Subscribe' 값을 반환한다. 프레임의 형태가 잘못된 경우 'Err'을 반환한다.
     * 
     * # Format
     * 
     * 세 앤트리를 포함하는 배열 프레임이 되어야 한다.
     * 
     * ```text
     * SUBSCRIBE channel [channel ...]
     * ```
     */
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Subscribe> {
        use ParseError::EndOfStream;

        /**
         * 'SUBSCRIBE' 문자열은 이미 소비되었다. 이 함수의 실행 시점에는 'parse'에는 하나 혹은 둘 이상의
         * 문자열이 존재한다. 이 문자열들은 구독할 대상 채널들이다.Subscribe
         * 
         * 첫 문자열을 추출한다. 문자열이 없다면 잘못된 프레임인 것이며, 에러가 반환된다.
         */
        let mut channels = vec![parse.next_string()?];

        /**
         * 이제 프레임의 나머지를 소비했다. 각 값은 문자열이거나, 문자열이 아니라면 잘못된 프레임이다.
         * 프레임 안의 모든 값이 소비되면 커맨드가 모두 파싱된 것이다.
         */
        loop {
            match parse.next_string() {
                /**
                 * 'parse'로부터 문자열 하나를 파싱하여 이를 구독 대상 채널 목록에 넣는다.
                 */
                Ok(s) => channels.push(s),

                // 'EndOfStream'은 더이상 파싱할 데이터가 남아있지 않음을 나타낸다.
                Err(EndOfStream) => break,

                // 이 외의 다른 모든 값은 에러가 되고, 커넥션을 중단한다.
                Err(err) => return Err(err.into()),
            }
        }

        Ok(Subscribe { channels })
    }

    /**
     * 'Subscribe' 커맨드를 특정 'Db' 인스턴스에 수행한다. 
     * 
     * 이 함수는 구독의 진입점이며, 구독 대상 채널의 초기 목록을 포함한다.
     * 이 함수 호출 이후에도 클라이언트로부터 'subscribe', 'unsubscribe' 커맨드를
     * 수신할 수 있으며, 이에 따라서 구독 목록을 갱신한다.
     * 
     * [here]: https://redis.io/topics/pubsub
     */
    pub(crate) async fn apply(
        mut self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        /**
         * 독립적인 각 채널 구독은 'sync::broadcast' 채널을 사용하여 핸들링한다.
         * 메시지들은 현재 채널을 구독 중인 모든 클라이언트에게 퍼지며 전송된다.
         * 
         * 독립적인 하나의 클라이언트는 여러 개의 채널을 구독할 수 있고, 자신의 구독
         * 목록에서 채널을 동적으로 추가하고 삭제할 수 있다. 이 기능을 위해, 'StreamMap'
         * 을 사용하여 활성화된 구독을 추적한다. 메시지를 수신할 때와 같이, 'SteramMap'은
         * 각 브로드캐스트 채널로부터의 메시지를 병합한다.
         */
        let mut subscriptions = StreamMap::new();

        loop {
            /**
             * 'self.channels'를 사용하여 추가적인 구독 대상 채널을 추적한다.
             * 'apply'를 실행하는 동안 새로운 'SUBSCRIBE' 커맨드를 수신하면 새 채널을
             * 여기의 vec에 추가한다.
             */
            for channel_name in self.channels.drain(..) {
                subscribe_to_channel(channel_name, &mut subscriptions, db, dst).await?;
            }

            /**
             * 다음 중 하나를 기다린다.
             * 
             * - 구독 채널 중 하나에서 메시지를 수신
             * - 클라이언트로부터 구독 혹은 구독 해지 커맨드를 수신
             * - 서버 셧다운 시그널
             */
            select! {
                Some((channel_name, msg)) = subscriptions.next() => {
                    dst.write_frame(&make_message_frame(channel_name, msg)).await?;
                }
                res = dst.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        none => return Ok(())
                    };

                    handle_command(
                        frame,
                        &mut self.channels,
                        &mut subscriptions,
                        dst,
                    ).await?;
                }
                _ = shutdown.recv() => {
                    return Ok(());
                }
            };
        }
    }

    /**
     * 커맨드를 'Frame'으로 변환한다.
     * 
     * 이 함수는 'Subscribe' 커맨드를 인코딩하여 서버로 전송하는 시점에 호출된다.
     */
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("subscribe".as_bytes()));
        for channel in self.channels {
            frame.push_bulk(Bytes::from(channel.into_bytes()));
        }
        frame
    }
}

async fn subscribe_to_channel(
    channel_name: String,
    subscriptions: &mut StreamMap<String, Messages>,
    db: &Db,
    dst: &mut Connection,
) -> crate::Result<()> {
    let mut rx = db.subscribe(channel_name.clone());

    let rx = Box::pin(async_stream::stream! {
        
    });
}

