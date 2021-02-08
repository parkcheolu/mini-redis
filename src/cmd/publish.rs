use crate::{Connection, Db, Frame, Parse};

use bytes::Bytes;
/**
 * 주어진 채널에 메시지를 전송한다.
 * 
 * 한 메세지를 채널에 전송한다. 채널의 각 구독자와 직접 연결되지 않는다.
 * 구독자들은 메시지를 수신하기 위해 체널을 구독할 수 있다.
 * 
 * 채널 이름은 키-값 namespace와 무관하다. "foo"라는 이름의 채널에 메시지를 전송하는 것과 
 * "foo" 키를 세팅하는 것은 아무런 관계가 없다.
 */
pub struct Publish {
    // 메시지가 전송되는 채널의 이름
    channel: String,

    // 전송되는 메시지
    message: Bytes,
}

impl Publish {
    // 'channel'에 'message'를 전송하는 새로운 'Publish'를 생성한다.
    pub(crate) fn new(channel: impl ToString, message: Bytes) -> Publish {
        Publish {
            channel.to_string(),
            message,
        }
    }


   /**
     * 수신한 프레임으로부터 'Publish' 인스턴스를 파싱한다.
     * 
     * 'Parse' 아규먼트는 'Frame'의 필드를 읽기 위한 커서 방식의 API를 제공한다.
     * 이 함수의 호출 시점에는 프레임은 소켓으로부터 수신한 하나의 완전한 프레임이다.
     * 
     * 'PUBLISH' 문자열은 이미 소비되었다.
     * 
     * # Returns
     * 
     * 성공의 경우 'Publish' 값을 반환한다. 프레임의 형태가 잘못된 경우 'Err'을 반환한다.
     * 
     * # Format
     * 
     * 세 앤트리를 포함하는 배열 프레임이 되어야 한다.
     * 
     * ```text
     * PUBLISH channel message
     * ```
     */
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Publish> {
        /**
         * 'PUBLISH' 문자열은 이미 소비되었다. 프레임에서 'channel', 'message' 값을 추출한다.
         * 
         * 'channel'은 반드시 문자열이어야 한다.
         */
        let channel = parse.next_string()?;

        // 'message'는 임의의 바이트이다.
        let message = parse.next_bytes()?;

        Ok(Publish { channel, message })
    }

    /**
     * 'Publish' 커맨드를 특정 'Db' 인스턴스에 수행한다. 
     * 
     * 응답은 'dst'에 쓰여진다. 수신한 커맨드를 실행하기 위해, 서버가 이 함수를 호출한다.
     */
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        /**
         * 공유 상태는 채널을 위한 'tokio::sync::broadcast::Sender'를 포함한다. 'db.publish'는
         * 메시지를 알맞은 채널에 보낸다.
         * 
         * 현재 채널을 구독 중인 구독자의 수를 반환한다. 이는 'num_subscribers' 만큼의 구독자가
         * 메시지를 수신했음을 의미하지는 않는다. 구독자는 메시지를 수신하기 전에 채널을 끊을 수 있다.
         * 'num_subscribers'는 "힌트" 정도로만 봐야 한다.
         */
        let num_subscribers = db.publish(&self.channel, self.message);

        // 구독자 수를 반환한다.
        let response = Frame::Integer(num_subscribers as u64);

        // 클라이이언트에 프레임을 쓴다.
        dst.write_frame(&response).await?;

        Ok(())
    }

    /**
     * 커맨드를 자신에 대응하는 'Frame'으로 변환한다.
     * 
     * 이 함수는 'Publish'커맨드를 서버로 전송하기 위한 인코딩 시 클라이언트에 의해 호출된다.
     */
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("publish".as_bytes()));
        frame.push_bulk(Bytes::from(self.channel.into_bytes()));
        frame.push_bulk(self.message);

        frame
    }
}