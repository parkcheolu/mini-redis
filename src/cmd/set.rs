use crate::cmd::{Parse, ParseError};
use crate::{Connection, Db, Frame};

use bytes::Bytes;
use std::time::Duration;
use tracing::{debug, instrument};

pub struct Set {
    key: String,

    value: Bytes,

    expire: Option<Duration>,
}

impl Set {

    pub fn new(key: impl ToString, value: Bytes, expire: Option<Duration>) -> Set {
        Set {
            key: key.to_string(),
            value,
            expire,
        }
    }

    pub fn key(&self) -> &str {
        &self.key
    }

    pub fn value(&self) -> &Bytes {
        &self.value
    }

    pub fn expire(&self) -> &Bytes {
        &self.expire
    }

    /**
     * 수신한 프레임으로부터 'Set' 인스턴스를 파싱한다.
     * 
     * 'Parse' 아규먼트는 'Frame'의 필드를 읽기 위한 커서 방식의 API를 제공한다.
     * 이 함수의 호출 시점에는 프레임은 소켓으로부터 수신한 하나의 완전한 프레임이다.
     * 
     * 'SET' 문자열은 이미 소비되었다.
     * 
     * # Returns
     * 
     * 성공의 경우 'Set' 값을 반환한다. 프레임의 형태가 잘못된 경우 'Err'을 반환한다.
     * 
     * # Format
     * 
     * 세 앤트리를 포함하는 배열 프레임이 되어야 한다.
     * 
     * ```text
     * SET key value [EX seconds|PX milliseconds]
     * ```
     */
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Set> {
        use ParseError::EndOfStream;

        // set을 위한 키를 읽는다. 필수 필드다.
        let key = parse.next_string()?;

        // set을 위한 값을 읽는다. 필수 필드다.
        let value = parse.next_bytes()?;

        // 만료 지정은 선택적이다. 뒤에 아무것도 없다면 'None'이 된다.
        let mut expire = None;

        // 다음 문자열 파싱을 시도한다.
        match parse.next_string() {
            Ok(s) if s.to_uppercase() == "EX" => {
                // 만료 시간이 초로 지정된 경우. 다음 값은 integer가 된다.
                let secs = parse.next_int()?;
                expires = Some(Duration::from_secs(secs));
            }
            Ok(s) if s.to_uppercase() == "PX" => {
                // 만료 시간이 ms로 지정된 경우. 다음 값은 integer가 된다.
                let ms = parse.next_int()?;
                expire = Some(Duration::from_millis(ms));
            }
            /**
             * 현재 mini-redis는 SET 커맨드에 다른 옵션을 지원하지 않는다. 여기서 반환하는 에러는
             * 커넥션을 중단시킨다. 다른 커넥션들은 영향을 받지 않는다.
             */
            Ok(_) => return Err("currently 'SET' only supports the expiration option".into()),
            /**
             * 'EndOfStream'에러는 앞으로 파싱을 위한 데이터가 존재하지 않음을 나타낸다. 이 경우는 런타임에
             * 일반적으로 있을 수 있는 상황이며, 요청된 'SET'커맨드에 다른 옵션이 없음을 나타낸다.
             */
            Err(EndOfStream) => {}
            /**
             * 이 외의 에러는 결과적으로 커넥션을 중단시킨다.
             */
            Err(err) => return Err(err.into()),
        }

        Ok(Set { key, value, expire })
    }

    /**
     * 'Set' 커맨드를 특정 'Db' 인스턴스에 수행한다. 
     * 
     * 응답은 'dst'에 쓰여진다. 수신한 커맨드를 실행하기 위해, 서버가 이 함수를 호출한다.
     */
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 공유 데이터베이스 상태로부터 값을 세팅한다.
        db.set(self.key, self.value, self.expire);

        //  성공 응답을 생성하여 'dst'에 쓴다.
        let response = Frame::Simple("OK".to_string());
        debug!(?response);
        dst.write_frame(&response).await?;

        Ok(())
    }
    
    /**
     * 커맨드를 자신에 대응하는 'Frame'으로 변환한다.
     * 
     * 이 함수는 'Set'커맨드를 서버로 전송하기 위한 인코딩 시 클라이언트에 의해 호출된다.
     */
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("set".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame.push_bulk(Bytes::from(self.value));
        if let Some(ms) = self.expire {
            /**
             * 레디스 프로토콜에서 만료를 지정하는 방법에는 두 가지가 있다.
             * 1. SET key value EX seconds
             * 2. SET key value PX milliseconds
             * 여기서는 두 번째 옵션을 사용한다. 왜냐하면 이 옵션이 값을 표현하기에 더 정밀하기 때문이다.
             * 그리고 src/bin/cli.rs 는 duration_from_ms_str() 함수에서 만료 아규먼트를 ms로 파싱한다.
             */
            frame.push_bulk(Bytes::from("px".as_bytes()));
            frame.push_int(ms.as_millis() as u64);
        }
        frame
    }
}