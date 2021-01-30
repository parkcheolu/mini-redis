use crate::{Connection, Db, Frame, Parse};

/**
 * 키의 값을 가져온다.
 * 
 * 키가 존재하지 않을 경우, 특별한 값인 nil을 반환한다.
 * 키에 해당하는 값이 문자열이 하닌 경우 에러를 반환한다. GET은 문자열 값만을 다루기 때문이다.
 */
pub struct Get {
    key: String,
}

impl Get {
    // 'key'로 데이터를 가져오는 'Get' 커맨드를 생성한다.
    pub fn new(key: impl ToString) -> Get {
        Get {
            key: key.to_string(),
        }
    }

    // 키를 가져온다.
    pub fn key(&self) -> &str {
        &self.key
    }

    /**
     * 수신한 프레임으로부터 'Get' 인스턴스를 파싱한다.
     *
     * 'Parse' 아규먼트는 'Frame'의 필드를 읽기 위한 커서 방식의 API를 제공한다.
     * 이 함수의 호출 시점에는 프레임은 소켓으로부터 수신한 하나의 완전한 프레임이다.
     * 
     * 'GET' 문자열은 이미 소비되었다.
     * 
     * # Returns
     * 
     * 성공의 경우 'Get' 값을 반환한다. 프레임의 형태가 잘못된 경우 'Err'을 반환한다.
     * 
     * # Format
     * 
     * 두 앤트리를 포함하는 배열 프레임이 되어야 한다.
     * 
     * ```text
     * GET key
     * ```
     */
    pub(crate) fn parse_frames(parse: &mut Parse) -> crate::Result<Get> {
        /**
         * 'GET' 문자열은 이미 소비되었다. 다음 값은 값을 가져오기 위한 키 이름이 된다.
         * 다음 값이 문자열이 아니거나, 완전히 소비된 입력값이 아니라면 에러를 반환한다.
         */
        let key = parse.next_string()?;

        Ok(Get { key })
    }

    /**
     * 'Get' 커맨드를 특정 'Db' 인스턴스에 수행한다. 
     * 
     * 응답은 'dst'에 쓰여진다. 수신한 커맨드를 실행하기 위해, 서버가 이 함수를 호출한다.
     */
    pub(crate) async fn apply(self, db: &Db, dst: &mut Connection) -> crate::Result<()> {
        // 공유 데이터베이스 상태로부터 값을 가져온다.
        let response = if let Some(value) = db.get(&self.key) {
            /**
             * 값이 존재하면 "bulk" 형식으로 클라이언트에게 응답한다.
             */
            Frame::Bulk(value)
        } else {
            // 값이 없다면 'Null'으로 응답한다.
            Frame::Null
        };

        debug!(?response);

        // 응답을 클라이언트에게 쓴다.
        dst.write_frame(&response).await?;

        Ok(())
    }

    /**
     * 커맨드를 자신에 대응하는 'Frame'으로 변환한다.
     * 
     * 이 함수는 'Get'커맨드를 서버로 전송하기 위한 인코딩 시 클라이언트에 의해 호출된다.
     */
    pub(crate) fn into_frame(self) -> Frame {
        let mut frame = Frame::array();
        frame.push_bulk(Bytes::from("get".as_bytes()));
        frame.push_bulk(Bytes::from(self.key.into_bytes()));
        frame
    }
}