
// "unknown" 커맨드를 나타낸다. 이것은 진짜 'Redis' 커맨드가 아니다.
pub struct Unknown {
    command_name: String,
}

impl Unknown {
    /**
     * 새로운 'Unknown' 커맨드를 생성한다. 이 커맨드는 클라이언트가 요청한 알 수 없는 커맨드에
     * 응답하기 위해 사용된다.
     */
    pub(crate) fn new(key: impl ToString) -> Unknown {
        Unknown {
            command_name: key.to_string(),
        }
    }

    // 커맨드 이름을 반환한다.
    pub(crate) fn get_name(&self) -> &str {
        &self.command_name
    }

    /**
     * 클라이언트에게 '이 커맨드는 알 수 없음' 을 알린다.
     * 
     * 이것은 주로 커맨드가 'mini-redis'에서 아직 구현되지 않았음을 의미한다.
     */
    pub(crate) async fn apply(self, dst: &mut Connection) -> crate::Result<()> {
        let response = Frame::Error(format!("ERR unknown command '{}'", self.command_name));

        debug!(?response);

        dst.write_frame(&response).await?;
        Ok(())
    }
}