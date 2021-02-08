mod get;
pub use get::Get;

mod publish;
pub use publish::Publish;

mod set;
pub use set::Set;

mod subscribe;
pub use subscribe::{Subscribe, Unsubscribe};

mod unknown;
pub use unknown::Unknown;

use crate::{Connection, Db, Frame, Parse, ParseError, Shutdown};

/**
 * 지원하는 Redis 커맨드 목록
 * 
 * 'Command'에 호출되는 메서드는 커맨드 구현체로 위임된다.
 */
pub enum Command {
    Get(Get),
    Publish(Publish),
    Set(Set),
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Unknwon(Unknown),
}

impl Command {
    /**
     * 수신된 프레임으로부터 커맨드를 파싱한다.
     * 
     * 'Frame'은 반드시 'mini-resit'에서 지원하는 Redis 커맨드여야 하며, 배열 프레임 타입
     * 이어야 한다.
     * 
     * # Returns
     * 
     * 성공일 경우 커맨드 값을 반환하고, 실패일 경우 'Err'을 반환한다.
     */
    pub fn from_frame(frame: Frame) -> crate::Result<Command> {
        /**
         * 'Parse'로 프레임 값을 감싼다. 'Parse'는 커맨드 파싱을 쉽게 해주는 커서 방식의 
         * API를 제공한다.
         * 
         * 프레임 값은 반드시 배열 프레임 타입을 취해야 한다. 다른 프레임 타입은 에러를 반환하게
         * 된다.
         */
        let mut parse = Parse::new(frame)?;

        /**
         * 모든 레디스 커맨드는 커맨드 이름으로 시작한다. 대소문자를 구분하는 매칭을 위해, 
         * 이 이름을 읽어서 소문자로 변환한다.
         */
        let command_name = parse.next_string()?.to_lowercase();

        /**
         * 커맨드 이름을 매칭하고 나머지 값들은 해당 커맨드에 위임한다.
         */
        let command = match &command_name[..] {
            "get" => Command::Get(Get::parse_frames(&mut parse)?),
            "publish" => Command::Publish(Publish::parse_frames(&mut parse)?),
            "set" => Command::Set(Set::parse_frames(&mut parse)?),
            "subscribe" => Command::Subscribe(Subscribe::parse_frames(&mut parse)?),
            "unsubscribe" => Command::Unsubscribe(Unsubscribe::parse_frames(&mut parse)?),
            _ => {
                /**
                 * 지원하지 않는 커맨드는 Unknwon 커맨드로 반환한다.
                 * 
                 * 여기의 'return'은 아래의 'finish()' 호출을 생략하기 위한 것이다.
                 * 지원하지 않는 커맨드이기 때문에, 높은 확률로 아직 소비되지 않는 필드가
                 * 'Parse' 인스턴스에 남아있을 수 있다.
                 */
                return Ok(Command::Unknwon(Unknown::new(command_name)));
            }
        };

        /**
         * 'Parse' 값에 소비되지 않은 값이 남아있는지 확인한다. 만약 남아있다면 이 프레임은
         * 예상하지 못한 형식을 취하고 있음을 의미하며, 에러를 반환한다.
         */
        parse.finish()?;

        // 커맨드 파싱이 성공적으로 이루어졌다.
        Ok(command)
    }

    /**
     * 커맨드를 특정 'Db' 인스턴스에 수행한다.
     * 
     * 응답은 'dst'에 쓴다. 수신한 커맨드를 실행하기 위해, 이 함수는 서버가 호출한다.
     */
    pub(crate) async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        shutdown: &mut Shutdown,
    ) -> crate::Result<()> {
        use Command::*;

        match self {
            Get(cmd) => cmd.apply(db, dst).await,
            Publish(cmd) => cmd.apply(db, dst).await,
            Set(cmd) => cmd.apply(db, dst).await,
            Subscribe(cmd) => cmd.apply(db, dst, shutdown).await,
            Unknwon(cmd) => cmd.apply(db).await,
            /**
             * 'Unsubscribe'는 수행할 수 없다. 이 커맨드는 'Subscribe' 커맨드로부터만 
             * 수신한다.
             */
            Unsubscribe(_) => Err("'Unsubscribe' is unsupported in this context".into()),
        }
    }

    // 커맨드 이름을 반환한다.
    pub(crate) fn get_name(&self) -> &str {
        match self {
            Command::Get(_) => "get",
            Command::Publish(_) => "pub",
            Command::Set(_) => "set",
            Command::Subscribe(_) => "subscribe",
            Command::Unsubscribe(_) => "unsubscribe",
            Command::Unknwon(cmd) => cmd.get_name(),
        }
    }
}

