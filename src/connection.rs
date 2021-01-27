
/**
 * 원격 피어로부터 'Frame' 값을 송신/수신한다.
 *
 * 네트워크 프로토콜을 구현할 때, 프로토콜 상의 하나의 메시지는 주로 프레임이라고 하는
 * 작은 메시지들로 구성된다. 'Connection'은 'TcpStream' 위에서 프레임들을 읽고 쓰는데에 목적이 있다.
 *
 * 'Connection'은 프레임을 읽기 위해 내부 버퍼를 사용한다. 완전한 하나의 프레임을 생성하기
 * 위한 충분한 수의 바이트가 모일 때까지 버퍼를 채우다가, 버퍼가 가득 차면 'Conneciton'은 프레임을 생성하고
 * 이를 호출자에게 반환한다.
 *
 * 프레임을 쓸(writing) 때는 먼저 프레임을 인코딩하여 버퍼에 쓴 뒤, 버퍼의 내용을 소켓에 쓴다.
 */
pub struct Connection {
    /**
     * 'TcpStream'을 'BufWriter'로 감싸 쓰기 레벨의 버퍼링을 지원한다.
     * Tokio의 'BufWriter' 구현체는 이 프로그램의 요구사항을 만족시키기에 충분하다.
     */
    stream: BufWriter<TcpStream>,

    // 프레임 읽기에 사용될 버퍼.
    buffer: BytesMut,
}

impl Connection {

    /**
     * 'socket' 기반의 새 'Connnection'을 생성한다.
     * 읽기/쓰기 버퍼를 초기화한다.
     */
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),

            /**
             * 읽기 버퍼의 기본 크기는 4KB가 된다. mini redis의 사용에 있어 이 크기는
             * 충분하다. 하지만 실제 어플리케이션의 경우 이 값을 특정한 사용처에 맞게 조정해야 한다.
             * 이보다 큰 사이즈의 버퍼가 더 잘 작동할 가능성이 높다.
             */
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /**
     * 기반 스트림으로부터 'Frame' 하나를 읽어들인다.
     * 
     * 이 함수는 한 프레임을 만들기 위한 충분한 데이터가 모일 때까지 기다린다.
     * 한 프레임이 만들어진 뒤에 읽기 버퍼에 남아있는 데이터는 다음 'read_frame' 호출을 위해
     * 그대로 남겨진다.
     * 
     * # 반환값
     * 성공할 경우 frame을 반환한다. 'TcpStream'이 프레임을 반으로 나누지 않는 방식으로 닫히면
     * 'None'을 반환한다. 그렇지 않으면 에러를 반환한다.
     */
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            /**
             * 버퍼 데이터로부터 프레임을 파싱한다.
             * 데이터가 프레임을 만들기에 충분하다면 프레임을 반환한다.
             */
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            /**
             * 버퍼 데이터가 프레임을 만들기에 충분하지 않다면, 소켓으로부터 데이터를 더 읽어들인다.
             * 
             * 읽기에 성공하면 읽어들인 바이트의 수를 반환한다. 반환값 '0'은 "end of stream"을 의미한다.
             */
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    /**
     * 버퍼로부터 프레임 파싱을 시도한다. 버퍼의 데이터가 충분하다면 프레임을 반환하고
     * 버퍼의 데이터를 제거한다. 데이터가 아직 충분하지 않다면 'Ok(None)'을 반환한다.
     * 버퍼 데이터가 유효한 프레임을 나타내지 않는다면 'Err'를 반환한다.
     */
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        /**
         * Cursor를 사용하여 버퍼의 "현재" 위치를 추적한다. Cursor는 'bytes' crate의
         * 'Buf'를 구현한다. 'bytes' crate는 바이트를 다루기 위한 많은 유용한 유틸리티를
         * 제공한다.
         */
        let mut buf = Cursor::new(&self.buffer[..]);

        /**
         * 파싱의 첫 단계는, 먼저 버퍼에 하나의 프레임을 만들기에 충분한 데이터가 존재하는지
         * 확인하는 일이다. 보통 이 단계는 프레임 전체 파싱보다 훨씬 빠르게 동작하며, 프레임
         * 전체를 수신했음을 아직 알지 못하는 상황에서 프레임 데이터를 보유하기 위한 데이터 구조 할당을
         * 생략할 수 있도록 해준다.
         */
        match Frame::check(&mut buf) {
            Ok(_) => {
                /**
                 * 'check'함수는 커서를 프레임의 끝까지 전진시킬 것이다.
                 * 'Frame::check'를 호출하기 전까지 커서의 포지션을 0 으로 세팅되기 때문에,
                 * 프레임의 크기는 커서의 포지션을 체크함으로써 얻을 수 있다.
                 */
                let len = buf.position() as usize;

                /**
                 * 'Frame::parse'를 호출하기 전에 커서의 포지션을 0 으로 세팅한다.
                 */
                buf.set_position(0);
                
                /**
                 * 버퍼로부터 프레임을 파싱한다. 이 동작은 프레임을 표현하기 위해 필요한 데이터 구조를
                 * 할당하고 프레임 값을 반환한다.
                 * 
                 * 인코딩된 프레임이 유효하지 않다면 에러를 반환한다. 이 경우 현재 커넥션을 종료해야
                 * 하지만, 동시에 다른 어떠한 클라이언트 커넥션에도 영향을 주지 않아야 한다.
                 */
                let frame = Frame::parse(&mut buf)?;

                /**
                 * 파싱된 데이터를 읽기 버퍼에서 제거한다.
                 * 
                 * 'advance'가 읽기 버퍼에 호출되면 'len'까지의 모든 데이터는 폐기된다.
                 * 상세한 동작 방식은 'BytesMut'가 가지고 있는데, 주로 내부 커서를 이동하는 방식으로
                 * 이루어지지만, 데이터 공간을 재할당하고 데이터를 복사하는 방식이 사용될 수도 있다.
                 */
                self.buffer.advance(len);

                // 파싱된 프레임을 호출자에게 반환한다.
                Ok(Some(frame))
            }

            /**
             * 버퍼의 데이터가 하나의 프레임을 만들기에 부족하다면, 소켓으로부터의 추가적인 데이터
             * 수신을 위해 대기해야 한다. 소켓 읽기는 이 'match' 후에 완료된다.
             * 
             * 여기에서 'Err'을 반환하지 않는 이유는 이 "에러"는 런타임에 예상할 수 있는 결과이기 때문이다.
             */
            Err(Incomplete) => Ok(None),
            /**
             * 프레임 파싱 중 발생한 에러. 커넥션은 이제 무효한 상태가 된다.
             * 여기서 반환하는 'Err'는 커넥션을 닫을 것이다.
             */
            Err(e) => Err(e.into()),
        }
    }

    /**
     * 한 'Frame' 값을 기반 스트림에 쓴다(write).
     * 
     * 'Frame' 값은 'AsyncWrite'가 제공하는 다양한 'write_*' 함수를 통해 소켓에 쓰여진다.
     * 이 함수들을 'TcpStream'에 직접 호출하는 일은 권장되지 않는다. 왜냐하면 이런 방식은 아주 많은
     * syscalls를 발생시키기 때문이다. 하지만 버퍼링된 쓰기 스트림에 대해서는 이런 방식도 괜찮다.
     * 데이터가 소켓이 아닌 버퍼에 쓰여지기 때문이다. 버퍼가 가득 차면 기반 소켓에 flush된다.
     */
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        /**
         * 배열은 배열 안의 각 앤트리를 인코딩하는 방식으로 인코딩된다. 다른 모든 프레임 타입은
         * 리터럴로 취급된다. 지금의 mini-redis는 재귀적 프레임 구조로 인코딩할 수 없다.
         * 자세한 내용은 아래에서 다룬다.
         */
        match frame {
            Frame::Array(val) => {
                // 프레임 타입 접두어를 인코딩한다. 배열의 경우, 이는 '*'가 된다.
                self.stream.write_u8(b'*').await?;
                
                // 배열의 길이를 인코딩한다.
                self.write_decimal(val.len() as u64).await?;

                // 배열 안의 각 앤트리를 순회하며 인코딩한다.
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // 프레임 타입이 리터럴이다. 값을 직접 인코딩한다.
            _ => self.write_value(frame).await?,
        }
        /**
         * 인코딩된 프레임을 소켓에 쓴다. 위 코드의 각 write 호출들은 버퍼 스트림에 이루어지고, 쓰여진다.
         * 'flush' 호출은 버퍼에 남아있는 내용을 소켓에 쓴다.
         */
        self.stream.flush().await
    }

    // 프레임 리터럴을 스트림에 쓴다.
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.steram.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await()?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }

            /**
             * 프레임 값 안의 'Array'는 재귀적으로 인코딩할 수 없다. 일반적으로, async 함수들은
             * 재귀를 지원하지 않는다. mini-redis는 아직 중첩 배열 인코딩이 필요하지 않다.
             * 때문에 당장은 이 경우는 생략한다.
             */
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    // 십진수 프레임을 스트림에 쓴다.
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // 값을 문자열로 변환한다.
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}