
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
     * 
     */
    stream: BufWriter<TcpStream>,

    buffer: BytesMut,
}