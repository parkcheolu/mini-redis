use crate::Frame;

use bytes::Bytes;
use std::{fmt, str, vec};

/// 커맨드 파싱 유틸리티
/// 
/// 커맨드는 프레임의 배열로 표현된다. 프레임의 각 앤트리는 "token"이다.
/// 'Parse'는 프레임 배열으로 초기화되고, 커서 방식의 API를 제공한다.
/// 각 커맨드 struct는 'parse_frames' 메서드를 가지는데, 이 메서드는 'Parse'를 사용하여
/// 커맨드 자신의 필드를 추출한다.
pub(crate) struct Parse {
    /// 프레임 배열 이터레이터
    parts: vec::IntoIter<Frame>,
}

/// 프레임 파싱 과정에서 발생하는 에러.
/// 
/// 런타임에는 오직 'EndOfStream' 에러만 핸들링한다. 다른 모든 에러는 발생 시 커넥션을 종료한다.
#[derive(Debug)]
pub(crate) enum ParseError {
    /// 프레임이 모두 소모되어 발생하는 실패값을 추출한다.
    EndOfStream,

    /// 다른 모든 에러들
    Other(crate::Error),
}

impl Parse {
    /// 프레임의 내용을 파싱하기 위한 새로운 'Parse'를 생성한다.
    /// 
    /// 프레임이 배열 프레임이 아닌 경우 'Err'를 반환한다.
    pub(crate) fn new(frame: Frame) -> Result<Parse, ParseError> {
        let array = match frame {
            Frame::Array(array) => array,
            frame => return Err(format!("protocol error; expected array, got {:?}", frame).into()),
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    /// 다음 앤트리를 반환한다. 배열 프레임은 프레임들의 배열이다.
    /// 즉 다음 앤트리는 프레임이 된다.
    fn next(&mut self) -> Result<Frame, ParseError> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    /// 다음 앤트리를 문자열로 반환한다.
    /// 
    /// 다음 앤트리가 문자열로 표현될 수 없는 경우, 에러를 반환한다.
    pub(crate) fn next_string(&mut self) -> Result<String, ParseError> {
        match self.next()? {
            //  'Simple', 'Bulk'는 문자열으로 표현될 수 있다. 문자열은 UTF-8으로 파싱한다.
            //  
            //  에러는 문자열으로 저장되기 때문에, 별도의 타입으로 간주한다.
            Frame::Simple(s) => Ok(s),
            Frame::Bulk(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| "protocol error; invalid string".into()),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    /// 다음 앤트리를 raw 바이트로 반환한다.
    /// 
    /// 다음 앤트리가 raw 바이트로 표현될 수 없는 경우, 에러를 반환한다.
    pub(crate) fn next_bytes(&mut self) -> Result<Bytes, ParseError> {
        // 'Simple', 'Bulk'는 raw 바이트로 표현할 수 있다.
        // 
        // 에러는 문자열로 저장되고 raw 바이트로 표현할 수 있지만, 별도의 타입으로 간주한다.
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::Bulk(data) => Ok(data),
            frame => Err(format!(
                "protocol error; expected simple frame or bulk frame, got {:?}",
                frame
            )
            .into()),
        }
    }

    /// 다음 앤트리를 integer로 반환한다.
    /// 
    /// 여기에는 'Simple', 'Bulk', 'Integer' 타입의 프레임을 포함한다.
    /// 'Simple', 'Bulk' 타입 프레임을 파싱한다.
    /// 
    /// 다음 앤트리가 integer로 표현될 수 없는 경우, 에러를 반환한다.
    pub(crate) fn next_int(&mut self) -> Result<u64, ParseError> {
        use atoi::atoi;

        const MSG: &str = "protocol error; invalid number";

        match self.next()? {
            // integer 타입 프레임은 이미 integer로 저장되어 있다.
            Frame::Integer(v) => Ok(v),
            // Simple, Bulk 프레임은 반드시 integer로 파싱해야 한다.
            // 파싱에 실패하면 에러를 반환한다.
            Frame::Simple(data) => atoi::<u64>(data.as_bytes()).ok_or_else(|| MSG.into()),
            Frame::Bulk(data) => atoi::<u64>(&data).ok_or_else(|| MSG.into()),
            frame => Err(format!("protocol error; expected int frame but got {:?}", frame).into()),
        }
    }

    /// 배열에 다음 앤트리가 남아있지 않음을 보장한다.
    pub(crate) fn finish(&mut self) -> Result<(), ParseError> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err("protocol error; expected end of frame, but there was more".into())
        }
    }
}

impl From<String> for ParseError {
    fn from(src: String) -> ParseError {
        ParseError::Other(src.into())
    }
}

impl From<&str> for ParseError {
    fn from(src: &str) -> ParseError {
        src.to_string().into()
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParseError::EndOfStream => "protocol error; unexpected end of stream".fmt(f),
            ParseError::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for ParseError {}