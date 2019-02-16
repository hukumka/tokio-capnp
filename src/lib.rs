#[macro_use]
extern crate log;

use bytes::{ByteOrder, LittleEndian};
use capnp::message::{ReaderSegments, Reader, ReaderOptions};
use capnp::{Error as CapnpError, Word};
use futures::try_ready;
use std::ops::Range;
use tokio::io::AsyncWrite;
use tokio::prelude::*;

pub fn read_message<S: AsyncRead>(stream: S) -> MessageRead<S> {
    MessageRead {
        stream,
        owned_space: None,
        state: ReadState::new(),
        table: vec![Word { raw_content: 0 }],
    }
}

pub struct MessageRead<S> {
    stream: S,
    owned_space: Option<OwnedSpace>,
    state: ReadState,
    table: Vec<Word>,
}

pub struct OwnedSpace {
    slices: Vec<Range<usize>>,
    space: Vec<Word>,
}

impl ReaderSegments for OwnedSpace {
    fn len(&self) -> usize {
        self.slices.len()
    }

    fn get_segment(&self, id: u32) -> Option<&[Word]> {
        if let Some(range) = self.slices.get(id as usize) {
            Some(&self.space[range.clone()])
        } else {
            None
        }
    }
}

enum ReadState {
    SegmentTableHeader(usize),
    SegmentTable { read: usize, total_bytes: usize },
    Segments { read: usize, total_bytes: usize },
}

impl ReadState {
    fn new() -> Self {
        ReadState::SegmentTableHeader(0)
    }
}

fn poll_read_helper<S: AsyncRead>(stream: &mut S, buf: &mut [u8]) -> Poll<usize, CapnpError>{
    let res = try_ready!(stream.poll_read(buf));
    if res == 0{
        Err(CapnpError::disconnected("Early EOF".to_string()))
    }else{
        Ok(Async::Ready(res))
    }
}

impl<S: AsyncRead> Future for MessageRead<S> {
    type Item = Reader<OwnedSpace>;
    type Error = CapnpError;

    fn poll(&mut self) -> Poll<Self::Item, CapnpError> {
        const WORD_SIZE: usize = std::mem::size_of::<Word>();
        loop {
            match self.state {
                ReadState::SegmentTableHeader(ref mut read) => {
                    let buf = Word::words_to_bytes_mut(&mut self.table[0..1]);
                    *read += try_ready!(poll_read_helper(&mut self.stream, &mut buf[*read..WORD_SIZE]));
                    if *read >= WORD_SIZE {
                        // prepare buffer for whole segment table
                        let segment_count = LittleEndian::read_u32(&buf[0..4]) + 1;
                        let total_words = table_size_in_words(segment_count as usize);
                        let total_bytes = total_words * WORD_SIZE;
                        self.table.resize(total_words, Word { raw_content: 0 });
                        self.state = ReadState::SegmentTable {
                            read: WORD_SIZE,
                            total_bytes,
                        }; // First word of table was already read read
                    }
                }
                ReadState::SegmentTable {
                    ref mut read,
                    total_bytes,
                } => {
                    let buf = Word::words_to_bytes_mut(&mut self.table);
                    *read += try_ready!(poll_read_helper(&mut self.stream, &mut buf[*read..WORD_SIZE]));
                    if *read >= total_bytes {
                        self.owned_space = Some(OwnedSpace::from_segment_table(&self.table));
                        self.state = ReadState::Segments {
                            read: 0,
                            total_bytes: self.owned_space.as_ref().unwrap().space.len() * WORD_SIZE,
                        };
                    }
                }
                ReadState::Segments {
                    ref mut read,
                    total_bytes,
                } => {
                    let buf =
                        Word::words_to_bytes_mut(&mut self.owned_space.as_mut().unwrap().space);
                    *read += try_ready!(poll_read_helper(&mut self.stream, &mut buf[*read..total_bytes]));
                    if *read >= total_bytes {
                        break;
                    }
                }
            }
        }
        let res = Reader::new(self.owned_space.take().unwrap(), ReaderOptions::default());
        Ok(Async::Ready(res))
    }
}

impl OwnedSpace {
    fn from_segment_table(table: &[Word]) -> Self {
        let buf = Word::words_to_bytes(table);
        let segment_count = LittleEndian::read_u32(&buf[0..4]) as usize + 1;
        let mut total_size = 0;
        let mut slices = vec![];
        for i in 0..segment_count {
            let segment_size = LittleEndian::read_u32(&buf[(i + 1) * 4..(i + 2) * 4]) as usize;
            slices.push(total_size..total_size + segment_size);
            total_size += segment_size;
        }
        let space: Vec<Word> = vec![Word{raw_content: 0}; total_size];
        OwnedSpace { slices, space }
    }
}

/// Write message into stream
pub fn write_message<S: AsyncWrite, T: ReaderSegments>(
    stream: S,
    message: T,
) -> MessageWrite<S, T> {
    MessageWrite {
        message,
        stream,
        state: WriteState::new(),
        table_and_segment: None,
    }
}

pub struct MessageWrite<S, T> {
    /// Stream into which we write our message
    stream: S,
    /// Our message
    message: T,
    /// Current segment to write. For segment 0 we want to write both segment table
    /// and segment
    state: WriteState,
    /// Here we hold combined segment table and first segment
    table_and_segment: Option<Vec<Word>>,
}

/// State of async write operation
struct WriteState {
    /// Current segment to write
    segment_id: usize,
    /// Amount of bytes already written
    bytes_written: usize,
}

impl WriteState {
    fn new() -> Self {
        Self {
            segment_id: 0,
            bytes_written: 0,
        }
    }
}

impl<S, T> Future for MessageWrite<S, T>
where
    S: AsyncWrite,
    T: ReaderSegments,
{
    type Item = ();
    type Error = CapnpError;

    fn poll(&mut self) -> Poll<(), CapnpError> {
        if self.state.segment_id == 0 {
            if self.table_and_segment.is_none() {
                self.table_and_segment = Some(build_table_and_segment(&self.message)?);
            }
            if let Some(ref table_and_segment) = self.table_and_segment {
                let buffer = Word::words_to_bytes(table_and_segment);
                info!("Write: {:?}", buffer);
                try_ready!(poll_write_state_buffer(
                    &mut self.stream,
                    &mut self.state,
                    buffer
                ));
            } else {
                unreachable!();
            }
        }
        while self.state.segment_id < self.message.len() {
            let segment = self
                .message
                .get_segment(self.state.segment_id as u32)
                .unwrap();
            let buffer = Word::words_to_bytes(segment);
            try_ready!(poll_write_state_buffer(
                &mut self.stream,
                &mut self.state,
                buffer
            ));
        }
        Ok(Async::Ready(()))
    }
}

/// Simple helper function
fn poll_write_state_buffer<S: AsyncWrite>(
    stream: &mut S,
    state: &mut WriteState,
    current_buf: &[u8],
) -> Poll<(), CapnpError> {
    while state.bytes_written < current_buf.len() {
        state.bytes_written += try_ready!(stream.poll_write(&current_buf[state.bytes_written..]));
    }
    state.segment_id += 1;
    state.bytes_written = 0;
    Ok(Async::Ready(()))
}

/// Build Vec containing both segment table and first segment in continious
/// memory.
fn build_table_and_segment<T: ReaderSegments>(message: &T) -> Result<Vec<Word>, CapnpError> {
    let segment = message.get_segment(0).ok_or_else(|| {
        CapnpError::failed("Message must contain at least 1 segment.".to_string())
    })?;
    let table_size = table_size_in_words(message.len());
    let mut res = Word::allocate_zeroed_vec(table_size + segment.len());
    write_table(message, &mut res[0..table_size])?;
    (&mut res[table_size..]).copy_from_slice(segment);
    Ok(res)
}

#[inline]
/// Compute hold many words needed to store segment table (including padding)
fn table_size_in_words(segment_count: usize) -> usize {
    segment_count / 2 + 1
}

/// Write segment table into buffer
fn write_table<T: ReaderSegments>(message: &T, into_buffer: &mut [Word]) -> Result<(), CapnpError> {
    const MAX_SEGMENT_COUNT: usize = 512;
    const MAX_SEGMENT_SIZE: usize = 1 << 16;
    const MAX_TOTAL_SIZE: usize = 1 << 16;
    if message.len() > MAX_SEGMENT_COUNT {
        return Err(CapnpError::failed(format!(
            "Message takes {} segments, but MAX_SEGMENT_COUNT is {}",
            message.len(),
            MAX_SEGMENT_COUNT
        )));
    }
    if table_size_in_words(message.len()) > into_buffer.len() {
        return Err(CapnpError::failed("Buffer not large enough".to_string()));
    }
    let buffer = Word::words_to_bytes_mut(into_buffer);
    LittleEndian::write_u32(&mut buffer[0..4], (message.len() - 1) as u32);
    // Write zero over place where padding would end up if it present.
    // If it's not, when it will be overriten by actual value, so we ok.
    let padding_range = (buffer.len() - 4)..buffer.len();
    LittleEndian::write_u32(&mut buffer[padding_range], 0u32);

    let mut total_size = 0;
    for i in 0..message.len() {
        let segment = message.get_segment(i as u32).unwrap();
        total_size += segment.len();
        if segment.len() > MAX_SEGMENT_SIZE {
            return Err(CapnpError::failed(format!(
                "Segment takes {} words, but MAX_SEGMENT_SIZE is {}",
                segment.len(),
                MAX_SEGMENT_SIZE
            )));
        }
        LittleEndian::write_u32(&mut buffer[4 * (i + 1)..4 * (i + 2)], segment.len() as u32);
    }
    if total_size > MAX_TOTAL_SIZE {
        return Err(CapnpError::failed(format!(
            "Total size is {} words, but MAX_TOTAL_SIZE is {}",
            total_size, MAX_SEGMENT_SIZE
        )));
    }
    Ok(())
}
