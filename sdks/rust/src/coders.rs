extern crate varint;
use varint::{ VarintRead, VarintWrite };

use std::io::Cursor;
use std::io::{ErrorKind, Read, Write};
use std::io::Error;

pub trait Coder {
    type T;

    /**
     * Encode an element into a stream of bytes.
     * @param element - an element within a PCollection
     * @param writer - a writer that interfaces the coder with the output byte stream.
     * @param context - the context within which the element should be encoded.
     */
    fn encode_in_context<W: Write>(&self, element: &Self::T, writer: &mut W, context: Context) -> EncodingResult;

    fn encode<W: Write>(&self, element: &Self::T, writer: &mut W) -> EncodingResult {
        self.encode_in_context(element, writer, Context::NeedsDelimiters)
    }

    /**
     * Decode an element from an incoming stream of bytes.
     * @param reader - a reader that interfaces the coder with the input byte stream
     * @param context - the context within which the element should be encoded */
    fn decode_in_context<R: Read>(&self, reader: &mut R, context: Context) -> DecodingResult<Self::T>;

    fn decode<R: Read>(&self, reader: &mut R) -> DecodingResult<Self::T> {
        self.decode_in_context(reader, Context::NeedsDelimiters)
    }

    // /**
    //  * Convert this coder into its protocol buffer representation for the Runner API.
    //  * A coder in protobuf format can be shared with other components such as Beam runners,
    //  * SDK workers; and reconstructed into its runtime representation if necessary.
    //  * @param pipelineContext - a context that holds relevant pipeline attributes such as other coders already in the pipeline.
    //  */
    //fn toProto(pipelineContext: ProtoContext): runnerApi.Coder;
}

pub enum Context {
    WholeStream,
    NeedsDelimiters,
}

type DecodingResult<T> = Result<T, std::io::Error>;
type EncodingResult = Result<(), std::io::Error>;

pub struct BoolCoder;

impl Coder for BoolCoder {
    type T = bool;

    fn encode_in_context<W: Write>(&self, element: &bool, writer: &mut W, _context: Context) -> EncodingResult {
        let byte = if *element { [1u8] } else { [0u8] };
        let _ = writer.write(&byte)?;
        Ok(())
    }

    fn decode_in_context<R: Read>(&self, reader: &mut R, _context: Context) -> DecodingResult<Self::T> {
        let mut buf = [0u8];
        reader.read(&mut buf)?;
        if buf[0] == 1 {
            DecodingResult::Ok(true)
        } else if buf[0] == 0 {
            DecodingResult::Ok(false)
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Encoded bool not 0 or 1",
            ))
        }
    }
}

fn write_signed_varint_32_to_stream<W: Write>(val: i32, writer: &mut W) -> Result<usize, Error> {
    let mut vector = Cursor::new(vec![0u8; 0]);
    assert!(vector.write_signed_varint_32(val).is_ok());
    writer.write(vector.into_inner().as_slice())
}

fn read_signed_varint_32_from_vector(vec: Vec<u8>) -> Result<i32, Error> {
    let mut vector = Cursor::new(vec);
    vector.read_signed_varint_32()
}

// This awkward setup is needed(?) since varint libary only supports reading from vectors but not byte slices.
fn read_signed_varint_32_from_stream<R: Read>(reader: &mut R) -> Result<i32, Error> {
    let mut vec = vec![0u8; 0];
    for _ in 1..6 { // five bytes is enough to fit varint32
        // read one more byte
        let mut buf = [0u8; 1];
        match reader.read(&mut buf) {
            Ok(_) => (),
            Err(err) => return Err(err)
        };
        vec.push(buf[0]);

        // try parsing again with the extra byte
        let val_or = read_signed_varint_32_from_vector(vec.clone());
        if val_or.is_ok() {
            return Ok(val_or.unwrap());
        }
    }
    return Err(Error::new(ErrorKind::Other, "oh no!"));
}

pub struct BytesCoder;

impl Coder for BytesCoder {
    type T = Vec<u8>;

    fn encode_in_context<W: Write>(&self, bytes: &Self::T, writer: &mut W, context: Context) -> EncodingResult {
        if matches!(context, Context::NeedsDelimiters) {
            write_signed_varint_32_to_stream(bytes.len() as i32, writer)?;
        }
        writer.write(bytes.as_slice())?;
        Ok(())
    }

    fn decode_in_context<R: Read>(&self, reader: &mut R, context: Context) -> DecodingResult<Self::T> {
        let mut bytes = Vec::<u8>::new();
        
        if matches!(context, Context::NeedsDelimiters) {
            let sz: i32 = read_signed_varint_32_from_stream(reader)?;
            bytes.resize(sz as usize, 0);
            reader.read_exact(bytes.as_mut_slice())?;
        } else {
            reader.read_to_end(&mut bytes)?;
        }
        Ok(bytes)
    }
}
