use std::io::{ErrorKind, Read, Write};

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

    fn decode_in_context<R: Read>(&self, reader: &mut R, _context: Context) -> DecodingResult<bool> {
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

// pub struct BytesCoder;
//
// impl Coder for BytesCoder {
//     type T = [u8];
//
//     fn encode_in_context<W: Write>(&self, element: &Self::T, writer: &mut W, context: Context) -> EncodingResult {
//         if context == Context::NeedsDelimiters {
//             writer.write(From::from(element.len()))?
//         }
//         writer.write(element)?;
//         Ok(())
//     }
//
//     fn decode_in_context<R: Read>(&self, reader: &mut R, context: Context) -> DecodingResult<Self::T> {
//         let mut size_buf = [0u8; 4];
//         VarIntCoder.read(reader, Context::NeedsDelimiters)
//     }
// }
