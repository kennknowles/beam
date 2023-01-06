/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * For a given coder, returns a parser for the standard coder test
 * representation of its test cases.
 */
// fn get_value_parser(coderRepr: CoderRepr) -> fn(str) -> Any {
//   match coderRepr.urn {
//     //"beam:coder:bytes:v1"       => |x| { new TextEncoder().encode(x) },
//     "beam:coder:bool:v1"        => |x| { x },
//     "beam:coder:string_utf8:v1" => |x| { x },
//     "beam:coder:varint:v1"      => |x| { x },
//     "beam:coder:double:v1"      => |x| { x.parse() },
//
//     "beam:coder:kv:v1" => |x| {
//       let keyParser = get_value_parser(coderRepr.components[0]);
//       let valueParser = get_value_parser(coderRepr.components[1]);
//       KV {
//         key: components[0](x["key"]),
//         value: components[1](x["value"]),
//       }
//     },
//       "beam:coder:iterable:v1" =>  |x| {
//         let elemParser = get_value_parser(coderRepr.components[0]);
//         x.map(elemParser)
//       },
//       "beam:coder:global_window:v1" => |x| {
//          GlobalWindow()
//       },
//       "beam:coder:interval_window:v1" => |x| {
//         IntervalWindow {
//           start: Long.fromNumber(x.end).sub(x.span),
//           end: Long.fromNumber(x.end),
//         }
//       },
//       // "beam:coder:windowed_value:v1" => |x| {
//       //   value: get_value_parser(components[0])(x.value),
//       //   windows: x.windows.map(get_value_parser(components[1])),
//       //   pane: {
//       //     isLast: x.pane.is_last,
//       //     isFirst: x.pane.is_first,
//       //     index: x.pane.index,
//       //     onTimeIndex: x.pane.on_time_index,
//       //     timing: Timing[x.pane.timing],
//       //   },
//       //   timestamp: Long.fromNumber(x.timestamp),
//       // },
//       // "beam:coder:nullable:v1" => |x| {
//       //    let value_parser = get_value_parser(components[0])
//       //    x === null || x === undefined ? undefined : value_parser(x);
//       // }
//       _ =>
//         util.format("Do not know how to parse example values for %s", coderRepr)
//     }
//   }

#[cfg(test)]
mod tests {
  extern crate apache_beam;

  use serde::{Serialize, Deserialize};
  use serde_yaml::Value;
  use std::collections::HashMap;
  use anyhow::{anyhow, bail, Context};
  use apache_beam::coders::{BoolCoder, BytesCoder, Coder, CoderContext};
  // use std::{thread, time};


  const STANDARD_CODERS_FILE: &str =
    "../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml";

  #[derive(Debug, PartialEq, Serialize, Deserialize)]
  struct CoderRepr {
    urn: String,
    payload: Option<String>,
    components: Option<Vec<CoderRepr>>,
    non_deterministic: Option<String>, // bool but our YAML is 1.1 and has upper and lowercase
  }

  #[derive(Debug, PartialEq, Serialize, Deserialize)]
  struct CoderSpec {
    coder: CoderRepr,
    nested: Option<String>,
    // bool but our YAML is 1.1 and has uppper and lowercase booleans
    examples: HashMap<String, Value>,
  }

  enum CoderReprError {
    Unsupported(String),
    BadYaml(CoderRepr),
  }

  type CoderTester = fn(&String, &Value, &CoderContext) -> Result<(), anyhow::Error>;

  fn bool_coder_tester(s: &String, v: &Value, _context: &CoderContext) -> Result<(), anyhow::Error> {
    let coder = BoolCoder;
    let value = v.as_bool().ok_or(anyhow!("YAML was not a bool"))?;      

    let mut bytes_to_decode = s.as_bytes();
    let decoded = coder.decode(&mut bytes_to_decode)?;
    if value != decoded {
      bail!("bytes {:?} should decode to {:?} but got {:?} instead",
        bytes_to_decode, value, decoded)
    }

    let expected_bytes = s.as_bytes(); // above bytes_to_decode was consumed
    let mut encoded_bytes = [7u8];
    coder.encode(&value, &mut encoded_bytes.as_mut_slice())?;
    if encoded_bytes != expected_bytes {
      bail!("value {:?} should encode to {:?} but got {:?} instead",
        value, bytes_to_decode, expected_bytes)
    }

    Ok(())
  }
  fn string_to_bytes(s: &String) -> Vec<u8> {
    let mut v = vec![0u8; 0];
    for c in s.chars() {
      v.push(c as u8);
    }
    v
  }

  fn bytes_coder_tester(s: &String, v: &Value, context: &CoderContext) -> Result<(), anyhow::Error> {

    let coder = BytesCoder;
    let value_str: &str  = v.as_str().ok_or(anyhow!("YAML was not a string"))?;
    let value_string: String = value_str.to_string();
    println!("bytes_coder_tester, {:?} {:?} {:?}", s, value_string, context);

    let value: Vec<u8> = value_string.into_bytes();

    println!("chars {:?}", s.chars().nth(0).unwrap());
    println!("chars {:?}", s.chars().nth(0).unwrap() as u32);
    
    let mut bytes_to_decode_vec: Vec<u8> = string_to_bytes(s);
    
    println!("bytes_to_decode {:?}", bytes_to_decode_vec);
    
    let mut bytes_to_decode_reader: &[u8] = bytes_to_decode_vec.as_mut_slice();
    
    let decoded = coder.decode_in_context(&mut bytes_to_decode_reader, context)?;

    println!("decoded {:?}", decoded);
    
    if value != decoded {
      bail!("bytes {:?} should decode to {:?} but got {:?} instead",
          s.as_bytes(), value, decoded)
    }

    let expected_bytes = string_to_bytes(s); // above bytes_to_decode was consumed
    let mut encoded_bytes = vec![0u8; expected_bytes.len()];
    coder.encode_in_context(&value, &mut encoded_bytes.as_mut_slice(), context)?;
    if encoded_bytes != expected_bytes {
      bail!("value {:?} should encode to {:?} but got {:?} instead",
          value, expected_bytes, encoded_bytes)
    }

    Ok(())
  }

  fn repr_to_coder_tester(repr: CoderRepr) -> Result<CoderTester, CoderReprError> {
    match repr.urn.as_str() {
      "beam:coder:bool:v1" => Ok(bool_coder_tester),
      "beam:coder:bytes:v1" => Ok(bytes_coder_tester),
      "beam:coder:kv:v1" |
      "beam:coder:iterable:v1" |
      "beam:coder:double:v1" |
      "beam:coder:varint:v1" |
      "beam:coder:global_window:v1" |
      "beam:coder:interval_window:v1" |
      "beam:coder:windowed_value:v1" |
      "beam:coder:param_windowed_value:v1" |
      "beam:coder:nullable:v1" |
      "beam:coder:row:v1" |
      "beam:coder:sharded_key:v1" |
      "beam:coder:state_backed_iterable:v1" |
      "beam:coder:custom_window:v1" |
      "beam:coder:timer:v1" |
      "beam:coder:string_utf8:v1" => Err(CoderReprError::Unsupported(repr.urn)),
      _ => Err(CoderReprError::BadYaml(repr))
    }
  }

  #[test]
  fn standard_coder_tests() -> Result<(), anyhow::Error> {
    let f = std::fs::File::open(STANDARD_CODERS_FILE)?;
    for doc in serde_yaml::Deserializer::from_reader(f) {
      let spec = CoderSpec::deserialize(doc)?;

      match repr_to_coder_tester(spec.coder) {
        Err(CoderReprError::Unsupported(_repr)) => (),
        Err(CoderReprError::BadYaml(repr)) => bail!("could not understand coder YAML: {:?}", repr),

        Ok(coder_tester) => {
          let mut context = CoderContext::WholeStream;
          if spec.nested.unwrap_or("".to_string()).as_str() == "true" {
            context = CoderContext::NeedsDelimiters
          }
          for (serialized, yaml_value) in spec.examples {
            coder_tester(&serialized, &yaml_value, &context)
              .with_context(|| format!("{:?}: {:?}", serialized, yaml_value))?;
          }
        },
      }
    }
    Ok(())
  }
}


// describe("standard Beam coders on Javascript", function () {
//   const docs: Array<CoderSpec> = yaml.loadAll(
//     fs.readFileSync(STANDARD_CODERS_FILE, "utf8")
//   );
//   docs.forEach((doc) => {
//     const urn = doc.coder.urn;
//     if (UNSUPPORTED_CODERS.includes(urn)) {
//       return;
//     }
//
//     // The YAML is designed so doc.nested is three-valued, and undefined means "test both variations"
//     var contexts: Context[] = [];
//     if (doc.nested !== true) {
//       contexts.push(Context.wholeStream);
//     }
//     if (doc.nested !== false) {
//       contexts.push(Context.needsDelimiters);
//     }
//
//     contexts.forEach((context) => {
//       describe("in Context " + context, function () {
//         const spec = doc;
//         var components;
//         if (spec.coder.components) {
//           components = spec.coder.components.map(
//             // Second level coders have neither payloads nor components.
//             (c) => globalRegistry().getCoder(c.urn)
//           );
//         } else {
//           components = [];
//         }
//         const coder = globalRegistry().getCoder(urn, undefined, ...components);
//         runCoderTest(coder, urn, context, spec);
//       });
//     });
//   });
// });
//

// function runCoderTest<T>(coder: Coder<T>, urn, context, spec: CoderSpec) {
//   describe(
//     util.format(
//       "coder %s (%s)",
//       util.inspect(coder, { colors: true, breakLength: Infinity }),
//       spec.coder.non_deterministic ? "nondeterministic" : "deterministic"
//     ),
//     function () {
//       const parser = get_json_value_parser(spec.coder);
//       for (let expected in spec.examples) {
//         var value = parser(spec.examples[expected]);
//         const expectedEncoded = Buffer.from(expected, "binary");
//         if (
//           (UNSUPPORTED_EXAMPLES[spec.coder.urn] || []).includes(
//             expectedEncoded.toString("hex")
//           )
//         ) {
//           continue;
//         }
//         coderCase(
//           coder,
//           value,
//           expectedEncoded,
//           context,
//           spec.coder.non_deterministic || false
//         );
//       }
//     }
//   );
// }
//
// function coderCase<T>(
//   coder: Coder<T>,
//   obj,
//   expectedEncoded: Uint8Array,
//   context,
//   non_deterministic
// ) {
//   let encodeObj;
//   // Normally we would not support non-deterministic cases, but in the
//   // implementation that we have, the non-deterministic encodings happen
//   // to match our samples.
//   if (non_deterministic && coder instanceof IterableCoder) {
//     // We support the particular case of iterables of unknown length.
//     // The encoding here is not deterministic, but the test case works
//     // fine.
//     let typedIterable: Iterable<any> = (function* it() {
//       for (let elm in obj) {
//         yield obj[elm];
//       }
//     })();
//     encodeObj = typedIterable;
//   } else {
//     encodeObj = obj;
//   }
//   it(
//     util.format(
//       "encodes %s to %s",
//       util.inspect(encodeObj, { colors: true, depth: Infinity }),
//       Buffer.from(expectedEncoded).toString("hex")
//     ),
//     function () {
//       var writer = new Writer();
//       coder.encode(encodeObj, writer, context);
//       assertions.deepEqual(writer.finish(), expectedEncoded);
//     }
//   );
//
//   it(
//     util.format(
//       "decodes %s to %s correctly",
//       Buffer.from(expectedEncoded).toString("hex"),
//       util.inspect(obj, { colors: true, depth: Infinity })
//     ),
//     function () {
//       const decoded = coder.decode(new Reader(expectedEncoded), context);
//       assertions.deepEqual(decoded, obj);
//     }
//   );
// }
