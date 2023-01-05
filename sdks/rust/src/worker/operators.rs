use std::collections::HashMap;
use std::iter::Iterator;

use log::info;
use std::boxed::Box;
use std::cell::RefCell;
use std::rc::Rc;

use std::any::Any;
use std::fmt::Debug;

use std::sync::Mutex;

use beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;

pub mod beam_api {
    tonic::include_proto!("beam_api");
}

type SomeError = String;

////////////////// Define some URNs //////////////////

pub static IMPULSE_URN: &str = "beam:transform:impulse:v1";
pub static FLATTEN_URN: &str = "beam:transform:flatten:v1";
pub static PAR_DO_URN: &str = "beam:transform:par_do:v1";
pub static GBK_URN: &str = "beam:transform:group_by_key:v1";
pub static RUNNER_SOURCE_URN: &str = "beam:runner:source:v1";
pub static RUNNER_SINK_URN: &str = "beam:runner:sink:v1";

////////////////// DoFn Serialization //////////////////

// TODO: Give these start/finish_bundles, etc.
type GenericDoFn = Box<dyn Fn(&dyn Any) -> Box<dyn Iterator<Item = Box<dyn Any>>>>;

struct GenericDoFnWrapper {
    func: GenericDoFn,
}

unsafe impl std::marker::Send for GenericDoFnWrapper {}

struct BoxedIter<O: Any, I: IntoIterator<Item = O>> {
    typed_iter: I::IntoIter,
}

impl<O: Any, I: IntoIterator<Item = O>> Iterator for BoxedIter<O, I> {
    type Item = Box<dyn Any>;

    fn next(&mut self) -> Option<Box<dyn Any>> {
        if let Some(x) = self.typed_iter.next() {
            return Some(Box::new(x));
        } else {
            return None;
        }
    }
}

pub fn to_generic_dofn<T: Any, O: Any, I: IntoIterator<Item = O> + 'static>(
    func: fn(&T) -> I,
) -> GenericDoFn {
    Box::new(
        move |untyped_input: &dyn Any| -> Box<dyn Iterator<Item = Box<dyn Any>>> {
            let typed_input: &T = untyped_input.downcast_ref::<T>().unwrap();
            Box::new(BoxedIter::<O, I> {
                typed_iter: func(typed_input).into_iter(),
            })
        },
    )
}

lazy_static! {
    static ref SERIALIZED_FNS: Mutex<HashMap<String, GenericDoFnWrapper>> =
        Mutex::new(HashMap::new());
}

pub fn serialize_fn(func: GenericDoFn) -> String {
    let name = format!("name{}", SERIALIZED_FNS.lock().unwrap().len());
    SERIALIZED_FNS
        .lock()
        .unwrap()
        .insert(name.to_string(), GenericDoFnWrapper { func: func });
    return name;
}

pub fn deserialize_fn(name: &String) -> Option<&'static GenericDoFn> {
    unsafe {
        return std::mem::transmute::<Option<&GenericDoFn>, Option<&'static GenericDoFn>>(
            SERIALIZED_FNS
                .lock()
                .unwrap()
                .get(name)
                .map(|wrapper| &wrapper.func),
        );
    }
}

////////////////// Windowed Element Primitives //////////////////

pub trait Window: core::fmt::Debug {}

#[derive(Clone, Debug)]
pub struct GlobalWindow;

impl Window for GlobalWindow {}

// TODO: Does this type parameter do any good, or should we just let value be Any?
#[derive(Debug)]
pub struct WindowedValue<T: ?Sized> {
    windows: Rc<Vec<Rc<dyn Window>>>, // Using Rc rather than Box for with_value.
    timestamp: std::time::Instant,
    pane_info: Box<[u8]>,
    value: Box<T>,
}

impl<T: ?Sized> WindowedValue<T> {
    fn in_global_window(value: Box<T>) -> WindowedValue<T> {
        WindowedValue {
            windows: Rc::new(vec![Rc::new(GlobalWindow {})]),
            timestamp: std::time::Instant::now(), // TODO: MinTimestamp
            pane_info: Box::new([]),
            value: value,
        }
    }

    fn with_value<O: ?Sized>(&self, value: Box<O>) -> WindowedValue<O> {
        WindowedValue {
            windows: self.windows.clone(),
            timestamp: self.timestamp,
            pane_info: self.pane_info.clone(),
            value: value,
        }
    }
}

////////////////// Operator definitions //////////////////

pub trait Operator: core::fmt::Debug {
    fn start(&self) -> Result<(), SomeError>;
    fn process(&self, element: &WindowedValue<dyn Any>) -> Result<(), SomeError>;
    fn finish(&self) -> Result<(), SomeError>;
}

#[derive(Debug)]
struct DataSourceOperator {
    // consumers: Vec<Rc<dyn Operator>>,
}

impl Operator for DataSourceOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, _element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        return Err("Data sources should never have inputs.".to_string());
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

#[derive(Debug)]
struct DataSinkOperator {}

impl Operator for DataSinkOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, _element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

//#[derive(Debug)]
struct DoOperator {
    consumers: Vec<Rc<dyn Operator>>,
    func: &'static GenericDoFn,
}

impl Operator for DoOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, windowed_element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        println!("DoFn {:?}", windowed_element);
        let func = self.func; // Can't use self.func directly in a call.
        for output in &mut func(&*windowed_element.value) {
            let windowed_output = windowed_element.with_value(output);
            for consumer in self.consumers.iter() {
                consumer.process(&windowed_output)?;
            }
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

impl std::fmt::Debug for DoOperator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DoOperator")
    }
}

#[derive(Debug)]
struct FlattenOperator {
    consumers: Vec<Rc<dyn Operator>>,
}

impl Operator for FlattenOperator {
    fn start(&self) -> Result<(), SomeError> {
        Ok(())
    }
    fn process(&self, windowed_element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        if let Some(x) = windowed_element.value.downcast_ref::<String>() {
            println!("Flatten {:?}", x);
        } else {
            println!("Flatten {:?}", windowed_element.value);
        }
        for consumer in self.consumers.iter() {
            consumer.process(windowed_element)?;
        }
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

#[derive(Debug)]
struct ImpulseForDirectRunnerOperator {
    consumers: Vec<Rc<dyn Operator>>,
}

impl Operator for ImpulseForDirectRunnerOperator {
    fn start(&self) -> Result<(), SomeError> {
        for consumer in self.consumers.iter() {
            consumer.process(&WindowedValue::in_global_window(Box::new(
                "IMPULSE_ELEMENT".to_string(),
            )))?;
        }
        Ok(())
    }
    fn process(&self, _element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        return Err("Data sources should never have inputs.".to_string());
    }
    fn finish(&self) -> Result<(), SomeError> {
        Ok(())
    }
}

trait KeyExtractor {
    fn extract(&self, kv: &dyn Any) -> (String, Box<dyn Any>);
    fn recombine(&self, key: &String, values: &Box<Vec<Box<dyn Any>>>) -> Box<dyn Any>;
}

struct TypedKeyExtractor<V> {
    _v: Option<V>, // so the compiler doesn't complain about unused type param.
}

impl<V: Clone + 'static> KeyExtractor for TypedKeyExtractor<V> {
    fn extract(&self, kv: &dyn Any) -> (String, Box<dyn Any>) {
        //println!("got type {:?} want {:?} {}" , kv.type_id(), std::any::TypeId::of::<(String, V)>(), std::any::type_name::<(String, V)>());
        let typed_kv = kv.downcast_ref::<(String, V)>().unwrap();
        return (typed_kv.0.clone(), Box::new(typed_kv.1.clone()));
    }
    fn recombine(&self, key: &String, values: &Box<Vec<Box<dyn Any>>>) -> Box<dyn Any> {
        let mut typed_values: Vec<V> = Vec::new();
        for untyped_value in values.iter() {
            typed_values.push(untyped_value.downcast_ref::<V>().unwrap().clone());
        }
        return Box::new((key.clone(), typed_values));
    }
}

fn create_key_extractor(name: &String) -> Result<Box<dyn KeyExtractor>, SomeError> {
    if name == "StringString" {
        return Ok(Box::new(TypedKeyExtractor::<String> { _v: None }));
    } else if name == "StringInt32" {
        return Ok(Box::new(TypedKeyExtractor::<i32> { _v: None }));
    } else if name == "StringInt64" {
        return Ok(Box::new(TypedKeyExtractor::<i64> { _v: None }));
    } else {
        return Err(format!("Unknown key extractor type: {}", name));
    }
}

//#[derive(Debug)]
struct GroupByKeyForDirectRunnerOperator {
    consumers: Vec<Rc<dyn Operator>>,
    grouped_values: RefCell<HashMap<String, Box<Vec<Box<dyn Any>>>>>,
    // TODO: Should this be a RefCell as well?
    key_extractor: Box<dyn KeyExtractor>,
}

impl Operator for GroupByKeyForDirectRunnerOperator {
    fn start(&self) -> Result<(), SomeError> {
        self.grouped_values.borrow_mut().clear();
        Ok(())
    }
    fn process(&self, element: &WindowedValue<dyn Any>) -> Result<(), SomeError> {
        // TODO: assumes global window
        let untyped_value: &dyn Any = &*element.value;
        let (key, value) = self.key_extractor.extract(untyped_value);
        if !self.grouped_values.borrow().contains_key(&key) {
            self.grouped_values
                .borrow_mut()
                .insert(key.clone(), Box::new(vec![]));
        }
        self.grouped_values
            .borrow_mut()
            .get_mut(&key)
            .unwrap()
            .push(value);
        Ok(())
    }
    fn finish(&self) -> Result<(), SomeError> {
        for (key, values) in self.grouped_values.borrow().iter() {
            // TODO: timestamp and pane info are wrong
            for consumer in self.consumers.iter() {
                consumer.process(&WindowedValue::in_global_window(
                    self.key_extractor.recombine(key, values),
                ))?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for GroupByKeyForDirectRunnerOperator {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "GroupByKeyForTestOperator")
    }
}

////////////////// Operator tree construction //////////////////

fn create_operator_fn(
    transform_id: &String,
    bundle_descriptor: &ProcessBundleDescriptor,
    output_consumers: Vec<Rc<dyn Operator>>,
) -> Rc<dyn Operator> {
    let transform_proto = bundle_descriptor.transforms.get(transform_id).unwrap();
    let urn = &transform_proto.spec.as_ref().unwrap().urn;
    if urn == IMPULSE_URN {
        return Rc::new(ImpulseForDirectRunnerOperator {
            consumers: output_consumers,
        });
    } else if urn == FLATTEN_URN {
        return Rc::new(FlattenOperator {
            consumers: output_consumers,
        });
    } else if urn == PAR_DO_URN {
        return Rc::new(DoOperator {
            consumers: output_consumers,
            func: deserialize_fn(
                &String::from_utf8(transform_proto.spec.as_ref().unwrap().payload.clone()).unwrap(),
            )
            .unwrap(),
        });
    } else if urn == GBK_URN {
        return Rc::new(GroupByKeyForDirectRunnerOperator {
            consumers: output_consumers,
            grouped_values: RefCell::new(HashMap::new()),
            // TODO: This should be derived from the Coder, not the payload.
            key_extractor: create_key_extractor(
                &String::from_utf8(transform_proto.spec.as_ref().unwrap().payload.clone()).unwrap(),
            )
            .unwrap(),
        });
    } else {
        panic!("Unknown operator type: {}", urn);
    }
}

pub fn create_bundle_processor<'a>(bundle_descriptor: &ProcessBundleDescriptor) -> BundleProcessor {
    let mut consumers = HashMap::new();
    let mut operators_by_id: HashMap<String, Rc<dyn Operator>> = HashMap::new();
    let mut rev_topo_order: Vec<String> = Vec::new();

    for (transform_id, transform_proto) in &bundle_descriptor.transforms {
        if transform_proto.subtransforms.is_empty() {
            // TODO: Named outputs.
            for (_name, pcoll_id) in &transform_proto.inputs {
                if !consumers.contains_key(pcoll_id) {
                    consumers.insert(pcoll_id, Vec::new());
                }
                consumers.get_mut(pcoll_id).unwrap().push(transform_id);
            }
        }
    }
    info!("{:#?}", consumers);
    println!("{:#?}", consumers);

    //  Issues with mutually recursive closures.
    //    let create_operator = |transform_id: &String| -> Rc<dyn Operator> {
    //        let transform_proto = bundle_descriptor.transforms.get(transform_id).unwrap();
    //        let mut output_consumers = Vec::new();
    //        for (_name, pcoll_id) in &transform_proto.outputs {
    //            for consumer in consumers.get(pcoll_id).unwrap() {
    //                output_consumers.push(get_operator(consumer, &operators_by_id, &rev_topo_order, &create_operator));
    //            }
    //        }
    //        return Rc::new(DoOperator {
    //            consumers: output_consumers,
    //        });
    //    };
    //
    //    let get_operator = |transform_id: &String| -> Rc<dyn Operator> {
    //        if !operators_by_id.contains_key(transform_id) {
    //            operators_by_id.insert(transform_id.to_string(), create_operator(transform_id));
    //            rev_topo_order.push(transform_id.to_string());
    //        }
    //        operators_by_id.get(transform_id).unwrap().clone()
    //    };

    fn get_operator(
        transform_id: &String,
        bundle_descriptor: &ProcessBundleDescriptor,
        operators_by_id: &mut HashMap<String, Rc<dyn Operator>>,
        rev_topo_order: &mut Vec<String>,
        consumers: &HashMap<&String, Vec<&String>>,
    ) -> Rc<dyn Operator> {
        if !operators_by_id.contains_key(transform_id) {
            let transform_proto = bundle_descriptor.transforms.get(transform_id).unwrap();
            let mut output_consumers = Vec::new();
            for (_name, pcoll_id) in &transform_proto.outputs {
                for consumer in consumers.get(pcoll_id).unwrap() {
                    output_consumers.push(get_operator(
                        consumer,
                        bundle_descriptor,
                        operators_by_id,
                        rev_topo_order,
                        consumers,
                    ));
                }
            }
            println!("output_consumers {}, {:?}", transform_id, output_consumers);
            operators_by_id.insert(
                transform_id.to_string(),
                create_operator_fn(transform_id, bundle_descriptor, output_consumers),
            );
            rev_topo_order.push(transform_id.to_string());
        }
        operators_by_id.get(transform_id).unwrap().clone()
    }

    for (transform_id, transform_proto) in &bundle_descriptor.transforms {
        // TODO: I'm trying to use a match, but maybe it's not a good fit here?
        match transform_proto.spec.as_ref().unwrap().urn.as_str() {
            //IMPULSE_URN | RUNNER_SOURCE_URN | RUNNER_SINK_URN
            "beam:transform:impulse:v1" | "beam:runnersource:v1" | "beam:runner:sink:v1" => {
                Some(get_operator(
                    transform_id,
                    bundle_descriptor,
                    &mut operators_by_id,
                    &mut rev_topo_order,
                    &consumers,
                ))
            }
            _ => None,
        };
    }
    println!("rev_topo_order {:#?}", rev_topo_order);

    let mut operators = Vec::new();
    for transform_id in rev_topo_order.iter().rev() {
        operators.push(operators_by_id.get(transform_id).unwrap().clone());
    }

    BundleProcessor {
        operators: operators,
    }
}

pub struct BundleProcessor {
    operators: Vec<Rc<dyn Operator>>,
}

impl BundleProcessor {
    pub fn start(&self) -> Result<(), SomeError> {
        for operator in self.operators.iter().rev() {
            operator.start()?;
        }

        Ok(())
    }

    pub fn finish(&self) -> Result<(), SomeError> {
        for operator in self.operators.iter() {
            operator.finish()?;
        }

        Ok(())
    }
}
