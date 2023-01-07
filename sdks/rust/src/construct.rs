use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;

use std::collections::HashMap;

use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1 as proto;

use crate::transforms::FlatMapTransform;
use crate::worker::operators::serialize_fn;
use crate::worker::operators::to_generic_dofn;

pub struct PipelineHolder {
    pipeline: RefCell<proto::Pipeline>,
    name_prefix: RefCell<Box<Vec<String>>>,
    sibling_transforms: RefCell<Box<Vec<Box<Vec<String>>>>>,
    counter: RefCell<i64>,
}

impl PipelineHolder {
    fn default() -> PipelineHolder {
        let mut pipeline = proto::Pipeline::default();
        pipeline.components = Some(proto::Components::default());
        PipelineHolder {
            pipeline: RefCell::new(pipeline),
            name_prefix: RefCell::new(Box::new(vec!["".to_string()])),
            sibling_transforms: RefCell::new(Box::new(vec![Box::new(vec![])])),
            counter: RefCell::new(0),
        }
    }

    // Actually "applies" a transform, i.e. adds it (and any transforms it
    // calls in its expansion method) to the underlying graph.
    fn apply<'a, PIn, O: Any + 'static>(
        &self,
        name: &String,
        transform: &dyn PTransform<'a, PIn, PCollection<'a, O>>,
        input: &PIn,
        input_pcolls: &[&String],
    ) -> PCollection<'a, O> {
        let transform_id = format!("transform{}", self.counter.borrow());
        *self.counter.borrow_mut() += 1;
        let mut named_inputs: HashMap<String, String> = HashMap::new();
        // TODO: Make it a trait to list this for an arbitrary PIn?
        for input_pcoll in input_pcolls {
            named_inputs.insert(input_pcoll.to_string(), input_pcoll.to_string());
        }
        let unique_name = format!(
            "{}{}",
            self.name_prefix.borrow().last().unwrap(),
            name.to_string()
        );
        let mut transform_proto = proto::PTransform {
            unique_name: unique_name.clone(),
            inputs: named_inputs,
            outputs: HashMap::new(),
            spec: None,
            annotations: HashMap::new(),
            display_data: [].to_vec(),
            environment_id: "rust_environment".to_string(),
            subtransforms: [].to_vec(),
        };
        // Push some stuff onto the stack for use by sub-transforms.
        self.name_prefix
            .borrow_mut()
            .push(format!("{}/", unique_name));
        self.sibling_transforms
            .borrow_mut()
            .last_mut()
            .unwrap()
            .push(transform_id.to_string());
        self.sibling_transforms.borrow_mut().push(Box::new(vec![]));
        // TODO: We want PipelineHolder to outlive any of the PCollections it
        // produces. Perhaps they could hold a Rc<PipelineHolder> instead
        // of a plain reference to keep it alive?
        let self_with_static_lifetime =
            unsafe { std::mem::transmute::<&PipelineHolder, &'static PipelineHolder>(&self) };
        // Actually call expand.
        let out_pcoll =
            transform.expand_internal(input, self_with_static_lifetime, &mut transform_proto);
        // Record subtransforms and pop the stacks.
        for sibling in self.sibling_transforms.borrow().last().unwrap().iter() {
            transform_proto.subtransforms.push(sibling.to_string());
        }
        self.sibling_transforms.borrow_mut().pop();
        self.name_prefix.borrow_mut().pop();
        // Populate any outputs.
        // TODO: Allow multiple outputs.
        transform_proto
            .outputs
            .insert("out".to_string(), out_pcoll.id.to_string());
        // Actually stick the fully-constructed proto into the graph.
        self.pipeline
            .borrow_mut()
            .components
            .as_mut()
            .unwrap()
            .transforms
            .insert(transform_id, transform_proto.clone());
        out_pcoll
    }

    pub fn create_pcollection_internal<O>(&self) -> PCollection<O> {
        let mut pipeline = self.pipeline.borrow_mut();
        let name = format!(
            "pcoll{}",
            pipeline.components.as_ref().unwrap().pcollections.len()
        );
        // TODO: Infer and populate Coders.
        pipeline.components.as_mut().unwrap().pcollections.insert(
            name.clone(),
            proto::PCollection {
                unique_name: name.clone(),
                coder_id: "fake_coder".to_string(),
                windowing_strategy_id: "fake_windowing_strategy".to_string(),
                display_data: vec![],
                is_bounded: 1,
            },
        );
        PCollection::<O> {
            pipeline: self,
            id: name,
            phantom: PhantomData,
        }
    }

    fn to_proto(&self) -> proto::Pipeline {
        let mut proto = self.pipeline.borrow().clone();
        for transform_id in self
            .sibling_transforms
            .borrow()
            .first()
            .unwrap_or(&Box::new(vec![]))
            .iter()
        {
            proto.root_transform_ids.push(transform_id.to_string());
        }
        proto
    }
}

pub trait PTransform<'a, PIn, POut> {
    // This is what a typical transform author would implement,
    // using the public API to apply transformations to `input`
    // and returning the resulting PCollection(s).
    fn expand(&self, input: &PIn) -> POut;

    // This is for implementing primitives or other transforms that need
    // to augment the proto spec directly and/or create new PCollections
    // ex nihilo as outputs.
    fn expand_internal(
        &self,
        input: &PIn,
        _pipeline: &'a PipelineHolder,
        _transform_proto: &mut proto::PTransform,
    ) -> POut {
        self.expand(input)
    }
}

#[derive(Copy, Clone)]
// The root pipeline object, from which everything derives.
// Transformations such as Impulse, Create, and Reads can be applied
// to this.
pub struct Root<'a> {
    pipeline: &'a PipelineHolder,
}

impl<'a> Root<'a> {
    pub fn apply<O: Any + 'static>(
        &self,
        // TODO: Should we be using str instead of &String? (Similar elsewhere.)
        name: &String,
        transform: &dyn PTransform<'a, Root<'a>, PCollection<'a, O>>,
    ) -> PCollection<'a, O> {
        self.pipeline.apply(name, transform, &self, &[])
    }
}

#[derive(Clone)]
// A Beam PCollection...
pub struct PCollection<'a, T: Any + 'static> {
    pipeline: &'a PipelineHolder,
    id: String,
    phantom: PhantomData<T>,
}

impl<'a, T: 'static> PCollection<'a, T> {
    pub fn apply<O: Any + 'static>(
        &self,
        name: &String,
        transform: &dyn PTransform<'a, PCollection<'a, T>, PCollection<'a, O>>,
    ) -> PCollection<O> {
        self.pipeline.apply(name, transform, &self, &[&self.id])
    }

    pub fn flat_map<O: Any, I: IntoIterator<Item = O> + 'static>(
        &self,
        name: &String,
        func: fn(&T) -> I
    ) -> PCollection<O> {
        let payload: String = serialize_fn(to_generic_dofn(func));
        self.apply(name, &FlatMapTransform{payload: payload})
    }
    
    // flat_map(name, func: Box<Fn>) would be a generic fn that calls
    // .apply() on a PPTransform that wraps the func with serialize_fn
    // and to_generic_dofn (currently found in operators.rs) and populates
    // the transform_proto with the right spec as seen in worker_test
    // and below.

    // map(name, func) could then call
    //     flat_map(name, |x: T| -> O { vec![func(x)] }).
}

// This is the main entry point the user should use.
// (Runners will take pipeline_constructor and pass it here.)
pub fn build_pipeline(pipeline_constructor: &dyn Fn(Root) -> ()) -> proto::Pipeline {
    let pipeline_holder = PipelineHolder::default(); //{pipeline: RefCell::new(proto::Pipeline::default())};
    let root = Root {
        pipeline: &pipeline_holder,
    };
    pipeline_constructor(root);
    pipeline_holder.to_proto()
}
