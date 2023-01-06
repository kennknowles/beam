use std::any::Any;
use std::cell::RefCell;
use std::marker::PhantomData;

//use std::collections::HashMap;

use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1 as proto;
use crate::worker::operators::IMPULSE_URN;

pub struct PipelineHolder {
    pipeline: RefCell<proto::Pipeline>,
}

impl PipelineHolder {
    fn apply<PIn, O: Any + 'static>(
        &self,
        _name: &String,
        _transform: &dyn PTransform<PIn, PCollection<O>>,
        _input: &PIn,
        _input_pcolls: &[&String],
    ) -> PCollection<O> {
        panic!("TODO: Get this to compile.");
        /*
        let mut pipeline = self.pipeline.borrow_mut();
        let transform_id = format!(
            "transform{}",
            pipeline.components.as_ref().unwrap().transforms.len()
        );
        let mut named_inputs : HashMap<String, String> = HashMap::new();
        // TODO: Make it a trait to list this for an arbitrary PIn?
        for input_pcoll in input_pcolls {
            named_inputs.insert(input_pcoll.to_string(), input_pcoll.to_string());
        }
        let mut transform_proto = proto::PTransform {
            unique_name: name.to_string(), // TODO: add nesting
            inputs: named_inputs,
            outputs: HashMap::new(),
            spec: None,
            annotations: HashMap::new(),
            display_data: [].to_vec(),
            environment_id: "rust_environment".to_string(),
            subtransforms: [].to_vec(),
        };
        // TODO: Allow multiple outputs.
        let out_pcoll = transform.expand_internal(input, &self, &mut transform_proto);
        transform_proto.outputs.insert("out".to_string(), out_pcoll.id.to_string());
        // TODO: Populate subtransforms
        pipeline
            .components
            .as_ref()
            .unwrap()
            .transforms
            .insert(transform_id, transform_proto.clone());
        out_pcoll
        */
    }

    fn create_pcollection<O>(&self) -> PCollection<O> {
        let mut pipeline = self.pipeline.borrow_mut();
        let name = format!(
            "pcoll{}",
            pipeline.components.as_ref().unwrap().pcollections.len()
        );
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
}

pub trait PTransform<'a, PIn, POut> {
    fn expand(&self, input: &PIn) -> POut;
    fn expand_internal(
        &self,
        input: &PIn,
        _pipeline: &'a PipelineHolder,
        _transform_proto: &mut proto::PTransform,
    ) -> POut {
        self.expand(input)
    }
}

pub struct Root<'a> {
    pipeline: &'a PipelineHolder,
}

impl<'a> Root<'a> {
    pub fn apply<O: Any + 'static>(
        &self,
        name: &String,
        transform: &dyn PTransform<'a, Root<'a>, PCollection<'a, O>>,
    ) -> PCollection<O> {
        self.pipeline.apply(name, transform, &self, &[])
    }
}

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

    // flat_map(name, func: Box<Fn>) would be a generic fn that calls
    // .apply() on a PPTransform that wraps the func with serialize_fn
    // and to_generic_dofn (currently found in operators.rs) and populates
    // the transform_proto with the right spec as seen in worker_test
    // and below.

    // map(name, func) could then call
    //     flat_map(name, |x: T| -> O { vec![func(x)] }).
}

// Create would be a PTransform<Root, PCollection<T>> that simply implements
// expand to call Impulse + FlatMap.
// Other composites could be created as well.

// GroupByKey should be just like Impulse. It should be a
// PTransform<PCollection<(K, V)>, PCollection<(K, Vec<V>)>>

struct Impulse {}

impl<'x> PTransform<'x, Root<'x>, PCollection<'x, String>> for Impulse {
    fn expand(&self, _input: &Root<'x>) -> PCollection<'x, String> {
        panic!("TODO: Provide default impl when exandInternal implemented.");
    }
    fn expand_internal(
        &self,
        _input: &Root<'x>,
        pipeline: &'x PipelineHolder,
        transform_proto: &mut proto::PTransform,
    ) -> PCollection<'x, String> {
        // Update the spec to say how it's created.
        transform_proto.spec = Some(proto::FunctionSpec {
            urn: IMPULSE_URN.to_string(),
            payload: vec![],
        });
        // Created out of thin air.
        pipeline.create_pcollection()
    }
}
