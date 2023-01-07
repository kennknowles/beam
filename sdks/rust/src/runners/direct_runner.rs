use std::collections::HashMap;
use std::option::Option;

use crate::worker::operators::beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::FunctionSpec;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::PTransform;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::Pipeline;

use crate::construct::Root;
use crate::construct::build_pipeline;

use crate::worker::operators::create_bundle_processor;


pub mod beam_api {
    tonic::include_proto!("beam_api");
}

fn make_transform(
    input: Option<String>,
    output: Option<String>,
    spec: FunctionSpec,
) -> PTransform {
    let inputs = match input {
        Some(x) => HashMap::from([("input_name".to_string(), x)]),
        None => HashMap::new(),
    };
    let outputs = match output {
        Some(x) => HashMap::from([("output_name".to_string(), x)]),
        None => HashMap::new(),
    };
    PTransform {
        inputs: inputs,
        outputs: outputs,
        annotations: HashMap::new(),
        display_data: [].to_vec(),
        environment_id: "environment_id".to_string(),
        spec: Some(spec),
        subtransforms: [].to_vec(),
        unique_name: "unique_name".to_string(),
    }
}

pub trait Runner {
    fn run(&self, pipeline: &dyn Fn(Root) -> ()) -> Result<(), String> {
        self.run_proto(build_pipeline(pipeline))
    }

    fn run_proto(&self, _pipeline: Pipeline) -> Result<(), String>;
}

pub struct DirectRunner {
}

impl DirectRunner {
    fn convert(pipeline: Pipeline) -> ProcessBundleDescriptor {
        return ProcessBundleDescriptor {
            // assign id
            id: "id".to_string(),

            // Read pipeline PTransforms into pbd
            transforms: pipeline.components.transforms.clone(),

            // Read pipeline PCollections into pbd
            pcollections: pipeline.components.pcollections.clone(),

            // Read Coders
            coders: pipeline.components.coders.clone(),

            // Read Envs
            environments: pipeline.components.environments.clone(),

            windowing_strategies: pipeline.components.windowing_strategies.clone(),

            state_api_service_descriptor: None,
            timer_api_service_descriptor: None,
        };
    }
}

impl Runner for DirectRunner {
    fn run_proto(&self, pipeline: Pipeline) -> Result<(), String> {
        let descriptor = DirectRunner::convert(pipeline);

        let processor = create_bundle_processor(&descriptor);
        processor.start()?;
        processor.finish()?;
        // translate error into correct return type in mehtod signature
        Ok(())
    }
}
