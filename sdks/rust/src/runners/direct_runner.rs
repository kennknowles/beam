use crate::worker::operators::beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::Pipeline;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1 as proto;

use crate::construct::Root;
use crate::construct::build_pipeline;

use crate::worker::operators::create_bundle_processor;


pub mod beam_api {
    tonic::include_proto!("beam_api");
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
            transforms: pipeline.components.as_ref().unwrap().transforms.clone(),

            // Read pipeline PCollections into pbd
            pcollections: pipeline.components.as_ref().unwrap().pcollections.clone(),

            // Read Coders
            coders: pipeline.components.as_ref().unwrap().coders.clone(),

            // Read Envs
            environments: pipeline.components.as_ref().unwrap().environments.clone(),

            windowing_strategies: pipeline.components.as_ref().unwrap().windowing_strategies.clone(),

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
