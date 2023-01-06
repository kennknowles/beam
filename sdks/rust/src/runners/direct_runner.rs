use std::collections::HashMap;
use std::option::Option;

use crate::worker::operators::beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::FunctionSpec;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::PTransform;
use crate::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::Pipeline;

use crate::worker::operators::create_bundle_processor;
use crate::worker::operators::serialize_fn;
use crate::worker::operators::to_generic_dofn;
use crate::worker::operators::FLATTEN_URN;
use crate::worker::operators::IMPULSE_URN;
use crate::worker::operators::PAR_DO_URN;



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
    fn run(&self, _pipeline: Pipeline) -> Result<(), String>;
}

pub struct DirectRunner {
}

impl DirectRunner {
    fn convert(_pipeline: Pipeline) -> ProcessBundleDescriptor {
        // return ProcessBundleDescriptor {
        //     // assign id
        //     id: "id".to_string,

        //     // Read pipeline PTransforms into pbd
        //     transforms: pipeline.components.transforms,

        //     // Read pipeline PCollections into pbd
        //     pcollections: pipeline.components.pcollections,

        //     // Read Coders
        //     coders: pipeline.components.coders,

        //     // Read Envs
        //     environments: pipeline.components.envs

        //     windowing_strategies: HashMap::new(),
        //     environments: HashMap::new(),

        //     state_api_service_descriptor: None,
        //     timer_api_service_descriptor: None,
        // };
        return ProcessBundleDescriptor {
            id: "id".to_string(),
            transforms: HashMap::from([
                (
                    "create1".to_string(),
                    make_transform(
                        None,
                        Some("pc1".to_string()),
                        FunctionSpec {
                            urn: IMPULSE_URN.to_string(),
                            payload: [].to_vec(),
                        },
                    ),
                ),
                (
                    "1to2".to_string(),
                    make_transform(
                        Some("pc1".to_string()),
                        Some("pc2".to_string()),
                        FunctionSpec {
                            urn: PAR_DO_URN.to_string(),
                            //                            payload: serialize_fn(Box::new(|x: &dyn Any| {
                            //                                Box::new(
                            //                                    vec![Box::new(format!("got {:?}", x)) as Box<dyn Any>]
                            //                                        .into_iter(),
                            //                                )
                            //                            }))
                            //                            .into_bytes(),
                            payload: serialize_fn(to_generic_dofn(|x: &String| {
                                [format!("here {:?}", x)]
                            }))
                            .into_bytes(),
                        },
                    ),
                ),
                (
                    "consume2".to_string(),
                    make_transform(
                        Some("pc2".to_string()),
                        None,
                        FunctionSpec {
                            urn: FLATTEN_URN.to_string(),
                            payload: [].to_vec(),
                        },
                    ),
                ),
            ]),
            pcollections: HashMap::new(),
            coders: HashMap::new(),
            windowing_strategies: HashMap::new(),
            environments: HashMap::new(),
            state_api_service_descriptor: None,
            timer_api_service_descriptor: None,
        };
    }
}

impl Runner for DirectRunner {
    fn run(&self, pipeline: Pipeline) -> Result<(), String> {
        let descriptor = DirectRunner::convert(pipeline);

        let processor = create_bundle_processor(&descriptor);
        processor.start()?;
        processor.finish()?;
        // translate error into correct return type in mehtod signature
        Ok(())
    }
}