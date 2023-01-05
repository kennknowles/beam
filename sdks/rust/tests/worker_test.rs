// cargo test worker_tests -- --nocapture

#[cfg(test)]
mod worker_tests {
    extern crate apache_beam;

    use std::collections::HashMap;
    //use std::any::Any;
    use std::option::Option;

    use apache_beam::worker::operators::beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;
    use apache_beam::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::FunctionSpec;
    use apache_beam::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::PTransform;

    use apache_beam::worker::operators::serialize_fn;
    use apache_beam::worker::operators::to_generic_dofn;
    use apache_beam::worker::operators::FLATTEN_URN;
    use apache_beam::worker::operators::GBK_URN;
    use apache_beam::worker::operators::IMPULSE_URN;
    use apache_beam::worker::operators::PAR_DO_URN;

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

    #[test]
    fn create_bundle_processor() -> Result<(), String> {
        let descriptor = ProcessBundleDescriptor {
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

        let processor = apache_beam::worker::operators::create_bundle_processor(&descriptor);
        processor.start()?;
        processor.finish()?;
        Ok(())
    }

    #[test]
    // TODO: This could be a direct runner, if we translate Pipeline to ProcessBundleDescriptor.
    fn gbk() -> Result<(), String> {
        let descriptor = ProcessBundleDescriptor {
            id: "id".to_string(),
            transforms: HashMap::from([
                (
                    "impulse".to_string(),
                    make_transform(
                        None,
                        Some("impulse.out".to_string()),
                        FunctionSpec {
                            urn: IMPULSE_URN.to_string(),
                            payload: [].to_vec(),
                        },
                    ),
                ),
                (
                    "elements".to_string(),
                    make_transform(
                        Some("impulse.out".to_string()),
                        Some("elements.out".to_string()),
                        FunctionSpec {
                            urn: PAR_DO_URN.to_string(),
                            payload: serialize_fn(to_generic_dofn(
                                |_unused: &String| -> Vec<(String, i32)> {
                                    vec![
                                        ("a".to_string(), 1),
                                        ("a".to_string(), 2),
                                        ("b".to_string(), 3),
                                    ]
                                },
                            ))
                            .into_bytes(),
                        },
                    ),
                ),
                (
                    "gbk".to_string(),
                    make_transform(
                        Some("elements.out".to_string()),
                        Some("gbk.out".to_string()),
                        FunctionSpec {
                            urn: GBK_URN.to_string(),
                            payload: "StringInt32".to_string().into_bytes(),
                        },
                    ),
                ),
                (
                    "print".to_string(),
                    make_transform(
                        Some("gbk.out".to_string()),
                        None,
                        FunctionSpec {
                            urn: PAR_DO_URN.to_string(),
                            payload: serialize_fn(to_generic_dofn(|x: &(String, Vec<i32>)| {
                                println!("grouped: {:?}", x);
                                [format!("{:?}", x)]
                            }))
                            .into_bytes(),
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

        let processor = apache_beam::worker::operators::create_bundle_processor(&descriptor);
        processor.start()?;
        processor.finish()?;
        Ok(())
    }
}
