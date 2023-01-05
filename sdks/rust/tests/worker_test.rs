// cargo test worker_tests -- --nocapture

#[cfg(test)]
mod worker_tests {
    extern crate apache_beam;

    use std::collections::HashMap;
    use std::option::Option;

    use apache_beam::worker::operators::beam_api::org::apache::beam::model::fn_execution::v1::ProcessBundleDescriptor;
    use apache_beam::worker::operators::beam_api::org::apache::beam::model::pipeline::v1::PTransform;

    fn make_transform(input: Option<String>, output: Option<String>) -> PTransform {
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
            spec: None,
            subtransforms: [].to_vec(),
            unique_name: "unique_name".to_string(),
        }
    }

    #[test]
    fn parse() {
        let descriptor = ProcessBundleDescriptor {
            id: "id".to_string(),
            transforms: HashMap::from([
                (
                    "create1".to_string(),
                    make_transform(None, Some("pc1".to_string())),
                ),
                (
                    "1to2".to_string(),
                    make_transform(Some("pc1".to_string()), Some("pc2".to_string())),
                ),
                (
                    "consume2".to_string(),
                    make_transform(Some("pc2".to_string()), None),
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
        processor.start();
        processor.finish();
    }
}
