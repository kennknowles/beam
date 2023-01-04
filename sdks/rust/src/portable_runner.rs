pub mod pipeline_api {
    tonic::include_proto!("pipeline_api");
}

pub mod job_api {
    tonic::include_proto!("job_api");
}

pub trait Runner {
    fn run(&self, pipeline: pipeline_api::org::apache::beam::model::pipeline::v1::Pipeline) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct PortableRunner {
    pub address: String,
}

impl Runner for PortableRunner {
    fn run(&self, _pipeline: pipeline_api::org::apache::beam::model::pipeline::v1::Pipeline) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
