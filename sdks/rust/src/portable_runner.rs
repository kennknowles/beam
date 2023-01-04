extern crate futures;

use beam_api::org::apache::beam::model::pipeline::v1::Pipeline;
use beam_api::org::apache::beam::model::job_management::v1::{PrepareJobRequest}; //, RunJobRequest, GetJobStateRequest};
use beam_api::org::apache::beam::model::job_management::v1::job_service_client::JobServiceClient;

pub mod beam_api {
    tonic::include_proto!("beam_api");
}

pub trait Runner {
    // Apparently traits can't be async...
    fn run(&self, pipeline: Pipeline) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct PortableRunner {
    pub address: String,
}

async fn run_async(_runner: &PortableRunner, pipeline: Pipeline) -> Result<(), Box<dyn std::error::Error>> {
  let mut client = JobServiceClient::connect("http://[::1]:7777").await?;

  let request = tonic::Request::new(PrepareJobRequest {
     // name: "Tonic".into(),
     job_name: "job".to_string(),
     pipeline: Some(pipeline),
     pipeline_options: None,
  });

  let response = client.prepare(request).await?;

  println!("RESPONSE={:?}", response);
  Ok(())
}

impl Runner for PortableRunner {
    fn run(&self, pipeline: Pipeline) -> Result<(), Box<dyn std::error::Error>> {
        futures::executor::block_on(run_async(self, pipeline))
    }
}
