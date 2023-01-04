extern crate futures;

use beam_api::org::apache::beam::model::job_management::v1::job_service_client::JobServiceClient;
use beam_api::org::apache::beam::model::job_management::v1::{
    job_state, GetJobStateRequest, PrepareJobRequest, RunJobRequest,
};
use beam_api::org::apache::beam::model::pipeline::v1::Pipeline;

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

// TODO: Does pretty much everyone have to do this?
#[derive(Debug)]
struct MyError {
    msg: String,
}

impl std::fmt::Display for MyError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.msg)
    }
}

impl std::error::Error for MyError {
    fn description(&self) -> &str {
        &self.msg
    }
}

async fn run_async(
    runner: &PortableRunner,
    pipeline: Pipeline,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = JobServiceClient::connect(runner.address.to_string()).await?;

    let prepare_request = tonic::Request::new(PrepareJobRequest {
        job_name: "job".to_string(),
        pipeline: Some(pipeline),
        pipeline_options: None,
    });

    let prepare_response = client.prepare(prepare_request).await?;
    let preparation_id = prepare_response.into_inner().preparation_id;

    let run_request = tonic::Request::new(RunJobRequest {
        preparation_id: preparation_id,
        retrieval_token: "unused".to_string(),
    });

    let run_response = client.run(run_request).await?;
    let job_id = run_response.into_inner().job_id;

    let mut wait_time = 10;
    loop {
        let status_response = client
            .get_state(tonic::Request::new(GetJobStateRequest {
                job_id: job_id.to_string(),
            }))
            .await?;
        let status = status_response.into_inner().state;
        if status == job_state::Enum::Done.into() {
            return Ok(());
        } else if status == job_state::Enum::Failed.into()
            || status == job_state::Enum::Cancelled.into()
        {
            // TODO: Figure out how to construct Errors.
            return Err(Box::new(MyError {
                msg: format!("Job failed with status {}", status),
            }));
        }

        if wait_time < 1000 {
            wait_time = wait_time * 6 / 5;
        }
        std::thread::sleep(std::time::Duration::from_millis(wait_time));
    }
}

impl Runner for PortableRunner {
    fn run(&self, pipeline: Pipeline) -> Result<(), Box<dyn std::error::Error>> {
        futures::executor::block_on(run_async(self, pipeline))
    }
}
