fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pipeline_root = "../../model/pipeline/src/main/proto";
    let pipeline_dir = "../../model/pipeline/src/main/proto/org/apache/beam/model/pipeline/v1";
    tonic_build::configure()
        .build_server(true)
         .include_file("pipeline_api.rs")
        .compile(
            &[
                format!("{}/{}", pipeline_dir, "beam_runner_api.proto"),
                format!("{}/{}", pipeline_dir, "endpoints.proto"),
                format!("{}/{}", pipeline_dir, "external_transforms.proto"),
                format!("{}/{}", pipeline_dir, "metrics.proto"),
                format!("{}/{}", pipeline_dir, "schema.proto"),
                format!("{}/{}", pipeline_dir, "standard_window_fns.proto"),
            ],
            &[pipeline_root],
        )
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    let fn_exec_root = "../../model/fn-execution/src/main/proto";
    let fn_exec_dir = "../../model/fn-execution/src/main/proto/org/apache/beam/model/fn_execution/v1";
    tonic_build::configure()
        .build_server(true)
         .include_file("fn_api.rs")
       .compile(
            &[
                format!("{}/{}", fn_exec_dir, "beam_fn_api.proto"),
                format!("{}/{}", fn_exec_dir, "beam_provision_api.proto"),
            ],
            &[pipeline_root, fn_exec_root],
        )
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    let job_root = "../../model/job-management/src/main/proto";
    let job_dir = "../../model/job-management/src/main/proto/org/apache/beam/model/job_management/v1";
    tonic_build::configure()
        .build_server(true)
        .include_file("job_api.rs")
        .compile(
            &[
                format!("{}/{}", job_dir, "beam_artifact_api.proto"),
                format!("{}/{}", job_dir, "beam_expansion_api.proto"),
                format!("{}/{}", job_dir, "beam_job_api.proto"),
            ],
            &[pipeline_root, fn_exec_root, job_root],
        )
        .unwrap_or_else(|e| panic!("protobuf compile error: {}", e));

    Ok(())
}
