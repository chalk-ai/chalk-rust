use std::path::PathBuf;

fn main() {
    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

    let proto_root = repo_root.join("protos");
    let out_dir = repo_root.join("src").join("gen");

    std::fs::create_dir_all(&out_dir).expect("failed to create src/gen/");

    let protos = &[
        "chalk/engine/v1/query_server.proto",
        "chalk/common/v1/online_query.proto",
        "chalk/common/v1/upload_features.proto",
        "chalk/common/v1/chalk_error.proto",
        "chalk/expression/v1/expression.proto",
        "chalk/graph/v1/graph.proto",
        "chalk/graph/v1/sources.proto",
        "chalk/graph/v2/sources.proto",
        "chalk/arrow/v1/arrow.proto",
        "chalk/dataframe/v1/dataframe.proto",
        "chalk/aggregate/v1/service.proto",
        "chalk/aggregate/v1/backfill.proto",
        "chalk/aggregate/v1/timeseries.proto",
        "chalk/auth/v1/permissions.proto",
        "chalk/auth/v1/audit.proto",
        "chalk/utils/v1/encoding.proto",
        "chalk/lsp/v1/lsp.proto",
    ];

    let proto_paths: Vec<PathBuf> = protos.iter().map(|p| proto_root.join(p)).collect();

    tonic_build::configure()
        .build_server(false)
        .out_dir(&out_dir)
        .compile_protos(&proto_paths, &[&proto_root])
        .expect("failed to compile protos");

    println!("Generated proto files in {}", out_dir.display());
}
