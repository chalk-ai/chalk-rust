//! Querying features via gRPC.
//!
//! ```sh
//! export CHALK_CLIENT_ID="..."
//! export CHALK_CLIENT_SECRET="..."
//! export CHALK_ACTIVE_ENVIRONMENT="..."
//! cargo run --example grpc_query
//! ```

use chalk_rs::gen::chalk::common::v1::{OnlineQueryRequest, OutputExpr};
use chalk_rs::ChalkGrpcClient;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> chalk_rs::error::Result<()> {
    let client = ChalkGrpcClient::new().build().await?;
    println!("Connected to environment: {}", client.environment_id());

    let request = OnlineQueryRequest {
        inputs: HashMap::from([(
            "user.id".to_string(),
            prost_types::Value {
                kind: Some(prost_types::value::Kind::NumberValue(42.0)),
            },
        )]),
        outputs: vec![
            OutputExpr {
                expr: Some(chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                    "user.name".to_string(),
                )),
            },
            OutputExpr {
                expr: Some(chalk_rs::gen::chalk::common::v1::output_expr::Expr::FeatureFqn(
                    "user.age".to_string(),
                )),
            },
        ],
        ..Default::default()
    };

    let response = client.query(request).await?;

    if let Some(data) = &response.data {
        for result in &data.results {
            println!("{}: {:?}", result.field, result.value);
        }
    }
    for err in &response.errors {
        eprintln!("error: {}", err.message);
    }

    Ok(())
}
