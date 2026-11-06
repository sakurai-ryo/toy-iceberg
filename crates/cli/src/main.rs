use std::process::ExitCode;

use toy_iceberg_core::catalog::{Catalog, S3Config};
use toy_iceberg_datafusion::session;

#[tokio::main]
async fn main() -> ExitCode {
    let sql = std::env::args().skip(1).collect::<Vec<_>>().join(" ");
    if sql.trim().is_empty() {
        eprintln!("usage: toy-iceberg '<SQL>'");
        return ExitCode::from(2);
    }
    match run(&sql).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run(sql: &str) -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog::s3(&S3Config::from_env())?;
    let ctx = session(catalog).await?;
    ctx.sql(sql).await?.show().await?;
    Ok(())
}
