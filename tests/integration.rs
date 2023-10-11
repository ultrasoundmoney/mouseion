use std::time::Duration;

use anyhow::Result;
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::{MultipleOrderedPairs, RedisConfig},
};
use mouseion::{env::ENV_CONFIG, run::run_all, BlockSubmission, STREAM_NAME};
use tokio::time::sleep;

#[tokio::test]
async fn archive_block_submission() -> Result<()> {
    let redis_config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(redis_config, None, None);
    redis_client.connect();
    redis_client.wait_for_connect().await?;

    let block_submission = {
        let file = std::fs::File::open("tests/fixtures/1697056058008140-0x8b6b5967a9651ead03112cf89826655e837f4b2dbf50c8a418e1ca7923248097.json")?;
        let submission: BlockSubmission = serde_json::from_reader(file)?;
        submission
    };
    let pairs: MultipleOrderedPairs = block_submission.try_into()?;

    redis_client
        .xadd(STREAM_NAME, false, None, "*", pairs)
        .await?;

    run_all().await?;

    sleep(Duration::from_secs(1)).await;

    // TODO: check that what was stored is what we expect

    Ok(())
}
