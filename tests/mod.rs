use std::time::Duration;

use anyhow::Result;
use block_submission_archiver::{env::ENV_CONFIG, run::run_all, BlockSubmission, STREAM_NAME};
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::{MultipleOrderedPairs, RedisConfig},
};
use tokio::time::sleep;

#[tokio::test]
async fn archive_block_submission() -> Result<()> {
    run_all().await?;

    let block_submission = {
        let file = std::fs::File::open("tests/fixtures/0xffe314e3f12d726cf9f4a4babfcbfc836ef53d3144469f886423a833c853e3ef.json.gz.decompressed")?;
        let submission: BlockSubmission = serde_json::from_reader(file)?;
        submission
    };

    let pairs: MultipleOrderedPairs = block_submission.try_into()?;

    let redis_config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let redis_client = RedisClient::new(redis_config, None, None);
    redis_client.connect();
    redis_client.wait_for_connect().await?;
    redis_client
        .xadd(STREAM_NAME, true, None, "*", pairs)
        .await?;

    sleep(Duration::from_secs(1)).await;

    // TODO: check that what was stored is what we expect

    Ok(())
}
