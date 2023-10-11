use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read},
    path::Path,
};

use anyhow::Result;
use flate2::read::GzDecoder;
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::RedisConfig,
};
use mouseion::{env::ENV_CONFIG, log, BlockSubmission, STREAM_NAME};
use tracing::{debug, info};

const EXAMPLE_PATHS: [&str; 1] = [
    "example_block_submissions/1697056058008140-0x8b6b5967a9651ead03112cf89826655e837f4b2dbf50c8a418e1ca7923248097.json.gz",
];

fn decompress_gz_to_file(input_path: &str, output_path: &Path) -> Result<(), std::io::Error> {
    let input_file = File::open(input_path)?;
    let decoder = GzDecoder::new(input_file);
    let mut reader = BufReader::new(decoder);

    let output_file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)?;

    let mut writer = BufWriter::new(output_file);

    std::io::copy(&mut reader, &mut writer)?;

    Ok(())
}

fn read_file(file_path: &Path) -> Result<String, std::io::Error> {
    let mut file = File::open(file_path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

#[tokio::main]
async fn main() -> Result<()> {
    log::init();

    info!("simulating block submissions");

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let client = RedisClient::new(config, None, None);
    client.connect();
    client.wait_for_connect().await?;

    for path in EXAMPLE_PATHS {
        let path_without_ext = &path.replace(".gz", "");
        let decompressed_path = Path::new(path_without_ext);

        if !decompressed_path.exists() {
            debug!("decompressing {}", path);
            decompress_gz_to_file(&path, decompressed_path)?;
        }

        let raw_block_submission = read_file(decompressed_path)?;
        let block_submission: BlockSubmission = serde_json::from_str(&raw_block_submission)?;
        let id = path.split('/').last().unwrap().replace(".json.gz", "");

        client
            .xadd(STREAM_NAME, false, None, "*", block_submission)
            .await?;

        debug!(id, "simulated block submission");
    }

    info!("done simulating block submissions");

    Ok(())
}
