use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read},
};

use anyhow::{Context, Result};
use async_nats::jetstream;
use flate2::read::GzDecoder;
use tracing::{debug, info};

fn decompress_gz_to_file(input_path: &str, output_path: &str) -> Result<(), std::io::Error> {
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

fn read_file(file_path: &str) -> Result<String, std::io::Error> {
    let mut file = File::open(file_path)?;
    let mut content = String::new();
    file.read_to_string(&mut content)?;
    Ok(content)
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    info!("publishing simulation messages");

    let nats_uri = std::env::var("NATS_URI").unwrap_or_else(|_| "localhost:4222".to_string());
    let nats_client = async_nats::connect(nats_uri)
        .await
        .context("connecting to NATS")?;
    let nats_context = jetstream::new(nats_client);

    let slots = vec![7041566, 7041571, 7041572, 7041573];
    let input_paths = slots
        .iter()
        .map(|slot| format!("example_payloads/{}.ndjson.gz", slot));

    let inputs = slots.iter().zip(input_paths);

    for (slot, input_path) in inputs {
        let decompressed_path = &format!("{}.decompressed", input_path);

        // Check if decompressed file exists
        if !std::path::Path::new(decompressed_path).exists() {
            debug!("decompressing {}", input_path);
            decompress_gz_to_file(&input_path, decompressed_path)?;
        }

        let ndjson = read_file(decompressed_path)?;
        let payloads = ndjson
            .split('\n')
            .filter(|s| !s.trim().is_empty()) // filter out empty strings
            .map(|s| s.to_string())
            .collect::<Vec<_>>();
        let messages_len = payloads.len();

        for payload in payloads.into_iter() {
            nats_context
                .publish(format!("slot.{}", slot), payload.into())
                .await?;
        }
        info!(slot, "published {} messages", messages_len);
    }

    info!("done publishing simulation messages");

    Ok(())
}
