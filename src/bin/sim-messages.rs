use std::{
    fs::{File, OpenOptions},
    io::{BufReader, BufWriter, Read},
};

use anyhow::Result;
use block_submission_archiver::{env::ENV_CONFIG, log, ArchiveEntry, STREAM_NAME};
use flate2::read::GzDecoder;
use fred::{
    prelude::{ClientLike, RedisClient, StreamsInterface},
    types::RedisConfig,
};
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

const STATE_ROOTS: [&str; 55] = [
    "0x02fdf00bcdbc6f0d5e81ef481b86874ad3b92511c0f2a03745f9cd8d6bf35787",
    "0x03d3e1c595fcf840b5e3f342922068d724e34eb430a6678f8fff031ab4dd5d33",
    "0x05014e5e38db33abba9b336fe35bfb6db924cf7aa669afe32c4624008786b223",
    "0x06b6ab55dd6e0787fc5a0c05c7aef07bdc33fdb9872fc6c9f6daec920d7f2999",
    "0x113f21242b29e7886eb5a76e461a71f2ff3de1ef1ef21d3268a2669fae9f0710",
    "0x155a157df6f6ff052322a34479fac7c83529edb4d2247a11636576fe5e0da738",
    "0x17909061a03e61312636b886da3b700d6c455be70fab62cc081fc8f03f1407a5",
    "0x1a070622f21b21f13668793957321fd294c0741845942608a883eeb6f032fc7f",
    "0x1b50bb3341355c56155e08f171f845ee9eb26e957c724e8df73caf20885df39d",
    "0x1c6e6d9d1732ea1032c0b5944701fd6b66fbb348cf239654dd3e0c414d7386ec",
    "0x255be1a573ddc9576292794be4011c6ab68447664213b3c3bb55824c3b9e722c",
    "0x28a328d98285298d1619b20588e57629ca1675418b517946c6c63d6ed02f93ff",
    "0x2b7a5aad085fdb8148ba5a039c148e5efae73b6571f2555ab96bf27450f5d42a",
    "0x2e2c7add6305eb526379d7bd1297a4e0d29f896a9cd271d4c707b51b053b8a5c",
    "0x4246509fe98bfcff310b74643395f9df1dcaec2ae4f3114894d188a736e19f15",
    "0x45e255d4bf0e74859d5d2e4536e482576c7b1362515ea70187a54bfa8e5be652",
    "0x4a864e08d5bcc9de4aa409b5cc3edb50f3a8cbe3b8a50f2b75c9a07aaca753b6",
    "0x4d4751bc85c538d7c21d24b7e2c54f11bc126cd4a3513a4f59c536387b23b32f",
    "0x4e6a938b4df91162db62f92f5d751176898616cb5263b66dd16e4befd4786fcf",
    "0x5742687bf9f1eab98537dade8e55e528449655537ca4da3a58fb101ce22efba2",
    "0x57a1f1724c850896ef805a5e81db06a1142e4549aea3a45a71bed20d47b1ab31",
    "0x6762230e1bee18fdd718833bba32d435fb893bd56e709ee5aeb7c93512fd62e0",
    "0x67d3acba38a2a10ea8fe06bad2594a58bd2eb7db2eef3ace2ef0f6211fad3efe",
    "0x68b4be36a6dd43b50ee97c8b59964638ebed63727c453ef2f4d234c822c9dc57",
    "0x69b1009c20690b779b110b6683457b32195a0a41a8ac37520cca7d081aa3744c",
    "0x6b320fff4bac1fcd2251057042c0c002d609245c15d2f8fc62633efc522b82df",
    "0x7f72851d58c47e13e5f95e96213c244de098c1ebb2c295db9229934e442013c6",
    "0x8088db6f5114d9782f24e7cf74ffc60f29874549f13150ab89ab74c2673d7897",
    "0x81cb2e27e2e3a0ac66b33d276617fd21da7d891f8b9bb5a9ec15678f10db7085",
    "0x833268c931ec09796b70f3b931cec71b39e17f3f41c29f65804cb474b2aa9efb",
    "0x84a1ef5f04b91b71bdf55019b8d04d16c1b0e45571cef664c718e6602d23b8a9",
    "0x89118b3b28f38abdeec268b87fc47b34a43c97fe6cc6331f507a6a29954bfab9",
    "0x8d16845248e1f1f541428aa163610e6336caba365bb60d29f5a539016c799e69",
    "0x92fac952297f2769d7d6d77f75f4c4cf33f786164e1a7672169d4a9eaacffd6e",
    "0x94f3714939395e5d9480604c0c24a5a95a1e04ff6fd838a5fe6eb678b37caf0a",
    "0x9d0e36a1fd4ba855cbcc2b17d77b70bcb4f41b22a98cd7540a2c88ac38f22da4",
    "0x9d417147d8620ee9c2a39a2e43369a3fd8793d408668bd9afba661da94908b82",
    "0x9d89f204744a3486a5e2f28ef454d3d48535f3d48daefbbffa347854fc462f4d",
    "0x9eecc9c58fb6c404e3a5702af80069b4cf8e4db2e7604e74d998d2cafd173d29",
    "0xa38730c81052a85442e2d7c5f00a52fcaf99a8e3e1a250f619c4b59ea19b5d07",
    "0xa5fb77fbed40fc4226b19afbf4412360ff0b7db27f7a3f7e5b307a954bd60998",
    "0xa6f63091b72a3b3a2364b5d372a821f3e8f65e93310690eef2fcd4a8b66a831b",
    "0xa7d88531701869558c3967e5803a93a2f8d3905f7c5dc615fd991e6e1bb33b88",
    "0xa8bcfc8e772aa481594660a72b04e0fa3cc014247d237b1e409190870bf9ef9d",
    "0xab4ca52b5fdecbfdb9836b30dcfc9c7a3c6da79f6464b3cadff86e14b92b44f4",
    "0xb147ffe7f300bf89ed62bfa982d19819f1af60a777c1424ef620a369a6450015",
    "0xb1b44bc8901a55bf28c4c862ecb71627dae3bc1ef7cef590e2e9678fcda3292b",
    "0xb4447f955d4ba4035183ac5f12368ab58160496841a096ebfeaaa027669cedb4",
    "0xb61c0df7cd9439322df6c4d8356728bb26c950d14aa324a283687dc65fe5d1e6",
    "0xbc5fe22fb08969c4e1cc6a857dff37a0aa7d27601aea4f07782fdd7d1c8b3f90",
    "0xbda4eb66e23686c4b2982f589529a8df3c2265000ebc12e2d1aec6385960d9ea",
    "0xbdeca84e24df017659590d2fc98ebbe9930616bfd56869a68a94b577c30d6583",
    "0xc084307e531c5178f3a3fdc10d03786728df867f25014d961e4491ab73bed690",
    "0xc4a818397eff6cbe74d53c695bd4ac0974fe3d28842c7368b88bfcc6a8205974",
    "0xca18d2cd142dd339ebca9d1abc4891c078119f389fd0577a7df7d4f4b2bfd87c",
];

#[tokio::main]
async fn main() -> Result<()> {
    log::init_with_env();

    info!("publishing simulation messages");

    let config = RedisConfig::from_url(&ENV_CONFIG.redis_uri)?;
    let client = RedisClient::new(config, None, None);
    client.connect();

    let input_paths = STATE_ROOTS
        .iter()
        .map(|state_root| format!("example_block_submissions/{}.json.gz", state_root));

    let inputs = STATE_ROOTS.iter().zip(input_paths);

    for (state_root, input_path) in inputs {
        let decompressed_path = &format!("{}.decompressed", input_path);

        // Check if decompressed file exists
        if !std::path::Path::new(decompressed_path).exists() {
            debug!("decompressing {}", input_path);
            decompress_gz_to_file(&input_path, decompressed_path)?;
        }

        let raw_block_submission = read_file(decompressed_path)?;
        let archive_entry: ArchiveEntry = serde_json::from_str(&raw_block_submission)?;

        client
            .xadd(STREAM_NAME, false, None, "*", archive_entry)
            .await?;

        debug!(%state_root, "published simulation message");
    }

    info!("done publishing simulation messages");

    Ok(())
}
