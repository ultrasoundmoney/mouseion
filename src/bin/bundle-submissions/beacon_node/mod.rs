pub struct BeaconNode {
    client: reqwest::Client,
    server_url: String,
}

impl BeaconNode {
    pub fn new(server_url: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            server_url: server_url.to_owned(),
        }
    }

    pub async fn current_slot(&self) -> anyhow::Result<i32> {
        let url = format!("{}/eth/v1/beacon/headers/head", self.server_url);

        let response = self
            .client
            .get(url)
            .send()
            .await
            .expect("failed to send request to beacon node");

        let response = response
            .json::<serde_json::Value>()
            .await
            .expect("failed to parse response from beacon node");

        let slot = response["data"]["header"]["message"]["slot"]
            .as_str()
            .expect("expected slot in header response")
            .parse()
            .expect("failed to parse slot");

        Ok(slot)
    }
}
