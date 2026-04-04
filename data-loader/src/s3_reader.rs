use anyhow::{Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use bytes::Bytes;

use crate::stream::ByteStream;

/// S3-compatible object reader that yields chunks via [`ByteStream`].
///
/// Works with any S3-compatible store (AWS S3, MinIO, etc.) by
/// configuring the endpoint URL via the `STORAGE_ENDPOINT` env var.
pub struct S3Reader {
    body: aws_sdk_s3::primitives::ByteStream,
}

impl S3Reader {
    /// Create an S3 client respecting `STORAGE_ENDPOINT` for the endpoint URL.
    pub async fn new_client() -> Result<Client> {
        let mut config_loader = aws_config::from_env();

        if let Ok(endpoint) = std::env::var("STORAGE_ENDPOINT") {
            config_loader = config_loader.endpoint_url(&endpoint);
        }

        let config = config_loader.load().await;
        let s3_config = aws_sdk_s3::config::Builder::from(&config)
            .force_path_style(true)
            .build();

        Ok(Client::from_conf(s3_config))
    }

    /// Open an object for streaming reads.
    pub async fn open(client: &Client, bucket: &str, key: &str) -> Result<Self> {
        let resp = client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .with_context(|| format!("failed to get s3://{bucket}/{key}"))?;

        Ok(Self { body: resp.body })
    }
}

#[async_trait]
impl ByteStream for S3Reader {
    async fn next_chunk(&mut self) -> Result<Option<Bytes>> {
        match self.body.next().await {
            Some(Ok(chunk)) => Ok(Some(chunk)),
            Some(Err(e)) => Err(anyhow::anyhow!("S3 stream error: {e}")),
            None => Ok(None),
        }
    }
}
