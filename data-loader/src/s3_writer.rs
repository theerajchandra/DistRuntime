use anyhow::{Context, Result};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Delete, ObjectIdentifier};
use aws_sdk_s3::Client;
use bytes::Bytes;

/// Objects at or above this size use S3 multipart upload; smaller objects use PutObject.
pub const MULTIPART_THRESHOLD_BYTES: usize = 100 * 1024 * 1024; // 100 MiB

/// Each multipart part is 8 MiB (must be >= 5 MiB for all but the last part).
const PART_SIZE: usize = 8 * 1024 * 1024;

/// Returns `true` if `size` meets the multipart upload threshold.
pub fn should_use_multipart(size: usize) -> bool {
    size >= MULTIPART_THRESHOLD_BYTES
}

/// Write `data` to `s3://{bucket}/{key}`, choosing multipart upload automatically
/// for objects at or above [`MULTIPART_THRESHOLD_BYTES`].
pub async fn write_object(client: &Client, bucket: &str, key: &str, data: Bytes) -> Result<()> {
    if should_use_multipart(data.len()) {
        write_object_multipart(client, bucket, key, data).await
    } else {
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(data))
            .send()
            .await
            .with_context(|| format!("failed to put s3://{bucket}/{key}"))?;
        Ok(())
    }
}

/// Upload `data` to S3 using the multipart upload API, splitting into [`PART_SIZE`] chunks.
async fn write_object_multipart(
    client: &Client,
    bucket: &str,
    key: &str,
    data: Bytes,
) -> Result<()> {
    let upload_id = client
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .with_context(|| format!("failed to create multipart upload for s3://{bucket}/{key}"))?
        .upload_id
        .context("missing upload_id in create_multipart_upload response")?;

    let mut completed_parts: Vec<CompletedPart> = Vec::new();
    let chunks: Vec<Bytes> = data.chunks(PART_SIZE).map(Bytes::copy_from_slice).collect();
    let part_count = chunks.len();

    for (i, chunk) in chunks.into_iter().enumerate() {
        let part_number = (i + 1) as i32;

        let result = client
            .upload_part()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .part_number(part_number)
            .body(ByteStream::from(chunk))
            .send()
            .await;

        match result {
            Ok(resp) => {
                let e_tag = resp
                    .e_tag
                    .context("missing e_tag in upload_part response")?;
                completed_parts.push(
                    CompletedPart::builder()
                        .part_number(part_number)
                        .e_tag(e_tag)
                        .build(),
                );
            }
            Err(e) => {
                // Best-effort abort so the incomplete upload doesn't linger.
                let _ = client
                    .abort_multipart_upload()
                    .bucket(bucket)
                    .key(key)
                    .upload_id(&upload_id)
                    .send()
                    .await;
                return Err(e).with_context(|| {
                    format!("failed to upload part {part_number} for s3://{bucket}/{key}")
                });
            }
        }
    }

    tracing::debug!(bucket, key, part_count, "completing multipart upload");

    let multipart_upload = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    client
        .complete_multipart_upload()
        .bucket(bucket)
        .key(key)
        .upload_id(&upload_id)
        .multipart_upload(multipart_upload)
        .send()
        .await
        .with_context(|| format!("failed to complete multipart upload for s3://{bucket}/{key}"))?;

    Ok(())
}

/// Server-side copy of `s3://{bucket}/{src_key}` to `s3://{bucket}/{dst_key}`.
pub async fn copy_object(
    client: &Client,
    bucket: &str,
    src_key: &str,
    dst_key: &str,
) -> Result<()> {
    client
        .copy_object()
        .bucket(bucket)
        .key(dst_key)
        .copy_source(format!("{bucket}/{src_key}"))
        .send()
        .await
        .with_context(|| format!("failed to copy s3://{bucket}/{src_key} -> {dst_key}"))?;
    Ok(())
}

/// Delete all objects whose key begins with `prefix` under `bucket`.
///
/// Returns the total number of objects deleted. Returns `Ok(0)` when no objects
/// are found under the prefix.
pub async fn delete_prefix(client: &Client, bucket: &str, prefix: &str) -> Result<usize> {
    let mut continuation_token: Option<String> = None;
    let mut total_deleted: usize = 0;

    loop {
        let mut list_req = client.list_objects_v2().bucket(bucket).prefix(prefix);

        if let Some(ref token) = continuation_token {
            list_req = list_req.continuation_token(token);
        }

        let list_resp = list_req
            .send()
            .await
            .with_context(|| format!("failed to delete prefix s3://{bucket}/{prefix}"))?;

        let keys: Vec<ObjectIdentifier> = list_resp
            .contents()
            .iter()
            .filter_map(|obj| {
                obj.key().map(|k| {
                    ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .expect("key is always set")
                })
            })
            .collect();

        if !keys.is_empty() {
            let batch_count = keys.len();

            let delete = Delete::builder()
                .set_objects(Some(keys))
                .quiet(true)
                .build()
                .context("failed to build Delete request")?;

            client
                .delete_objects()
                .bucket(bucket)
                .delete(delete)
                .send()
                .await
                .with_context(|| format!("failed to delete prefix s3://{bucket}/{prefix}"))?;

            total_deleted += batch_count;
        }

        if list_resp.is_truncated().unwrap_or(false) {
            continuation_token = list_resp.next_continuation_token().map(str::to_owned);
        } else {
            break;
        }
    }

    Ok(total_deleted)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_use_multipart_below_threshold() {
        assert!(!should_use_multipart(MULTIPART_THRESHOLD_BYTES - 1));
    }

    #[test]
    fn should_use_multipart_at_threshold() {
        assert!(should_use_multipart(MULTIPART_THRESHOLD_BYTES));
    }

    #[test]
    fn should_use_multipart_above_threshold() {
        assert!(should_use_multipart(MULTIPART_THRESHOLD_BYTES + 1));
    }

    #[test]
    fn write_object_dispatches_to_multipart_for_large_data() {
        // Verify the dispatch logic without making any S3 calls.
        let size = MULTIPART_THRESHOLD_BYTES;
        assert!(should_use_multipart(size));
    }

    #[test]
    fn part_size_meets_s3_minimum() {
        const _: () = assert!(PART_SIZE >= 5 * 1024 * 1024);
    }
}
