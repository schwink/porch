use std::{error::Error, path::Path};

use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

#[derive(Debug, Deserialize, Serialize)]
pub struct FrameMetadata {
    pub name: String,
    pub timestamp: i64,
    pub p_hash: String,
    pub p_hash_distance: Option<u64>,
}

/**
 * Save the frame to disk in the central data directory, along with metadata collected at capture
 * time.
 *
 * This produces files {name}.jpg and {name}.json which are consumed by the API and by later stages
 * in image processing.
 */
pub async fn write_frame_capture_data(
    image_storage_dir: &Path,
    frame: crate::camera::Frame,
    p_hash_distance: Option<u64>,
) -> Result<FrameMetadata, Box<dyn Error>> {
    let filename = crate::api::time_to_file_basename(&frame.timestamp);
    let mut path = image_storage_dir.join(&filename);
    path.set_extension("jpg");

    tokio::fs::write(&path, &frame.jpeg).await?;

    let frame_metadata = FrameMetadata {
        name: filename,
        timestamp: frame.timestamp.timestamp_millis(),
        p_hash: frame.p_hash,
        p_hash_distance,
    };
    let frame_metadata_json = to_string_pretty(&frame_metadata)?;

    path.set_extension("json");
    tokio::fs::write(&path, frame_metadata_json).await?;

    Ok(frame_metadata)
}
