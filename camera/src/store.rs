use std::{path::Path, sync::Arc};

use futures::{StreamExt, stream::FuturesOrdered};

use std::error::Error;

use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FrameMetadata {
    pub name: String,
    pub src: String,
    pub timestamp: i64,
    pub p_hash: String,
    pub p_hash_distance: Option<u64>,
}

pub struct FrameStore {
    pub image_storage_dir: Box<Path>,
}

impl FrameStore {
    pub fn new(image_storage_dir: &Path) -> Arc<FrameStore> {
        Arc::new(FrameStore {
            image_storage_dir: Box::from(image_storage_dir),
        })
    }

    /**
     * Save the frame to disk in the central data directory, along with metadata collected at capture
     * time.
     *
     * This produces files {name}.jpg and {name}.json which are consumed by the API and by later stages
     * in image processing.
     */
    pub async fn write_frame_capture_data(
        &self,
        frame: crate::camera::Frame,
        p_hash_distance: Option<u64>,
    ) -> Result<FrameMetadata, Box<dyn Error>> {
        let filename = crate::api::time_to_file_basename(&frame.timestamp);
        let mut path = self.image_storage_dir.join(&filename);
        path.set_extension("jpg");

        tokio::fs::write(&path, &frame.jpeg).await?;

        let frame_metadata = FrameMetadata {
            name: filename.clone(),
            src: format!("{}.jpg", filename),
            timestamp: frame.timestamp.timestamp_millis(),
            p_hash: frame.p_hash,
            p_hash_distance,
        };
        let frame_metadata_json = to_string_pretty(&frame_metadata)?;

        path.set_extension("json");
        tokio::fs::write(&path, frame_metadata_json).await?;

        Ok(frame_metadata)
    }

    pub async fn list_frames(
        &self,
        last: Option<usize>,
        before: Option<String>,
    ) -> Result<Vec<FrameMetadata>, Box<dyn Error>> {
        let mut ls = tokio::fs::read_dir(self.image_storage_dir.as_ref()).await?;

        let mut entries: Vec<tokio::fs::DirEntry> = Vec::new();
        while let Ok(Some(entry)) = ls.next_entry().await {
            let Ok(name) = entry.file_name().into_string() else {
                // Verifies that file names are valid UTF-8
                continue;
            };

            if !name.ends_with(".jpg") {
                // We only want to list JPEG files
                continue;
            }

            match before {
                None => entries.push(entry),
                Some(ref c) => {
                    if &name < c {
                        entries.push(entry);
                    }
                }
            }
        }
        entries.sort_by_cached_key(|e| e.file_name());
        entries.reverse();

        let max_size: usize = 100;
        let size = match last {
            None => max_size,
            Some(s) => {
                if s < max_size {
                    s
                } else {
                    max_size
                }
            }
        };
        entries.truncate(size);

        let frames: Vec<FrameMetadata> = entries
            .into_iter()
            .map(async |e| -> Result<FrameMetadata, ()> {
                // We know from above that the file name is valid UTF-8, i.e. can be a String
                let jpg_file_name = e.file_name().into_string().unwrap();

                let mut json_file_path = e.path();
                json_file_path.set_extension("json");

                let serialized_metadata = match tokio::fs::read(json_file_path).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!(
                            "Failed to load metadata file for {:?}: {:?}",
                            jpg_file_name, e
                        );
                        return Err(());
                    }
                };
                Ok(match serde_json::from_slice(&serialized_metadata) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!(
                            "Failed to parse metadata file for {:?}: {:?}",
                            jpg_file_name, e
                        );
                        return Err(());
                    }
                })
            })
            // Collect into a FuturesUnordered to run the file reads in parallel
            .collect::<FuturesOrdered<_>>()
            // Discard frames that failed to load
            .filter_map(|r| async move { r.ok() })
            // Join the futures and collect into the result Vec
            .collect()
            .await;
        Ok(frames)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use std::sync::Arc;
    use tempfile::tempdir;

    use crate::camera;
    use crate::store::FrameStore;

    #[tokio::test]
    async fn test_metadata_file_round_trip() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        let stub_frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };

        let metadata = store
            .write_frame_capture_data(stub_frame, None)
            .await
            .unwrap();
        assert_eq!(metadata.name, "2025-10-13_23-20-22-231_+0000");
        assert_eq!(metadata.src, "2025-10-13_23-20-22-231_+0000.jpg");
        assert_eq!(metadata.p_hash, "c41782ed3c9263cd");
        assert_eq!(metadata.p_hash_distance, None);
    }

    /**
     * Write five test frames with different times
     */
    async fn write_test_frames(store: &FrameStore) {
        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };
        store.write_frame_capture_data(frame, None).await.unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397623231).unwrap(),
            jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "cc17c7cd3c9263cd".to_string(),
        };
        store.write_frame_capture_data(frame, None).await.unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397624231).unwrap(),
            jpeg: Arc::from(b"zxcv".as_slice()),
            p_hash: "ac1387ed3c9463cd".to_string(),
        };
        store.write_frame_capture_data(frame, None).await.unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397626231).unwrap(),
            jpeg: Arc::from(b"jkl;".as_slice()),
            p_hash: "541783dc1c92638c".to_string(),
        };
        store.write_frame_capture_data(frame, None).await.unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397625231).unwrap(),
            jpeg: Arc::from(b"uiop".as_slice()),
            p_hash: "4c1787683caa73cc".to_string(),
        };
        store.write_frame_capture_data(frame, None).await.unwrap();
    }

    #[tokio::test]
    async fn test_list_frames() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(Some(5), None).await.unwrap();

        assert_eq!(ls.len(), 5);
        assert_eq!(ls.get(0).unwrap().name, "2025-10-13_23-20-26-231_+0000");
        assert_eq!(ls.get(1).unwrap().name, "2025-10-13_23-20-25-231_+0000");
        assert_eq!(ls.get(2).unwrap().name, "2025-10-13_23-20-24-231_+0000");
        assert_eq!(ls.get(3).unwrap().name, "2025-10-13_23-20-23-231_+0000");
        assert_eq!(ls.get(4).unwrap().name, "2025-10-13_23-20-22-231_+0000");
    }

    #[tokio::test]
    async fn test_list_frames_last_2() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(Some(2), None).await.unwrap();

        assert_eq!(ls.len(), 2);
        // The two most recent ones, per "last: 2"
        assert_eq!(ls.get(0).unwrap().name, "2025-10-13_23-20-26-231_+0000");
        assert_eq!(ls.get(1).unwrap().name, "2025-10-13_23-20-25-231_+0000");
    }

    #[tokio::test]
    async fn test_list_frames_before() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(None, Some("2025-10-13_23-20-24-231_+0000".to_string()))
            .await
            .unwrap();

        assert_eq!(ls.len(), 2);
        assert_eq!(ls.get(0).unwrap().name, "2025-10-13_23-20-23-231_+0000");
        assert_eq!(ls.get(1).unwrap().name, "2025-10-13_23-20-22-231_+0000");
    }

    #[tokio::test]
    async fn test_list_frames_first_before() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(Some(1), Some("2025-10-13_23-20-24-231_+0000".to_string()))
            .await
            .unwrap();

        assert_eq!(ls.len(), 1);
        assert_eq!(ls.get(0).unwrap().name, "2025-10-13_23-20-23-231_+0000");
    }

    #[tokio::test]
    async fn test_list_frames_before_first() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(None, Some("2025-10-13_23-20-22-231_+0000".to_string()))
            .await
            .unwrap();

        assert_eq!(ls.len(), 0);
    }
}
