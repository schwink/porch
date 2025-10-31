use std::{path::Path, sync::Arc};

use futures::{StreamExt, stream::FuturesOrdered};

use log::{debug, error, info};
use std::error::Error;

use tracing::{Level, span};

use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;
use tokio::sync::broadcast;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct FrameMetadata {
    pub name: String,
    pub timestamp: i64,
    pub p_hash: String,
    pub p_hash_distance: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct FrameStoreEntry {
    pub metadata: FrameMetadata,
    pub inference: Option<Vec<crate::inference::InferenceResult>>,
}

#[derive(Debug)]
pub struct FrameStore {
    pub image_storage_dir: Box<Path>,
    frame_tx: broadcast::Sender<FrameStoreEntry>,
    pub frame_rx: broadcast::Receiver<FrameStoreEntry>,
}

impl FrameStore {
    pub fn new(image_storage_dir: &Path) -> Arc<FrameStore> {
        let (frame_tx, frame_rx) = broadcast::channel::<FrameStoreEntry>(32);

        Arc::new(FrameStore {
            image_storage_dir: Box::from(image_storage_dir),
            frame_tx: frame_tx,
            frame_rx: frame_rx,
        })
    }

    /**
     * Save the frame to disk in the central data directory, along with metadata collected at capture
     * time.
     *
     * This produces files {name}.jpg and {name}.json which are consumed by the API and by later stages
     * in image processing.
     */
    #[tracing::instrument(level = Level::TRACE)]
    pub async fn write_frame_capture_data(
        &self,
        frame: &crate::camera::Frame,
        p_hash_distance: Option<u64>,
        inference_results: Option<Vec<crate::inference::InferenceResult>>,
    ) -> Result<FrameStoreEntry, Box<dyn Error>> {
        let filename = crate::api::time_to_file_basename(&frame.timestamp);
        let mut path = self.image_storage_dir.join(&filename);

        let frame_metadata = {
            let span = span!(Level::TRACE, "write_json");
            let _enter = span.enter();

            let frame_metadata = FrameMetadata {
                name: filename.clone(),
                timestamp: frame.timestamp.timestamp_millis(),
                p_hash: frame.p_hash.clone(),
                p_hash_distance,
            };
            let frame_metadata_json = to_string_pretty(&frame_metadata)?;

            path.set_extension("json");
            tokio::fs::write(&path, frame_metadata_json).await?;

            frame_metadata
        };

        {
            let span = span!(Level::TRACE, "write_jpeg");
            let _enter = span.enter();

            path.set_extension("jpg");
            tokio::fs::write(&path, &frame.jpeg).await?;
        }

        {
            let span = span!(Level::TRACE, "write_224_jpeg");
            let _enter = span.enter();

            let mut path = path.clone();
            path.set_extension("224.jpg");
            tokio::fs::write(&path, &frame.inference_jpeg).await?;
        }

        if let Some(ref inference) = inference_results {
            let span = span!(Level::TRACE, "write_inference_json");
            let _enter = span.enter();

            self.write_inference(&filename, inference).await?;
        }

        let entry = FrameStoreEntry {
            metadata: frame_metadata,
            inference: inference_results,
        };

        if let Ok(n) = self.frame_tx.send(entry.clone()) {
            info!("Broadcast new frame to {} subscribers", n);
        }

        Ok(entry)
    }

    #[tracing::instrument(level = Level::TRACE)]
    pub async fn write_inference(
        &self,
        frame_name: &str,
        inference_results: &Vec<crate::inference::InferenceResult>,
    ) -> Result<(), Box<dyn Error>> {
        let mut path = self.image_storage_dir.join(&frame_name);
        path.set_extension("inference.json");

        let json = to_string_pretty(&inference_results)?;

        tokio::fs::write(&path, json).await?;

        Ok(())
    }

    pub async fn list_frames(
        &self,
        last: Option<usize>,
        before: Option<String>,
        first: Option<usize>,
        after: Option<String>,
    ) -> Result<Vec<FrameStoreEntry>, Box<dyn Error>> {
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
            if name.ends_with(".224.jpg") {
                // We only want the full-size JPEGs, not the inference previews
                continue;
            }
            // Slice off the file extension
            let cursor = &name.as_str()[0..(name.len() - 4)];

            let pass_before = if let Some(ref b) = before {
                cursor < b.as_str()
            } else {
                true
            };
            let pass_after = if let Some(ref a) = after {
                cursor > a.as_str()
            } else {
                true
            };
            if pass_before && pass_after {
                entries.push(entry);
            }
        }
        entries.sort_by_cached_key(|e| e.file_name());

        // Typically only first or last will be specified
        if let Some(f) = first {
            entries.truncate(f);
        }
        entries.reverse();
        if let Some(l) = last {
            entries.truncate(l);
        }

        // Maximum page size
        entries.truncate(100);

        let frames: Vec<FrameStoreEntry> = entries
            .into_iter()
            .map(async |e| -> Result<FrameStoreEntry, ()> {
                // We know from above that the file name is valid UTF-8, i.e. can be a String
                let jpg_file_name = e.file_name().into_string().unwrap();

                let mut file_path = e.path();
                file_path.set_extension("json");

                let serialized_metadata = match tokio::fs::read(&file_path).await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            "Failed to load metadata file for {:?}: {:?}",
                            jpg_file_name, e
                        );
                        return Err(());
                    }
                };
                let metadata = match serde_json::from_slice(&serialized_metadata) {
                    Ok(m) => m,
                    Err(e) => {
                        error!(
                            "Failed to parse metadata file for {:?}: {:?}",
                            jpg_file_name, e
                        );
                        return Err(());
                    }
                };

                let inference: Option<Vec<crate::inference::InferenceResult>> = {
                    let mut file_path = file_path.clone();
                    file_path.set_extension("inference.json");

                    match tokio::fs::read(&file_path).await {
                        Err(_) => {
                            debug!("No inference file for {:?}", jpg_file_name);
                            None
                        }
                        Ok(serialized_inference) => {
                            match serde_json::from_slice(&serialized_inference) {
                                Ok(i) => Some(i),
                                Err(e) => {
                                    error!(
                                        "Failed to parse inference file for {:?}: {:?}",
                                        jpg_file_name, e
                                    );
                                    None
                                }
                            }
                        }
                    }
                };

                Ok(FrameStoreEntry {
                    metadata,
                    inference,
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

    pub async fn delete(&self, name: &str) -> Result<(), Box<dyn Error>> {
        let mut path = self.image_storage_dir.join(name);
        path.set_extension("jpg");
        let remove_jpg = tokio::fs::remove_file(&path).await;
        path.set_extension("json");
        let remove_json = tokio::fs::remove_file(&path).await;
        path.clone().set_extension("224.jpg");
        let _remove_224_jpg = tokio::fs::remove_file(&path).await;
        path.clone().set_extension("inference.json");
        let _remove_inference_json = tokio::fs::remove_file(&path).await;

        remove_jpg?;
        remove_json?;
        Ok(())
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

        let store = FrameStore::new(dir.as_ref());

        let stub_frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };

        let entry = store
            .write_frame_capture_data(&stub_frame, None, Some(vec![]))
            .await
            .unwrap();
        assert_eq!(entry.metadata.name, "2025-10-13_23-20-22-231_+0000");
        assert_eq!(entry.metadata.p_hash, "c41782ed3c9263cd");
        assert_eq!(entry.metadata.p_hash_distance, None);
    }

    #[test]
    fn test_deserialize_extra_fields() {
        let json = r#"{
            "name": "2025-10-15_16-59-50-912_+0000",
            "src": "2025-10-15_16-59-50-912_+0000.jpg",
            "timestamp": 1760547590912,
            "p_hash": "751b86ec3cb379cc",
            "p_hash_distance": 10
        }"#;

        let actual: Result<crate::store::FrameMetadata, serde_json::Error> =
            serde_json::from_slice(json.as_bytes());
        assert_eq!(actual.is_ok(), true);
    }

    /**
     * Write five test frames with different times
     */
    async fn write_test_frames(store: &FrameStore) {
        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, Some(vec![]))
            .await
            .unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397623231).unwrap(),
            jpeg: Arc::from(b"qwer".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "cc17c7cd3c9263cd".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, Some(vec![]))
            .await
            .unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397624231).unwrap(),
            jpeg: Arc::from(b"zxcv".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "ac1387ed3c9463cd".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, Some(vec![]))
            .await
            .unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397626231).unwrap(),
            jpeg: Arc::from(b"jkl;".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "541783dc1c92638c".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, None)
            .await
            .unwrap();

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397625231).unwrap(),
            jpeg: Arc::from(b"uiop".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "4c1787683caa73cc".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_list_frames() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 5);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-26-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
        assert_eq!(
            ls.get(2).unwrap().metadata.name,
            "2025-10-13_23-20-24-231_+0000"
        );
        assert_eq!(
            ls.get(3).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
        assert_eq!(
            ls.get(4).unwrap().metadata.name,
            "2025-10-13_23-20-22-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_last_2() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(Some(2), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 2);
        // The two most recent ones, per "last: 2"
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-26-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_before() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                Some("2025-10-13_23-20-24-231_+0000".to_string()),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 2);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-22-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_first_before() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                Some(1),
                Some("2025-10-13_23-20-24-231_+0000".to_string()),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 1);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_before_first() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                Some("2025-10-13_23-20-22-231_+0000".to_string()),
                None,
                None,
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 0);
    }

    #[tokio::test]
    async fn test_list_frames_first_2() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(None, None, Some(2), None).await.unwrap();

        assert_eq!(ls.len(), 2);
        // The two most oldest ones, per "first: 2"
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-22-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_after() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                None,
                None,
                Some("2025-10-13_23-20-24-231_+0000".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 2);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-26-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_last_after() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                None,
                Some(1),
                Some("2025-10-13_23-20-24-231_+0000".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 1);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_after_last() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                None,
                None,
                Some("2025-10-13_23-20-26-231_+0000".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 0);
    }

    #[tokio::test]
    async fn test_list_frames_before_after() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                Some("2025-10-13_23-20-26-231_+0000".to_string()),
                None,
                Some("2025-10-13_23-20-23-231_+0000".to_string()),
            )
            .await
            .unwrap();

        assert_eq!(ls.len(), 2);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-24-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_list_frames_before_after_not_overlapping() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store
            .list_frames(
                None,
                Some("2025-10-13_23-20-23-231_+0000".to_string()),
                None,
                Some("2025-10-13_23-20-26-231_+0000".to_string()),
            )
            .await
            .unwrap();

        // after > before, so nothing will match both
        assert_eq!(ls.len(), 0);
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        write_test_frames(&store).await;

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();
        assert_eq!(ls.len(), 5);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-26-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
        assert_eq!(
            ls.get(2).unwrap().metadata.name,
            "2025-10-13_23-20-24-231_+0000"
        );
        assert_eq!(
            ls.get(3).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
        assert_eq!(
            ls.get(4).unwrap().metadata.name,
            "2025-10-13_23-20-22-231_+0000"
        );

        store.delete("2025-10-13_23-20-24-231_+0000").await.unwrap();

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();
        assert_eq!(ls.len(), 4);
        assert_eq!(
            ls.get(0).unwrap().metadata.name,
            "2025-10-13_23-20-26-231_+0000"
        );
        assert_eq!(
            ls.get(1).unwrap().metadata.name,
            "2025-10-13_23-20-25-231_+0000"
        );
        assert_eq!(
            ls.get(2).unwrap().metadata.name,
            "2025-10-13_23-20-23-231_+0000"
        );
        assert_eq!(
            ls.get(3).unwrap().metadata.name,
            "2025-10-13_23-20-22-231_+0000"
        );
    }

    #[tokio::test]
    async fn test_stores_inference_none() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, None)
            .await
            .unwrap();

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 1);
        let actual_frame = ls.get(0).unwrap();
        assert_eq!(actual_frame.metadata.name, "2025-10-13_23-20-22-231_+0000");
        assert_eq!(actual_frame.inference.is_none(), true);
    }

    #[tokio::test]
    async fn test_stores_inference_empty() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };
        store
            .write_frame_capture_data(&frame, None, Some(vec![]))
            .await
            .unwrap();

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 1);
        let actual_frame = ls.get(0).unwrap();
        assert_eq!(actual_frame.metadata.name, "2025-10-13_23-20-22-231_+0000");
        assert_eq!(actual_frame.inference.is_some(), true);
        let actual_inference = actual_frame.inference.as_ref().unwrap();
        assert_eq!(actual_inference.len(), 0);
    }

    #[tokio::test]
    async fn test_stores_inference_result_empty_outputs() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };

        let inference = vec![crate::inference::InferenceResult {
            model: "test-model".to_string(),
            outputs: vec![],
        }];
        store
            .write_frame_capture_data(&frame, None, Some(inference))
            .await
            .unwrap();

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 1);
        let actual_frame = ls.get(0).unwrap();
        assert_eq!(actual_frame.metadata.name, "2025-10-13_23-20-22-231_+0000");
        assert_eq!(actual_frame.inference.is_some(), true);
        let actual_inference = actual_frame.inference.as_ref().unwrap();
        assert_eq!(actual_inference.len(), 1);
        let actual_inference_result = actual_inference.get(0).unwrap();
        assert_eq!(actual_inference_result.model, "test-model");
        assert_eq!(actual_inference_result.outputs.len(), 0);
    }

    #[tokio::test]
    async fn test_stores_inference_result() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = FrameStore::new(dir.path());

        let frame = camera::Frame {
            timestamp: DateTime::from_timestamp_millis(1760397622231).unwrap(),
            jpeg: Arc::from(b"asdf".as_slice()),
            inference_tensor: Arc::from([0.]),
            inference_jpeg: Arc::from(b"qwer".as_slice()),
            p_hash: "c41782ed3c9263cd".to_string(),
        };

        let inference = vec![crate::inference::InferenceResult {
            model: "test-model".to_string(),
            outputs: vec![crate::inference::InferenceOutput {
                key: "output_key_1".to_string(),
                values: vec![0.95, 0.05],
                values_softmax: vec![0.95, 0.05],
            }],
        }];
        store
            .write_frame_capture_data(&frame, None, Some(inference))
            .await
            .unwrap();

        let ls = store.list_frames(Some(5), None, None, None).await.unwrap();

        assert_eq!(ls.len(), 1);
        let actual_frame = ls.get(0).unwrap();
        assert_eq!(actual_frame.metadata.name, "2025-10-13_23-20-22-231_+0000");

        assert_eq!(actual_frame.inference.is_some(), true);
        let actual_inference = actual_frame.inference.as_ref().unwrap();

        assert_eq!(actual_inference.len(), 1);
        let actual_inference_result = actual_inference.get(0).unwrap();
        assert_eq!(actual_inference_result.model, "test-model");

        assert_eq!(actual_inference_result.outputs.len(), 1);
        let actual_inference_output = actual_inference_result.outputs.get(0).unwrap();
        assert_eq!(actual_inference_output.key, "output_key_1");
        assert_eq!(actual_inference_output.values.len(), 2);
        assert_eq!(actual_inference_output.values.get(0).unwrap(), &0.95);
        assert_eq!(actual_inference_output.values.get(1).unwrap(), &0.05);
    }
}
