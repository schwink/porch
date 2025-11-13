use std::{
    error::Error,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::io::AsyncReadExt;
use tokio::sync::RwLock;

use serde::{Deserialize, Serialize};

/// The set of all possible labels that can be applied to a frame.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct LabelingConfig {
    #[serde(rename = "tagSets")]
    pub tag_sets: Vec<TagSet>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Labels {
    #[serde(rename = "tagSets")]
    pub tag_sets: Vec<TagSet>,
}

impl Default for Labels {
    fn default() -> Self {
        Self {
            tag_sets: Default::default(),
        }
    }
}

/// A set of tags that a frame can be labeled with.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TagSet {
    pub name: String,
    pub tags: Vec<String>,
}

pub async fn load_labeling_config(path: &Path) -> Result<LabelingConfig, Box<dyn Error>> {
    let mut file = tokio::fs::File::open(path).await?;

    let mut serialized_metadata = Vec::new();
    file.read_to_end(&mut serialized_metadata).await?;

    let config: LabelingConfig = serde_json::from_slice(&serialized_metadata)?;
    Ok(config)
}

/// LabelStore manages storage and retrieval of frame labels set via the API.
///
/// The labels are stored as JSON files on disk, in practice in the same directory as
/// [crate::frame::FrameStore] uses. Maybe the two stores should be combined.
pub struct LabelStore {
    pub config: LabelingConfig,
    storage_dir: Box<Path>,
    global_lock: RwLock<()>,
}

impl LabelStore {
    pub fn new(config: LabelingConfig, storage_dir: Box<Path>) -> Arc<LabelStore> {
        Arc::new(LabelStore {
            config,
            storage_dir,
            global_lock: RwLock::new(()),
        })
    }

    fn label_file_path(&self, frame_id: &str) -> PathBuf {
        let mut path = self.storage_dir.join(frame_id);
        path.set_extension("labels.json");
        path
    }

    pub async fn get_labels(&self, frame_id: &str) -> Result<Labels, Box<dyn Error>> {
        let path = self.label_file_path(frame_id);

        let _lock = self.global_lock.read().await;

        let mut file = match tokio::fs::File::open(path).await {
            Ok(f) => f,
            Err(_) => return Ok(Labels::default()),
        };

        let mut serialized_metadata = Vec::new();
        file.read_to_end(&mut serialized_metadata).await?;

        let labels: Labels = serde_json::from_slice(&serialized_metadata)?;
        Ok(labels)
    }

    pub async fn set_tags(&self, frame_id: &str, tags: TagSet) -> Result<Labels, Box<dyn Error>> {
        let path = self.label_file_path(frame_id);

        let _lock = self.global_lock.write().await;

        let mut labels: Labels = match tokio::fs::File::open(&path).await {
            Ok(mut file) => {
                let mut serialized_metadata = Vec::new();
                file.read_to_end(&mut serialized_metadata).await?;
                serde_json::from_slice(&serialized_metadata)?
            }
            Err(_) => Labels::default(),
        };

        let mut tag_sets: Vec<TagSet> = labels
            .tag_sets
            .into_iter()
            .filter(|t| t.name != tags.name)
            .collect();
        tag_sets.push(tags);
        tag_sets.sort_by_key(|s| s.name.clone());

        labels.tag_sets = tag_sets;

        let json = serde_json::ser::to_string_pretty(&labels)?;

        tokio::fs::write(path, json).await?;

        Ok(labels)
    }

    pub async fn delete(&self, frame_id: &str) -> Result<(), std::io::Error> {
        let path = self.label_file_path(frame_id);

        let _lock = self.global_lock.write().await;

        tokio::fs::remove_file(path).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::training::{LabelStore, LabelingConfig, TagSet};

    #[tokio::test]
    async fn test_get_empty() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = LabelStore::new(
            LabelingConfig {
                tag_sets: vec![TagSet {
                    name: "foo".to_string(),
                    tags: vec!["foo1".to_string(), "foo2".to_string(), "foo3".to_string()],
                }],
            },
            dir.path().into(),
        );

        let actual = store.get_labels("2025-10-15_16-59-50-912_+0000").await;
        assert_eq!(actual.is_ok(), true);
        assert_eq!(actual.unwrap().tag_sets.len(), 0);
    }

    #[tokio::test]
    async fn test_labels_file_round_trip() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = LabelStore::new(
            LabelingConfig {
                tag_sets: vec![TagSet {
                    name: "foo".to_string(),
                    tags: vec!["foo1".to_string(), "foo2".to_string(), "foo3".to_string()],
                }],
            },
            dir.path().into(),
        );

        let tags = TagSet {
            name: "foo".to_string(),
            tags: vec!["foo2".to_string()],
        };
        store
            .set_tags("2025-10-15_16-59-50-912_+0000", tags)
            .await
            .unwrap();

        let actual = store.get_labels("2025-10-15_16-59-50-912_+0000").await;
        assert_eq!(actual.is_ok(), true);
        let actual_tags = actual.unwrap().tag_sets;
        assert_eq!(actual_tags.len(), 1);
        let actual_tags_0 = actual_tags.get(0).unwrap();
        assert_eq!(actual_tags_0.name, "foo");
        let actual_tags_0_tags = &actual_tags_0.tags;
        assert_eq!(actual_tags_0_tags.len(), 1);
        assert_eq!(actual_tags_0_tags.get(0).unwrap(), "foo2");
    }

    #[tokio::test]
    async fn test_delete() {
        let dir = tempdir().expect("Failed to create temporary directory");

        let store = LabelStore::new(
            LabelingConfig {
                tag_sets: vec![TagSet {
                    name: "foo".to_string(),
                    tags: vec!["foo1".to_string(), "foo2".to_string(), "foo3".to_string()],
                }],
            },
            dir.path().into(),
        );

        let tags = TagSet {
            name: "foo".to_string(),
            tags: vec!["foo2".to_string()],
        };
        store
            .set_tags("2025-10-15_16-59-50-912_+0000", tags)
            .await
            .unwrap();

        let actual = store.get_labels("2025-10-15_16-59-50-912_+0000").await;
        assert_eq!(actual.is_ok(), true);
        assert_eq!(actual.unwrap().tag_sets.len(), 1);

        store.delete("2025-10-15_16-59-50-912_+0000").await.unwrap();

        let actual = store.get_labels("2025-10-15_16-59-50-912_+0000").await;
        // Returns a default one
        assert_eq!(actual.is_ok(), true);
        assert_eq!(actual.unwrap().tag_sets.len(), 0);
    }
}
