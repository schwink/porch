use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Frame {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Frames {
    nodes: Vec<Frame>,
    has_next_page: bool,
    cursor: Option<String>,
}

pub struct ApiError {
    pub code: axum::http::StatusCode,
    pub message: String,
}

pub struct Api {
    image_storage_dir: PathBuf,
}

impl Api {
    pub fn new(image_storage_dir: PathBuf) -> Arc<Api> {
        Arc::new(Api { image_storage_dir })
    }

    pub async fn frames(
        &self,
        after_cursor: Option<String>,
        page_size: Option<usize>,
    ) -> Result<Frames, ApiError> {
        let mut ls = match tokio::fs::read_dir(self.image_storage_dir.clone()).await {
            Ok(ls) => ls,
            Err(e) => {
                return Err(ApiError {
                    code: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    message: e.to_string(),
                });
            }
        };

        let mut entries: Vec<tokio::fs::DirEntry> = Vec::new();
        while let Ok(Some(entry)) = ls.next_entry().await {
            let Ok(name) = entry.file_name().into_string() else {
                // Verifies that file names are valid UTF-8
                continue;
            };

            match after_cursor {
                None => entries.push(entry),
                Some(ref c) => {
                    if &name > c {
                        entries.push(entry);
                    }
                }
            }
        }
        entries.sort_by_key(|e| e.file_name());

        let max_size: usize = 100;
        let size = match page_size {
            None => max_size,
            Some(s) => {
                if s < max_size {
                    s
                } else {
                    max_size
                }
            }
        };
        let frames: Vec<Frame> = entries
            .into_iter()
            .take(size)
            .map(|e| Frame {
                name: e.file_name().into_string().unwrap(),
            })
            .collect();
        let cursor = frames.last().map(|f| f.name.clone());

        Ok(Frames {
            nodes: frames,
            has_next_page: cursor.is_some(),
            cursor,
        })
    }
}
