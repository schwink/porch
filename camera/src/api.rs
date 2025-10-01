use std::{path::PathBuf, sync::Arc};

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Frame {
    name: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Frames {
    nodes: Vec<Frame>,

    #[serde(rename = "hasNextPage")]
    has_next_page: bool,
    #[serde(rename = "endCursor")]
    end_cursor: Option<String>,

    #[serde(rename = "hasPreviousPage")]
    has_previous_page: bool,
    #[serde(rename = "startCursor")]
    start_cursor: Option<String>,
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

    /**
     * Load frames with cursor-based pagination.
     *
     * [oldest] ... [start of page] ... [end of page] ... [newest]
     *              ^ start cursor      ^ end cursor
     *
     * To fetch neweset frames:
     *                                       |-------------------|
     *                                       ^ last: count       ^
     *
     * To paginate backwards to older frames:
     *                   |-------------------|
     *                   ^ last: count       ^ before: previous start cursor
     *
     * To fetch oldest frames: (not implemented)
     * |-------------------|
     * ^                   ^ first: count
     *
     * To paginate forwards to newer frames: (not implemented)
     *                     |-------------------------------|
     *                     ^ after: previous end cursor    ^ first: count
     */
    pub async fn frames(
        &self,
        last: Option<usize>,
        before: Option<String>,
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
        let frames: Vec<Frame> = entries
            .into_iter()
            .take(size)
            .map(|e| Frame {
                name: e.file_name().into_string().unwrap(),
            })
            .collect();
        let start_cursor = frames.last().map(|f| f.name.clone());
        let end_cursor = frames.first().map(|f| f.name.clone());

        Ok(Frames {
            nodes: frames,
            has_next_page: false,
            has_previous_page: end_cursor.is_some(),
            start_cursor,
            end_cursor,
        })
    }
}
