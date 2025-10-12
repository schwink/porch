use std::{path::PathBuf, sync::Arc};

use chrono::{DateTime, TimeZone};
use futures::{StreamExt, stream::FuturesOrdered};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct Frame {
    name: String,
    timestamp: i64,
    p_hash: String,
    p_hash_distance: Option<u64>,
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

static FILE_NAME_FORMAT: &str = "%Y-%m-%d_%H-%M-%S-%3f_%z";

pub fn time_to_file_basename<Tz>(time: &DateTime<Tz>) -> String
where
    Tz: TimeZone,
    <Tz as TimeZone>::Offset: std::fmt::Display,
{
    time.format(FILE_NAME_FORMAT).to_string()
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

        let frames: Vec<Frame> = entries
            .into_iter()
            .map(async |e| -> Result<Frame, ()> {
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
                let metadata: crate::FrameMetadata =
                    match serde_json::from_slice(&serialized_metadata) {
                        Ok(m) => m,
                        Err(e) => {
                            eprintln!(
                                "Failed to parse metadata file for {:?}: {:?}",
                                jpg_file_name, e
                            );
                            return Err(());
                        }
                    };

                Ok(Frame {
                    name: jpg_file_name,
                    timestamp: metadata.timestamp,
                    p_hash: metadata.p_hash,
                    p_hash_distance: metadata.p_hash_distance,
                })
            })
            // Collect into a FuturesUnordered to run the file reads in parallel
            .collect::<FuturesOrdered<_>>()
            // Discard frames that failed to load
            .filter_map(|r| async move { r.ok() })
            // Join the futures and collect into the result Vec
            .collect()
            .await;

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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    #[test]
    fn test_timestamp_file_name() {
        let dt = Utc.with_ymd_and_hms(2024, 6, 15, 12, 34, 56).unwrap();
        let file_name = time_to_file_basename(&dt);
        assert_eq!(file_name, "2024-06-15_12-34-56-000_+0000");
    }
}
