use std::sync::Arc;

use chrono::{DateTime, TimeZone};
use futures::StreamExt;
use futures::stream::FuturesOrdered;
use serde::{Deserialize, Serialize};

use crate::store;
use crate::training;

#[derive(Debug, Deserialize, Serialize)]
pub struct Frame {
    pub id: String,
    pub src: String,
    pub timestamp: i64,
    pub p_hash: String,
    pub p_hash_distance: Option<u64>,
    pub labels: Option<training::Labels>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FrameEdge {
    pub node: Frame,
    pub cursor: String,
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
    frame_store: Arc<store::FrameStore>,
    label_store: Arc<training::LabelStore>,
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
    pub fn new(
        frame_store: Arc<store::FrameStore>,
        label_store: Arc<training::LabelStore>,
    ) -> Arc<Api> {
        Arc::new(Api {
            frame_store,
            label_store,
        })
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
        let frame_metadatas = self
            .frame_store
            .list_frames(last, before, None, None)
            .await
            .map_err(|e| ApiError {
                code: axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                message: e.to_string(),
            })?;

        let frames: Vec<Frame> = frame_metadatas
            .into_iter()
            .map(async |metadata| {
                let labels = self.label_store.get_labels(&metadata.name).await;

                Frame {
                    id: metadata.name.clone(),
                    src: format!("{}.jpg", metadata.name),
                    timestamp: metadata.timestamp,
                    p_hash: metadata.p_hash,
                    p_hash_distance: metadata.p_hash_distance,
                    labels: labels.ok(),
                }
            })
            // Collect into a FuturesUnordered to run the file reads in parallel
            .collect::<FuturesOrdered<_>>()
            // Join the futures and collect into the result Vec
            .collect()
            .await;

        let start_cursor = frames.last().map(|f| f.src.clone());
        let end_cursor = frames.first().map(|f| f.src.clone());

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
