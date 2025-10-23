use std::cell::Cell;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use log::{error, info};

use crate::camera;
use crate::controller::capture;
use crate::inference;
use crate::store;

/**
 * Represents a recurring period of time in which the camera is woken up and frames are captured,
 * checked for visual changes since the previous frame, and saved to disk if changed.
 */
pub struct ScheduledCapture<Tz>
where
    Tz: TimeZone + std::marker::Send + 'static,
    <Tz as TimeZone>::Offset: std::marker::Send,
{
    pub handle: Cell<tokio::task::JoinHandle<()>>,
    cron: String,
    timezone: Tz,
    duration: chrono::Duration,
}

impl<Tz> ScheduledCapture<Tz>
where
    Tz: TimeZone + std::marker::Send + 'static,
    <Tz as TimeZone>::Offset: std::marker::Send,
{
    /**
     * Cron format is "sec min hour day month weekday year"
     */
    pub fn start_with_cron(
        camera_service: Arc<camera::CameraService>,
        frame_store: Arc<store::FrameStore>,
        inference_service: Arc<inference::InferenceService>,
        trace_dir: Option<std::path::PathBuf>,
        start_watching_cron_expression: &str,
        timezone: Tz,
        duration: chrono::Duration,
    ) -> Result<ScheduledCapture<Tz>, Box<dyn Error>> {
        let schedule = cron::Schedule::from_str(start_watching_cron_expression)?;
        let tz = timezone.clone();

        let handle = tokio::spawn(async move {
            for start_time in schedule.upcoming(timezone.clone()) {
                let stop_time = start_time.clone() + duration;

                let start_timestamp_offset =
                    start_time.timestamp() - Utc::now().with_timezone(&timezone).timestamp();
                info!(
                    "Next scheduled camera start is at {:?} in {} seconds",
                    start_time, start_timestamp_offset
                );
                if start_timestamp_offset > 0 {
                    let delay = Duration::from_secs(start_timestamp_offset.try_into().unwrap());
                    tokio::time::sleep_until(tokio::time::Instant::now() + delay).await;
                }

                if let Err(e) = capture::start_capture(
                    &camera_service,
                    frame_store.clone(),
                    inference_service.clone(),
                    &trace_dir,
                    stop_time,
                    timezone.clone(),
                )
                .await
                {
                    error!("Scheduled capture error: {:?}", e)
                };
            }
        });

        return Ok(ScheduledCapture {
            handle: Cell::from(handle),
            cron: start_watching_cron_expression.to_string(),
            timezone: tz,
            duration,
        });
    }
}

impl<Tz> Drop for ScheduledCapture<Tz>
where
    Tz: TimeZone + std::marker::Send,
    <Tz as TimeZone>::Offset: std::marker::Send,
{
    fn drop(&mut self) {
        self.handle.get_mut().abort();
    }
}

impl<Tz> Display for ScheduledCapture<Tz>
where
    Tz: TimeZone + std::marker::Send + std::fmt::Display,
    <Tz as TimeZone>::Offset: std::marker::Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "\"{}\" ({}) for {} minutes",
            self.cron,
            self.timezone,
            self.duration.as_seconds_f64() / (60 as f64)
        )
    }
}
