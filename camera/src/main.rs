use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use clap::Parser;

use serde::{Deserialize, Serialize};
use serde_json::to_string_pretty;

mod api;
mod camera;
mod webserver;

#[derive(Debug, Deserialize, Serialize)]
pub struct FrameMetadata {
    pub name: String,
    pub timestamp: i64,
    pub average_hash: String,
    pub p_hash: String,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    image_storage_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    let image_storage_dir: PathBuf = cli.image_storage_dir;
    match tokio::fs::create_dir_all(image_storage_dir.as_path()).await {
        Err(e) => {
            panic!(
                "Unable to create image storage path {:?}: {:?}",
                image_storage_dir, e
            )
        }
        _ => (),
    };

    let camera_service = camera::CameraService::new();

    let api = api::Api::new(image_storage_dir.clone());

    let _webserver = webserver::WebServer::new(
        camera_service.clone(),
        api.clone(),
        image_storage_dir.clone(),
    )
    .await;

    let handle = tokio::spawn(async move {
        // Every 5 minutes
        let cron_expression = "0 0,5,10,15,20,25,30,35,40,45,50,55 * * * * *";

        let schedule = cron::Schedule::from_str(cron_expression).unwrap();
        for datetime in schedule.upcoming(chrono::Utc).take(10) {
            let offset = datetime.timestamp() - chrono::Utc::now().timestamp();
            println!("Next scheduled photo at {} in {} seconds", datetime, offset);
            if offset > 0 {
                let delay = Duration::from_secs(offset.try_into().unwrap());
                tokio::time::sleep_until(tokio::time::Instant::now() + delay).await;
            }

            let mut handle: camera::StreamHandle = camera_service.start().await;

            let frame = handle.rx.recv().await.unwrap();

            let filename = api::time_to_file_basename(frame.timestamp);
            let mut path = image_storage_dir.join(&filename);
            path.set_extension("jpg");

            match tokio::fs::write(&path, frame.jpeg).await {
                Ok(()) => println!("Wrote {:?}", path),
                Err(e) => {
                    eprintln!("Failed to write path {:?}: {:?}", path, e);
                }
            };

            let frame_metadata = FrameMetadata {
                name: filename,
                timestamp: frame.timestamp.timestamp_millis(),
                average_hash: frame.average_hash,
                p_hash: frame.p_hash,
            };
            let frame_metadata_json = to_string_pretty(&frame_metadata).unwrap();

            path.set_extension("json");
            match tokio::fs::write(&path, frame_metadata_json).await {
                Ok(()) => println!("Wrote {:?}", path),
                Err(e) => {
                    eprintln!("Failed to write path {:?}: {:?}", path, e);
                }
            };
        }
    });

    handle.await.unwrap();
}
