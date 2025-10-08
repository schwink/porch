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
    pub average_hash_distance: Option<u64>,
    pub p_hash: String,
    pub p_hash_distance: Option<u64>,
}

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    image_storage_dir: PathBuf,

    #[arg(long)]
    start_watching_immediately: bool,
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

    if cli.start_watching_immediately {
        watch_scheduled_camera(
            &camera_service,
            &image_storage_dir,
            chrono::Local::now() + chrono::Duration::hours(1),
        )
        .await;
    }

    let scheduled_watch_handle = tokio::spawn(async move {
        // Every weekday at 7:00am, watch for two hours
        // Format is "sec min hour day month weekday year"
        let start_watching_cron_expression = "0 0 7 * * Mon,Tue,Wed,Thu,Fri *";
        let watch_duration = chrono::Duration::hours(2);

        let schedule = cron::Schedule::from_str(start_watching_cron_expression).unwrap();
        for start_time in schedule.upcoming(chrono::Local) {
            let stop_time = start_time + watch_duration;

            let start_timestamp_offset = start_time.timestamp() - chrono::Local::now().timestamp();
            println!(
                "Next scheduled camera start is at {} in {} seconds",
                start_time, start_timestamp_offset
            );
            if start_timestamp_offset > 0 {
                let delay = Duration::from_secs(start_timestamp_offset.try_into().unwrap());
                tokio::time::sleep_until(tokio::time::Instant::now() + delay).await;
            }

            watch_scheduled_camera(&camera_service, &image_storage_dir, stop_time).await;
        }
    });

    scheduled_watch_handle.await.unwrap();
}

async fn watch_scheduled_camera(
    camera_service: &camera::CameraService,
    image_storage_dir: &PathBuf,
    stop_time: chrono::DateTime<chrono::Local>,
) {
    println!("Starting scheduled camera at {}", chrono::Local::now());
    let mut handle: camera::StreamHandle = camera_service.start().await;

    let mut prev_average_hash: Option<String> = None;
    let mut prev_p_hash: Option<String> = None;

    loop {
        if chrono::Local::now() >= stop_time {
            println!("Stopping scheduled camera at {}", stop_time);
            break;
        }

        let frame = handle.rx.recv().await.unwrap();

        let average_hash_distance = prev_average_hash
            .map(|a| hamming::distance(a.as_bytes(), &frame.average_hash.as_bytes()));
        prev_average_hash = Some(frame.average_hash.clone());

        let p_hash_distance =
            prev_p_hash.map(|p| hamming::distance(p.as_bytes(), &frame.p_hash.as_bytes()));
        prev_p_hash = Some(frame.p_hash.clone());

        if let Some(distance) = average_hash_distance {
            println!("average hash distance is {}", distance);
        }
        if let Some(distance) = p_hash_distance {
            println!("p hash distance is {}", distance);
            if distance < 25 {
                println!(
                    "Skipping frame at {} due to low p hash distance of {}",
                    frame.timestamp, distance
                );
                // Skip duplicate frames
                continue;
            }
        }

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
            average_hash_distance,
            p_hash: frame.p_hash,
            p_hash_distance,
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
}
