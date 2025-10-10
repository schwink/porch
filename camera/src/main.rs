use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use chrono_tz::America;
use clap::Parser;
use log::LevelFilter;
use log::{error, info};
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
    log_dir: Option<PathBuf>,

    #[arg(long)]
    start_watching_immediately: bool,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    let cli = Cli::parse();

    let simplelog_config = simplelog::ConfigBuilder::new()
        .set_thread_level(LevelFilter::Error)
        .set_thread_mode(simplelog::ThreadLogMode::Both)
        .set_location_level(LevelFilter::Error)
        .build();
    let mut loggers: Vec<Box<dyn simplelog::SharedLogger>> = vec![simplelog::TermLogger::new(
        LevelFilter::Info,
        simplelog_config.clone(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )];
    if let Some(log_dir) = &cli.log_dir {
        match tokio::fs::create_dir_all(log_dir).await {
            Ok(_) => {
                let log_path = log_dir.join("porch.log");
                if let Ok(log_file) = std::fs::File::create(&log_path) {
                    println!("Logging to {:?}", log_path);
                    loggers.push(simplelog::WriteLogger::new(
                        LevelFilter::Debug,
                        simplelog_config,
                        log_file,
                    ));
                } else {
                    eprintln!("Unable to create log file {:?}", log_path);
                }
            }
            Err(e) => {
                eprintln!("Unable to create log directory {:?}: {:?}", log_dir, e);
            }
        }
    }

    simplelog::CombinedLogger::init(loggers).unwrap();

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

    let camera_service = camera::CameraService::new(cli.log_dir);

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
            Utc::now().with_timezone(&America::Los_Angeles) + chrono::Duration::hours(1),
            America::Los_Angeles,
        )
        .await;
    }

    let scheduled_watch_handle = tokio::spawn(async move {
        // Every weekday at 7:00am, watch for two hours
        // Format is "sec min hour day month weekday year"
        let start_watching_cron_expression = "0 0 7 * * Mon,Tue,Wed,Thu,Fri *";
        let timezone = America::Los_Angeles;
        let watch_duration = chrono::Duration::hours(2);

        let schedule = cron::Schedule::from_str(start_watching_cron_expression).unwrap();
        for start_time in schedule.upcoming(timezone) {
            let stop_time = start_time + watch_duration;

            let start_timestamp_offset =
                start_time.timestamp() - Utc::now().with_timezone(&timezone).timestamp();
            info!(
                "Next scheduled camera start is at {} in {} seconds",
                start_time, start_timestamp_offset
            );
            if start_timestamp_offset > 0 {
                let delay = Duration::from_secs(start_timestamp_offset.try_into().unwrap());
                tokio::time::sleep_until(tokio::time::Instant::now() + delay).await;
            }

            watch_scheduled_camera(&camera_service, &image_storage_dir, stop_time, timezone).await;
        }
    });

    scheduled_watch_handle.await.unwrap();
}

async fn watch_scheduled_camera<Tz: TimeZone>(
    camera_service: &camera::CameraService,
    image_storage_dir: &PathBuf,
    stop_time: chrono::DateTime<Tz>,
    timezone: Tz,
) {
    info!(
        "Starting scheduled camera at {:?}",
        Utc::now().with_timezone(&timezone)
    );
    let mut handle: camera::StreamHandle = camera_service.start().await;

    let mut prev_average_hash: Option<String> = None;
    let mut prev_p_hash: Option<String> = None;

    loop {
        if chrono::Local::now() >= stop_time {
            info!("Stopping scheduled camera at {:?}", stop_time);
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
            info!("average hash distance is {}", distance);
        }
        if let Some(distance) = p_hash_distance {
            info!("p hash distance is {}", distance);
            if distance < 10 {
                info!(
                    "Skipping frame at {} due to low p hash distance of {}",
                    frame.timestamp, distance
                );
                // Skip duplicate frames
                continue;
            }
        }

        let filename = api::time_to_file_basename(&frame.timestamp);
        let mut path = image_storage_dir.join(&filename);
        path.set_extension("jpg");

        match tokio::fs::write(&path, frame.jpeg).await {
            Ok(()) => info!("Wrote {:?}", path),
            Err(e) => {
                error!("Failed to write path {:?}: {:?}", path, e);
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
            Ok(()) => info!("Wrote {:?}", path),
            Err(e) => {
                error!("Failed to write path {:?}: {:?}", path, e);
            }
        };
    }
}
