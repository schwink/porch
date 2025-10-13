use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::time::Duration;

use chrono::{TimeZone, Utc};
use chrono_tz::America;
use clap::Parser;
use log::LevelFilter;
use log::{error, info};

mod api;
mod camera;
mod pipeline;
mod webserver;

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
    image_storage_dir: &Path,
    stop_time: chrono::DateTime<Tz>,
    timezone: Tz,
) {
    info!(
        "Starting scheduled camera at {:?}",
        Utc::now().with_timezone(&timezone)
    );
    let mut handle: camera::StreamHandle = camera_service.start().await;

    let mut prev_p_hash: Option<String> = None;

    loop {
        if chrono::Local::now() >= stop_time {
            info!("Stopping scheduled camera at {:?}", stop_time);
            break;
        }

        let frame = handle.rx.recv().await.unwrap();

        let p_hash_distance =
            prev_p_hash.map(|p| hamming::distance(p.as_bytes(), &frame.p_hash.as_bytes()));
        prev_p_hash = Some(frame.p_hash.clone());

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

        match pipeline::write_frame_capture_data(image_storage_dir, frame, p_hash_distance).await {
            Ok(metadata) => info!("Persisted frame {}", metadata.name),
            Err(e) => {
                error!("Failed to persist frame: {:?}", e)
            }
        }
    }
}
