use std::path::PathBuf;

use chrono::Utc;
use chrono_tz::America;
use clap::Parser;
use log::{LevelFilter, info};

use crate::controller::scheduled::ScheduledCapture;

mod api;
mod camera;
mod controller;
mod store;
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

    let image_storage_dir = cli.image_storage_dir;
    match tokio::fs::create_dir_all(image_storage_dir.clone()).await {
        Err(e) => {
            panic!(
                "Unable to create image storage path {:?}: {:?}",
                image_storage_dir, e
            )
        }
        _ => (),
    };

    let frame_store = store::FrameStore::new(image_storage_dir.as_path());

    let camera_service = camera::CameraService::new(cli.log_dir);

    let api = api::Api::new(frame_store.clone());

    let _webserver = webserver::WebServer::new(
        camera_service.clone(),
        api.clone(),
        frame_store.clone(),
        Box::from(image_storage_dir.as_path()),
    )
    .await;

    if cli.start_watching_immediately {
        controller::capture::start_capture(
            &camera_service,
            &frame_store,
            Utc::now().with_timezone(&America::Los_Angeles) + chrono::Duration::hours(1),
            America::Los_Angeles,
        )
        .await
        .unwrap();
    }

    let weekday_mornings = &mut ScheduledCapture::start_with_cron(
        camera_service.clone(),
        frame_store.clone(),
        // Every weekday at 7:00am, watch for three hours
        "0 0 7 * * Mon,Tue,Wed,Thu,Fri *",
        America::Los_Angeles,
        chrono::Duration::hours(3),
    )
    .unwrap();
    info!("Scheduled capture {}", weekday_mornings);

    let weekends = &mut ScheduledCapture::start_with_cron(
        camera_service,
        frame_store.clone(),
        // Every weekend at 7:00am, watch for six hours
        "0 0 7 * * Sat,Sun *",
        America::Los_Angeles,
        chrono::Duration::hours(6),
    )
    .unwrap();
    info!("Scheduled capture {}", weekends);

    weekday_mornings.handle.get_mut().await.unwrap();
    weekends.handle.get_mut().await.unwrap();
}
