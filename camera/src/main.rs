use std::path::PathBuf;
use std::time::Duration;

use chrono::{DateTime, Local};

use clap::Parser;

mod camera;
mod webserver;

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

    let _webserver =
        webserver::WebServer::new(camera_service.clone(), image_storage_dir.clone()).await;

    let handle = tokio::spawn(async move {
        // Take a picture every 5 seconds
        loop {
            println!("5-second sleeping");
            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("5-second woke up");

            let mut handle: camera::StreamHandle = camera_service.start().await;
            println!("5-second timer got stream handle");

            let frame = handle.rx.recv().await.unwrap();
            println!("5-second timer took a picture, size: {} bytes", frame.len());

            let now: DateTime<Local> = Local::now();
            let filename = now.format("%Y-%m-%d_%H-%M-%S-%3f_%z.jpg").to_string();
            let path = image_storage_dir.join(filename);

            match tokio::fs::write(&path, frame).await {
                Ok(()) => println!("Wrote {:?}", path),
                Err(e) => {
                    eprintln!("Failed to write path {:?}: {:?}", path, e);
                }
            };
        }
    });

    handle.await.unwrap();
}
