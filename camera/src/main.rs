use std::time::Duration;

mod camera;

#[tokio::main]
async fn main() {
    let camera_service = camera::CameraService::new();

    let handle = tokio::spawn(async move {
        // Take a picture every 5 seconds
        loop {
            println!("5-second sleeping");
            tokio::time::sleep(Duration::from_secs(5)).await;
            println!("5-second woke up");

            let mut handle: camera::StreamHandle = camera_service.start().await;
            println!("5-second timer got stream handle");

            let frame = handle.rx.recv().await.unwrap();
            println!(
                "5-second timer took a picture, format: {:?}, size: {} bytes",
                frame.format(),
                frame.to_bytes().len()
            );
        }
    });

    handle.await.unwrap();
}
