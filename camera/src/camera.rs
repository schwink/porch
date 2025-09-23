use std::sync::{Arc, Weak};

use tokio::sync::Mutex;

/**
 * Manages access to the camera.
 */
pub struct CameraService {
    weak_self: Weak<CameraService>,
    subscriber_count: Mutex<usize>,
    cmd_tx: tokio::sync::mpsc::Sender<StreamCommand>,
    frame_rx: tokio::sync::broadcast::Receiver<Arc<uvc::Frame>>,
}

impl CameraService {
    pub fn new() -> Arc<Self> {
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<StreamCommand>(10);
        let (frame_tx, frame_rx) = tokio::sync::broadcast::channel::<Arc<uvc::Frame>>(1);

        // The libuvc library wrapper involves a lot of references between structs, which
        tokio::spawn(async move {
            let context = uvc::Context::new().expect("Could not get uvc context");

            let devices = context.devices().expect("Could not enumerate uvc devices");
            let device = devices.last().expect("No uvc devices found");

            let device_handle = device.open().expect("Could not open device");

            loop {
                // Wait for Start command
                loop {
                    let command = cmd_rx.recv().await;
                    match command {
                        Some(StreamCommand::Start) => break,
                        _ => continue,
                    }
                }

                println!("Starting the camera");

                let mut stream_handle = device_handle
                    .get_stream_handle_with_format_size_and_fps(
                        uvc::FrameFormat::MJPEG,
                        800,
                        600,
                        5,
                    )
                    .expect("Could not get stream handle");

                let stream = stream_handle
                    .start_stream(stream_callback, frame_tx.clone())
                    .unwrap();

                // Wait for Stop command
                loop {
                    let command = cmd_rx.recv().await;
                    match command {
                        Some(StreamCommand::Stop) => break,
                        _ => continue,
                    }
                }

                println!("Stopping the camera");

                stream.stop();
            }
        });

        Arc::new_cyclic(|w: &Weak<CameraService>| CameraService {
            weak_self: w.clone(),
            subscriber_count: Mutex::new(0),
            cmd_tx,
            frame_rx,
        })
    }

    pub async fn start(&self) -> StreamHandle {
        println!("CameraService::start");

        let mut subscriber_count = self.subscriber_count.lock().await;
        *subscriber_count += 1;

        self.cmd_tx.send(StreamCommand::Start).await.unwrap();

        StreamHandle {
            service: self.weak_self.upgrade().unwrap(),
            rx: self.frame_rx.resubscribe(),
        }
    }

    fn stop(&self) {
        println!("CameraService::stop");

        let mut subscriber_count = self.subscriber_count.try_lock().unwrap();
        *subscriber_count -= 1;

        if *subscriber_count <= 0 {
            let _ = self.cmd_tx.try_send(StreamCommand::Stop);
        }
    }
}

fn stream_callback(frame: &uvc::Frame, tx: &mut tokio::sync::broadcast::Sender<Arc<uvc::Frame>>) {
    println!("Broadcasting a frame");
    tx.send(Arc::new(frame.duplicate().unwrap())).unwrap();
}

enum StreamCommand {
    Start,
    Stop,
}

/**
 * Handle to the active camera stream which is broadcasting frames.
 */
pub struct StreamHandle {
    service: Arc<CameraService>,
    pub rx: tokio::sync::broadcast::Receiver<Arc<uvc::Frame>>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.service.stop();
    }
}
