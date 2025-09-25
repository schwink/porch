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

        // The libuvc library wrapper involves a lot of references between components, which makes
        // it difficult to keep state in a Rust struct due to lifetime dependencies. To work around
        // this, we start a dedicated task to manage the camera, and communicate with it via
        // channels.
        tokio::task::spawn(async move {
            let mut state = StreamCommand::Stop;

            'initialization: loop {
                let context = match uvc::Context::new() {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("Failed to get uvc context: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                let devices = match context.devices() {
                    Ok(d) => d,
                    Err(e) => {
                        eprintln!("Failed to enumerate uvc devices: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                let device = match devices.last() {
                    Some(d) => d,
                    None => {
                        eprintln!("No uvc devices found");
                        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        continue 'initialization;
                    }
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                // This is the most common step to fail
                let device_handle = match device.open() {
                    Ok(handle) => handle,
                    Err(e) => {
                        eprintln!("Failed to open device: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                wait_for_state(StreamCommand::Start, &mut state, &mut cmd_rx).await;

                loop {
                    {
                        println!("Starting the camera");

                        let mut stream_handle = match device_handle
                            .get_stream_handle_with_format_size_and_fps(
                                uvc::FrameFormat::MJPEG,
                                800,
                                600,
                                5,
                            ) {
                            Ok(handle) => handle,
                            Err(e) => {
                                eprintln!("Failed to get stream handle: {:?}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                                continue 'initialization;
                            }
                        };

                        let stream =
                            match stream_handle.start_stream(stream_callback, frame_tx.clone()) {
                                Ok(s) => s,
                                Err(e) => {
                                    eprintln!("Failed to start stream: {:?}", e);
                                    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                                    continue 'initialization;
                                }
                            };

                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                        // Wait for Stop command
                        wait_for_state(StreamCommand::Stop, &mut state, &mut cmd_rx).await;

                        println!("Stopping the camera");

                        stream.stop();

                        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                    }

                    // Wait for Start command
                    wait_for_state(StreamCommand::Start, &mut state, &mut cmd_rx).await;
                }
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

        if let Err(_) = self.cmd_tx.send(StreamCommand::Start).await {
            panic!("Failed to send start command to camera task");
        }

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
            if let Err(_) = self.cmd_tx.try_send(StreamCommand::Stop) {
                panic!("Failed to send stop command to camera task");
            }
        }
    }
}

async fn wait_for_state(
    desired_state: StreamCommand,
    current_state: &mut StreamCommand,
    rx: &mut tokio::sync::mpsc::Receiver<StreamCommand>,
) {
    if *current_state == desired_state {
        return;
    }

    loop {
        let message = rx.recv().await;
        if message.is_some_and(|v| v == desired_state) {
            *current_state = desired_state;
            break;
        } else {
            continue;
        }
    }
}

fn stream_callback(frame: &uvc::Frame, tx: &mut tokio::sync::broadcast::Sender<Arc<uvc::Frame>>) {
    let num_subscribers = match tx.send(Arc::new(frame.duplicate().unwrap())) {
        Ok(n) => n - 1, // Don't count the service's copy of the receiver
        Err(_) => 0,
    };
    println!("Broadcasted a frame to {} subscribers", num_subscribers)
}

#[derive(PartialEq, Eq)]
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
