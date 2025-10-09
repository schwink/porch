use std::sync::{Arc, Weak};

use log::{error, info, warn};
use opencv::prelude::*;
use tokio::sync::Mutex;
use tracing::{Level, span};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{prelude::*, registry::Registry};

#[derive(Clone)]
pub struct Frame {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub jpeg: Arc<[u8]>,
    pub average_hash: String,
    pub p_hash: String,
}

/**
 * Manages access to the camera.
 */
pub struct CameraService {
    weak_self: Weak<CameraService>,
    subscriber_count: Mutex<usize>,
    cmd_tx: tokio::sync::mpsc::Sender<StreamCommand>,
    pub frame_rx: tokio::sync::broadcast::Receiver<Frame>,
}

impl CameraService {
    pub fn new(trace_dir: Option<std::path::PathBuf>) -> Arc<Self> {
        let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel::<StreamCommand>(10);
        let (frame_tx, frame_rx) = tokio::sync::broadcast::channel::<Frame>(1);

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
                        warn!("Failed to get uvc context: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                let devices = match context.devices() {
                    Ok(d) => d,
                    Err(e) => {
                        warn!("Failed to enumerate uvc devices: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                let device = match devices.last() {
                    Some(d) => d,
                    None => {
                        warn!("No uvc devices found");
                        tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
                        continue 'initialization;
                    }
                };
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

                // This is the most common step to fail
                let device_handle = match device.open() {
                    Ok(handle) => handle,
                    Err(e) => {
                        warn!("Failed to open device: {:?}", e);
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                wait_for_state(StreamCommand::Start, &mut state, &mut cmd_rx).await;

                loop {
                    {
                        info!("Starting the camera");

                        let mut stream_handle = match device_handle
                            .get_stream_handle_with_format_size_and_fps(
                                uvc::FrameFormat::Uncompressed,
                                800,
                                600,
                                5,
                            ) {
                            Ok(handle) => handle,
                            Err(e) => {
                                warn!("Failed to get stream handle: {:?}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                                continue 'initialization;
                            }
                        };

                        let stream_callback_data = StreamCallbackData {
                            tx: frame_tx.clone(),
                            trace_dir: trace_dir.clone(),
                            trace_subscriber_guard: None,
                            trace_flush_guard: std::sync::Mutex::new(None),
                        };
                        let stream = match stream_handle
                            .start_stream(stream_callback, stream_callback_data)
                        {
                            Ok(s) => s,
                            Err(e) => {
                                warn!("Failed to start stream: {:?}", e);
                                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                                continue 'initialization;
                            }
                        };

                        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                        // Wait for Stop command
                        wait_for_state(StreamCommand::Stop, &mut state, &mut cmd_rx).await;

                        info!("Stopping the camera");

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
        info!("CameraService::start");

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
        info!("CameraService::stop");

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

struct StreamCallbackData {
    tx: tokio::sync::broadcast::Sender<Frame>,
    trace_dir: Option<std::path::PathBuf>,

    // When subscriber_guard drops, the subscriber is removed from the thread
    trace_subscriber_guard: Option<tracing::subscriber::DefaultGuard>,

    // When chrome_guard drops, the log file is written
    trace_flush_guard: std::sync::Mutex<Option<tracing_chrome::FlushGuard>>,
}

/**
 * Install trace collection on the current thread, outputting JSON files that can be read as
 * icicle charts by e.g. chrome://tracing/.
 *
 * The stream_callback occurs on a dedicated thread managed by UVC. We don't want to do too much
 * work in this thread lest it become bogged down.
 */
fn install_tracing(data: &mut StreamCallbackData, timestamp: &chrono::DateTime<chrono::Utc>) {
    if data.trace_dir.is_none() {
        // Tracing not enabled
        return;
    }

    if data.trace_subscriber_guard.is_some() {
        // Already instrumented
        println!("stream_callback is already instrumented");
        return;
    }

    let trace_dir = data.trace_dir.as_ref().unwrap();

    let mut trace_file = std::path::Path::join(
        &trace_dir,
        crate::api::time_to_file_basename::<chrono::Utc>(timestamp),
    );
    trace_file.set_extension("json");

    let (chrome_layer, chrome_guard) = ChromeLayerBuilder::new().file(trace_file).build();

    // Register the ChromeLayer with the tracing subscriber
    let subscriber = Registry::default().with(chrome_layer);

    let subscriber_guard = tracing::subscriber::set_default(subscriber);

    data.trace_subscriber_guard = Some(subscriber_guard);
    data.trace_flush_guard = std::sync::Mutex::new(Some(chrome_guard));

    println!("stream_callback is now instrumented");
}

fn stream_callback(frame: &uvc::Frame, data: &mut StreamCallbackData) {
    info!("Got a frame in format {:?}", frame.format());
    let timestamp = chrono::Utc::now();
    install_tracing(data, &timestamp);

    let span = span!(Level::TRACE, "stream_callback");
    let _enter = span.enter();

    let rgb = {
        let span = span!(Level::TRACE, "to_rgb");
        let _enter = span.enter();

        match frame.to_rgb() {
            Ok(f) => f,
            Err(e) => {
                error!("Failed to convert frame to RGB: {:?}", e);
                return;
            }
        }
    };
    let rgb_slice: &[u8] = &rgb.to_bytes();

    let jpeg_buffer: Arc<[u8]> = {
        let span = span!(Level::TRACE, "turbojpeg");
        let _enter = span.enter();

        let image = turbojpeg::Image {
            format: turbojpeg::PixelFormat::RGB,
            height: frame.height() as usize,
            width: frame.width() as usize,
            pixels: rgb_slice,
            pitch: (frame.width() * 3) as usize,
        };

        let jpeg = match turbojpeg::compress(image, 90, turbojpeg::Subsamp::Sub2x2) {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to compress frame to JPEG: {:?}", e);
                return;
            }
        };

        let jpeg_slice: &[u8] = &jpeg;

        Arc::from(jpeg_slice)
    };

    let mat = {
        let span = span!(Level::TRACE, "opencv_mat");
        let _enter = span.enter();

        let mat = match opencv::prelude::Mat::new_rows_cols_with_bytes::<u8>(
            frame.height() as i32,
            frame.width() as i32 * 3,
            rgb_slice,
        ) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to create opencv Mat from RGB data {}", e);
                return;
            }
        };

        if let Err(e) = opencv::imgproc::cvt_color(
            &mat,
            &mut mat.clone_pointee(),
            opencv::imgproc::COLOR_RGB2BGR,
            0,
            // hint parameter added in opencv v4.11
            // opencv::core::AlgorithmHint::ALGO_HINT_DEFAULT,
        ) {
            error!("Failed to convert RGB to BGR: {:?}", e);
            return;
        };

        mat
    };

    let average_hash = {
        let span = span!(Level::TRACE, "opencv_average_hash");
        let _enter = span.enter();

        match opencv::img_hash::AverageHash::create().and_then(|mut hasher| {
            let mut hash = opencv::core::Mat::default();
            hasher.compute(&mat, &mut hash)?;
            Ok(hash_to_hex_string(&hash))
        }) {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to compute average hash: {}", e);
                return;
            }
        }
    };

    let p_hash = {
        let span: span::Span = span!(Level::TRACE, "opencv_p_hash");
        let _enter = span.enter();

        match opencv::img_hash::PHash::create().and_then(|mut hasher| {
            let mut hash = opencv::core::Mat::default();
            hasher.compute(&mat, &mut hash)?;
            Ok(hash_to_hex_string(&hash))
        }) {
            Ok(h) => h,
            Err(e) => {
                error!("Failed to compute p hash: {}", e);
                return;
            }
        }
    };

    {
        let span: span::Span = span!(Level::TRACE, "send");
        let _enter = span.enter();

        let frame = Frame {
            timestamp,
            jpeg: jpeg_buffer,
            average_hash: average_hash,
            p_hash: p_hash,
        };

        let num_subscribers = match data.tx.send(frame) {
            Ok(n) => n - 1, // Don't count the service's copy of the receiver
            Err(_) => 0,
        };
        info!("Broadcasted a frame to {} subscribers", num_subscribers)
    }
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
    pub rx: tokio::sync::broadcast::Receiver<Frame>,
}

impl Drop for StreamHandle {
    fn drop(&mut self) {
        self.service.stop();
    }
}

fn hash_to_hex_string(hash: &opencv::core::Mat) -> String {
    let mut s = String::with_capacity(16);
    for i in 0..hash.total() {
        let v = hash.at::<u8>(i as i32).unwrap_or(&0);
        s.push_str(&format!("{:02x}", v));
    }
    s
}
