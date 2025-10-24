use std::{
    sync::{Arc, Weak},
    time::Duration,
};

use log::{error, info, warn};
use opencv::core::MatTraitConst;
use opencv::prelude::*;
use tokio::sync::Mutex;
use tracing::{Level, span};
use tracing_chrome::ChromeLayerBuilder;
use tracing_subscriber::{prelude::*, registry::Registry};
use uvc::{FrameFormat, StreamFormat};

#[derive(Clone)]
pub struct Frame {
    pub timestamp: chrono::DateTime<chrono::Utc>,
    pub jpeg: Arc<[u8]>,

    /// A RGB float32[1,3,224,224] tensor
    pub inference_tensor: Arc<[f32]>,
    /// JPEG of the above, for debugging
    pub inference_jpeg: Arc<[u8]>,

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

                let mut device: Option<uvc::Device> = None;
                devices.for_each(|d| {
                    if let Ok(desc) = d.description() {
                        info!(
                            "UVC Device: Vendor 0x{:04x} ({}), Product ID 0x{:04x} ({}), Serial Number: {}",
                            desc.vendor_id,
                            desc.manufacturer.unwrap_or("unknown".to_string()),
                            desc.product_id,
                            desc.product.unwrap_or("unknown".to_string()),
                            desc.serial_number.unwrap_or("unknown".to_string()),
                        );

                        device = Some(d);
                    } else {
                        error!("Could not get device descriptor");
                    }
                });

                let device = match device {
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

                device_handle.supported_formats().for_each(|format| {
                    format.supported_formats().for_each(|supported| {
                        let fps: Vec<u32> = supported
                            .intervals_duration()
                            .iter()
                            .map(|d| (Duration::from_secs(1).as_millis() / d.as_millis()) as u32)
                            .collect();
                        info!(
                            "Format: subtype {:?}, width {} height {}, fps {:?}",
                            format.subtype(),
                            supported.width(),
                            supported.height(),
                            fps,
                        );
                    });
                });

                // Select the format with lowest FPS and highest dimensions below width=800
                let preferred_format = match device_handle.get_preferred_format(cmp_stream_format) {
                    Some(f) => {
                        info!(
                            "Selected format: subtype {:?}, width {} height {}, fps {}",
                            f.format, f.width, f.height, f.fps,
                        );
                        f
                    }
                    None => {
                        warn!("No stream formats found");
                        tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
                        continue 'initialization;
                    }
                };

                wait_for_state(StreamCommand::Start, &mut state, &mut cmd_rx).await;

                loop {
                    {
                        info!("Starting the camera");

                        let mut stream_handle =
                            match device_handle.get_stream_handle_with_format(preferred_format) {
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

fn cmp_stream_format(left: StreamFormat, right: StreamFormat) -> StreamFormat {
    if left.format == FrameFormat::Uncompressed && right.format != FrameFormat::Uncompressed {
        return left;
    } else if left.format != FrameFormat::Uncompressed && right.format == FrameFormat::Uncompressed
    {
        return right;
    }
    if left.format == FrameFormat::MJPEG && right.format != FrameFormat::MJPEG {
        return left;
    } else if left.format != FrameFormat::MJPEG && right.format == FrameFormat::MJPEG {
        return right;
    }
    // else both have formats we have confirmed work

    if left.width <= 800 && right.width > 800 {
        return left;
    } else if right.width <= 800 && left.width > 800 {
        return right;
    }
    // else both have width <= 800

    if left.width > right.width {
        return left;
    } else if right.width > left.width {
        return right;
    }
    // else same width

    if left.height > right.height {
        return left;
    } else if right.height < left.height {
        return right;
    }
    // else same height

    if left.fps < right.fps {
        return left;
    } else {
        return right;
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

        match to_jpg(rgb_slice, frame.height() as usize, frame.width() as usize) {
            Ok(b) => b,
            Err(e) => {
                error!("Failed to compress frame to JPEG: {:?}", e);
                return;
            }
        }
    };

    let mat = {
        let span = span!(Level::TRACE, "opencv_mat");
        let _enter = span.enter();

        match opencv::prelude::Mat::new_rows_cols_with_bytes::<opencv::core::Vec3b>(
            frame.height() as i32,
            frame.width() as i32,
            rgb_slice,
        ) {
            Ok(m) => m,
            Err(e) => {
                error!("Failed to create opencv Mat from RGB data {}", e);
                return;
            }
        }
    };
    debug_assert_eq!(mat.channels(), 3);

    let (inference_tensor, inference_jpeg): (Arc<[f32]>, Arc<[u8]>) = {
        let span = span!(Level::TRACE, "opencv_inference_input");
        let _enter = span.enter();

        let mat_224_u8 = {
            let span = span!(Level::TRACE, "resize");
            let _enter = span.enter();

            let mut m = match unsafe {
                opencv::prelude::Mat::new_rows_cols(224, 224, opencv::core::CV_8UC3)
            } {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to create opencv u8 Mat for 224x224: {}", e);
                    return;
                }
            };

            if let Err(e) = opencv::imgproc::resize(
                &mat,
                &mut m,
                opencv::core::Size_ {
                    width: 224,
                    height: 224,
                },
                0 as f64,
                0 as f64,
                opencv::imgproc::INTER_AREA,
            ) {
                error!("Failed to resize image to 224x224: {}", e);
                return;
            };
            debug_assert_eq!(m.size().unwrap().width, 224);
            debug_assert_eq!(m.size().unwrap().height, 224);
            debug_assert_eq!(m.channels(), 3);

            m
        };

        let jpeg_224 = {
            let span = span!(Level::TRACE, "jpeg");
            let _enter = span.enter();

            to_jpg(
                mat_224_u8.data_bytes().expect("Bytes from resized image"),
                224usize,
                224usize,
            )
        };

        let mat_224_f32 = {
            let span = span!(Level::TRACE, "u8_to_f32");
            let _enter = span.enter();

            let mut m = match unsafe {
                opencv::prelude::Mat::new_rows_cols(224, 224, opencv::core::CV_32FC3)
            } {
                Ok(m) => m,
                Err(e) => {
                    error!("Failed to create opencv u8 Mat for 224x224: {}", e);
                    return;
                }
            };

            // Convert to a 32-bit float image, scaling values to [0.0, 1.0]
            if let Err(e) = mat_224_u8.convert_to(&mut m, opencv::core::CV_32FC3, 1.0 / 255.0, 0.) {
                error!("Failed to convert to opencv f32 Mat for 224x224: {}", e);
                return;
            };

            debug_assert_eq!(m.size().unwrap().width, 224);
            debug_assert_eq!(m.size().unwrap().height, 224);
            debug_assert_eq!(m.channels(), 3);

            m
        };

        let flat = mat_224_f32.reshape(1, 224).expect("reshape");
        let slice: &[f32] = flat.data_typed::<f32>().unwrap();
        debug_assert_eq!(slice.len(), 224 * 224 * 3);

        (Arc::from(slice), jpeg_224.unwrap())
    };

    let mat = {
        let span = span!(Level::TRACE, "opencv_mat_to_bgr");
        let _enter = span.enter();
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
            inference_tensor,
            inference_jpeg,
            p_hash: p_hash,
        };

        let num_subscribers = match data.tx.send(frame) {
            Ok(n) => n - 1, // Don't count the service's copy of the receiver
            Err(_) => 0,
        };
        info!("Broadcasted a frame to {} subscribers", num_subscribers)
    }
}

fn to_jpg(rgb: &[u8], height: usize, width: usize) -> turbojpeg::Result<Arc<[u8]>> {
    let image = turbojpeg::Image {
        format: turbojpeg::PixelFormat::RGB,
        height,
        width,
        pixels: rgb,
        pitch: (width * 3) as usize,
    };

    let jpeg = turbojpeg::compress(image, 90, turbojpeg::Subsamp::Sub2x2)?;

    let jpeg_slice: &[u8] = &jpeg;

    Ok(Arc::from(jpeg_slice))
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

#[cfg(test)]
mod tests {
    use uvc::StreamFormat;

    use crate::camera::cmp_stream_format;

    #[test]
    fn test_cmp_stream_format_pixel_9a() {
        let a = StreamFormat {
            format: uvc::FrameFormat::MJPEG,
            width: 640,
            height: 360,
            fps: 30,
        };
        let b = StreamFormat {
            format: uvc::FrameFormat::MJPEG,
            width: 640,
            height: 480,
            fps: 30,
        };
        let c = StreamFormat {
            format: uvc::FrameFormat::MJPEG,
            width: 1280,
            height: 720,
            fps: 30,
        };
        let d = StreamFormat {
            format: uvc::FrameFormat::MJPEG,
            width: 1920,
            height: 1080,
            fps: 30,
        };

        // b beats a because width is larger
        let ab = cmp_stream_format(a, b);
        assert_eq!(ab.height, 480);

        // b beats c because width is <= 800
        let bc = cmp_stream_format(b, c);
        assert_eq!(bc.width, 640);

        // b beats d because width is <= 800
        let bd = cmp_stream_format(b, d);
        assert_eq!(bd.width, 640);
    }
}
