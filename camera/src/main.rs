use std::time::Duration;

mod camera;

#[tokio::main]
async fn main() {
    let uvc_context = uvc::Context::new().expect("Could not get uvc context");

    let uvc_devices = uvc_context
        .devices()
        .expect("Could not enumerate uvc devices");
    uvc_devices.for_each(|device| {
        let desc = device
            .description()
            .expect("Could not get device descriptor");
        println!(
            "UVC Device: Bus {:03}, Device Address {:03}, Vendor 0x{:04x} ({}), Product ID 0x{:04x} ({}), Serial Number: {}",
            device.bus_number(),
            device.device_address(),
            desc.vendor_id,
            desc.manufacturer.unwrap_or("Unknown".to_string()),
            desc.product_id,
            desc.product.unwrap_or("Unknown".to_string()),
            desc.serial_number.unwrap_or("Unknown".to_string()),
        );

        let handle = device.open().expect("Could not open device");

        let formats = handle.supported_formats();
        formats.for_each(|format| {
            println!(
                "Format: subtype:{:?}",
                    format.subtype(),
            );
            let frame_formats = format.supported_formats();
            frame_formats.for_each(|frame_format| {
                println!(
                    "Frame Format: w:{} h:{} fps:{:?}",
                        frame_format.width(),
                        frame_format.height(),
                        frame_format.intervals().into_iter().map(|d| 10_000_000 / d).collect::<Vec<u32>>()
                );
            });
        });
    });

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
