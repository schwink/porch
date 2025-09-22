fn main() {
    println!("Hello, world!");

    let uvc_context = uvc::Context::new().expect("Could not get uvc context");

    let uvc_devices = uvc_context
        .devices()
        .expect("Could not enumerate uvc devices");
    uvc_devices.for_each(|device| {
        let desc = device
            .description()
            .expect("Could not get device descriptor");
        println!(
            "UVC Device: Vendor 0x{:04x} ({}), Product ID 0x{:04x} ({}), Serial Number: {}",
            desc.vendor_id,
            desc.manufacturer.unwrap_or("unknown".to_string()),
            desc.product_id,
            desc.product.unwrap_or("unknown".to_string()),
            desc.serial_number.unwrap_or("unknown".to_string()),
        );
    });
}
