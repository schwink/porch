use rusb::{LibraryVersion, version};

fn main() {
    println!("Hello, world!");

    let rusb_version = rusb::version();
    println!(
        "rusb version: {}.{}",
        rusb_version.major(),
        rusb_version.minor(),
    );

    let devices = rusb::devices();
    match devices {
        Ok(devices) => {
            println!("Found {} devices", devices.len());
            for device in devices.iter() {
                let device_desc = device.device_descriptor();
                match device_desc {
                    Ok(desc) => {
                        println!(
                            "{:?}\nBus {:03} Device {:03} ID {:04x}:{:04x}\n",
                            desc,
                            device.bus_number(),
                            device.address(),
                            desc.vendor_id(),
                            desc.product_id()
                        );
                    }
                    Err(e) => {
                        eprintln!("Error getting device descriptor: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            eprintln!("Error getting device list: {}", e);
        }
    }
}
