---
layout: post
title:  "Image capture and labeling"
date:   2025-09-22 19:55:42 -0800
tags: rust opencv2 tokio axum react
---
The camera application is a Rust executable. It runs in Docker, with a USB device (the camera) and a storage volume (on a mounted USB memory stick) configured via Docker Compose.

## Frame flow

The application wakes up on a fixed schedule. It acquires a camera stream with `libuvc`, selecting the largest image size <= 600 x 800 pixels at the lowest available frame rate.

For each captured frame, `libuvc` invokes a callback on a dedicated thread. The callback loads the image into an `opencv2` mat, converting it into the BGR color space expected by opencv. At this point we perform any image processing work that will be required downstream:
* Taking a hash of the image to detect whether the scene has changed
* Encoding to jpeg
* Converting it to a tensor for inference (more on that later)

### Webcam image formats

Cameras offer various formats, resolutions, and frame rates, as well as formats and color spaces. When starting a stream, it is necessary to enumerate the options and select the desired stream.

I had the opportunity to test this with two cameras: The Logitec c310 I am using in production, and my Google Pixel 9a which I sometimes paired with my laptop for testing. The MacBook's built-in camera is in a secure enclave that prevents it being accessed as a USB device.

For example, this is what the options look like. ([USB vendor IDs available here](http://www.linux-usb.org/usb.ids)).

<details markdown="1">
  <summary><strong>USB Vendor 0x046d (Logitech Inc.), Product ID 0x081b (Webcam C310)</strong></summary>

| Format subtype | Dimensions | FPS                     |
| -------------- | ---------- | ----------------------- |
| Uncompressed   | 160 x 120  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 176 x 144  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 320 x 176  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 320 x 240  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 352 x 288  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 432 x 240  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 544 x 288  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 640 x 360  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 640 x 480  | [30, 25, 20, 15, 10, 5] |
| Uncompressed   | 752 x 416  | [25, 20, 15, 10, 5]     |
| Uncompressed   | 800 x 448  | [25, 20, 15, 10, 5]     |
| Uncompressed   | 800 x 600  | [20, 15, 10, 5]         |
| Uncompressed   | 864 x 480  | [20, 15, 10, 5]         |
| Uncompressed   | 960 x 544  | [15, 10, 5]             |
| Uncompressed   | 960 x 720  | [10, 5]                 |
| Uncompressed   | 1024 x 576 | [10, 5]                 |
| Uncompressed   | 1184 x 656 | [10, 5]                 |
| Uncompressed   | 1280 x 720 | [10, 5]                 |
| Uncompressed   | 1280 x 960 | [7, 5]                  |
| MJPEG          | 160 x 120  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 176 x 144  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 320 x 176  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 320 x 240  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 352 x 288  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 432 x 240  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 544 x 288  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 640 x 360  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 640 x 480  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 752 x 416  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 800 x 448  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 800 x 600  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 864 x 480  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 960 x 544  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 960 x 720  | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 1024 x 576 | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 1184 x 656 | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 1280 x 720 | [30, 25, 20, 15, 10, 5] |
| MJPEG          | 1280 x 960 | [30, 25, 20, 15, 10, 5] |

</details><br/>

### MJPEG

The MJPEG format means that each frame is compressed into a JPEG, presumably via a hardware encoder on the device. It was apparently a popular early format for streaming live video over the web, basically just sending a sequence of JPEGs in a `multipart/x-mixed-replace` response, but lives on in these webcams; it is the only format supported by the Pixel 9a.

I [set up an MJPEG stream in an early version of the Porch web UI, to help position the camera, before taking it out when I implemented the labeling UI](https://github.com/schwink/porch/blob/main/camera/src/webserver.rs#L90).

The JPEGs I got from the C310 had the annoying limitation of not being viewable in Safari or Preview. Examining the buffers with a hex editor, I found that they started with `FF D8 FF E0`, which suggests the earlier JPEG [JFIF](https://en.wikipedia.org/wiki/JPEG_File_Interchange_Format#File_format_structure) container format, or maybe a "raw" JPEG with no metadata, rather than the more familiar EXIF format which starts with `FF D8 FF EE`.

In light of this, I opted to re-encode the incoming frames rather than passing them through.

## Labeling

To actually use the captured images for machine learning, it is necessary to label them.

I ultimately want to detect specific members of my family in frame, but as a first application I settled for detecting whether our baby gate is open or closed, which is a strong proxy for whether we have left the house to take my daugher to school.

### Serving a web site from the camera application

For training with PyTorch, I ultimately needed a directory full of JPG images and some kind of corresponding label metadata associated with them. By far the easiest option was to create a simple web application that saved the labels as JSON files alongside the source images.

This project was as much an excuse for me to learn Rust as to learn ML. I used the axum web framework to expose some simple REST endpoints to list frames and set labels on each.

I went with a simple React frontend [defined inline without the build step](https://dev.to/dperrymorrow/using-react-without-jsx-no-build-14gg), which is a pattern I have reached for in the past for personal projects. I love developing for the web and often enjoy writing JavaScript, but I have never not gotten annoyed trying to use the broader JS ecosystem and tooling for anything non-trivial.

### User interface

The ideal user interface would enable me to quickly scroll through the images for the day, either labeling or deleting each.

For label input I used screen-width buttons with [distinct colors hashed from the label names](https://stackoverflow.com/questions/3426404/create-a-hexadecimal-colour-based-on-a-string-with-javascript) to help visually differentiate them.

![The labeling UI]({{site.baseurl}}/static/labeling_ui.jpg)
