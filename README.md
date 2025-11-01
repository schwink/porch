# Webcam application for deep learning

This is my image processing project for the [Practical Deep Learning for Coders](https://course.fast.ai) course.

It consists of two parts:
- A native Rust executable that
  - Schedules frame capture times
  - Uses `libuvc` to pull images from a USB camera
  - Displays the images in a web UI for labeling
  - Uses `onnxruntime` to perform inference on the incoming frames
- Training and model verification documented in Jupyter

It's a work in progress! If you happen to read it, I am grateful for any feedback, either via email or GitHub issues.
