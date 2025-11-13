---
layout: page
title: About
permalink: /about/
---

Porch is my deep learning image recognition project for the [Practical Deep Learning for Coders](https://fast.ai) course.

You can find the source code on GitHub at [schwink/porch](https://github.com/schwink/porch).

It has the form of a Rust application running on a cheap ARM single board computer in my hallway, capturing images via a ten-year-old USB webcam. I periodically label the images via a web UI, and occasionally sync them to my laptop to train models. Models can be exported to onnx format and inference run on the device.
