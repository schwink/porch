---
layout: post
title:  "Inference on device"
date:   2025-11-01 19:55:42 -0800
tags: fastai pytorch onnx onnxruntime
---
This project began as an exercise for the [Practical Deep Learning for Coders](https://fast.ai) course, which uses some Python `fastai` wrappers around PyTorch.

This made it very quick and easy to see results in Jupyter, but we all know that the real pros [do inference in C](https://www.youtube.com/watch?v=xyrgkui0uCA). So I set out to do inference on device.

## Exporting the model

After some online searching (the `fastai` source itself being difficult to decipher), I determined that the `fastai` `vision_learner`'s `model` property is in fact a PyTorch model, and PyTorch models don't directly run in other environments.

Further research turned up the ONNX portable model format, developed by the PyTorch project for this purpose.

[Exporting to onnx was straightforward](https://github.com/onnx/tutorials/blob/main/tutorials/PytorchOnnxExport.ipynb), though it did produce deprecation warnings. Sadly, setting `dynamo=True` per the recommendation caused inscrutable errors, likely due to some unsupported operation in the fastai model architecture.

## Tensor format

To the neural network, each image is just a sequence of numbers, which in ML-speak is called a "tensor".

Representing images as bytes is nothing new. There are many ways to do it. Frustratingly for me, the exact transformation used by `fastai` was not explicitly documented anywhere I could find, so it took several iterations of trial and error and searching online before I was able to understand what was going on and replicate it with `opencv`. I suspect this is an area where tribal knowledge is significant among real ML experts.

Tensors have dimensions. We can use a program called [Netron](https://netron.app) to view the properties of our `.onnx` file, which reveals that our input tensor is a four-dimensional array: `float32[1,3,224,224]`.

![ONNX properties viewed in Netron]({{site.baseurl}}/static/netron_gate_openness.jpg)

I don't know the purpose of the size 1 first dimension (batching?), but the `3` represents the RGB color space, and the `224` are the width and height of the images. So the tensor is arranged as a series of three-tuples of `float32` RGB values between `0.0` and `1.1`, multipled by `224` columns, multiplied by `224` rows.

### Python

I walk through this in detail in [the Jupityr notebook](https://github.com/schwink/porch/blob/main/training/gate_openness_classifier.ipynb).

The first thing to know is the image dimensions, which `fastai` is explicit about. In our training we "squash" everything to 122px square images.

The next thing to find out is the color space. Training uses the Python "Pillow" image library, which is how we know it's RGB. It would be fine to use a differnet color space for the model, as long as training and inference are coordinated; there is nothing special about RGB.

Finally comes conversion to a tensor of `float32`, which the [`torchvision` `transforms.ToTensor`](https://docs.pytorch.org/vision/main/generated/torchvision.transforms.ToTensor.html) function does a good job documenting. The familiar (Height x Width x Color) layout of `int8` color values is for some reason rearranged to (Color x Height x Width) and converted to `float32`, matching the `float32[1,3,224,224]` tensor format we saw earlier.

Now that we understand what is going on in Python, how do we do it for incoming frames inside the camera application?

### Rust

It took quite a bit of trial and error, leavened with search results from Stack Overflow and Reddit, to get the same process working on device.

![Trace of image processing operations within a single frame callback]({{site.baseurl}}/static/frame_callback_icicle.png)

Within each `libuvc` callback, we are currently doing the following work:
* Using `libuvc` to normalize the frame to BGR format, which `opencv2` expects, and loading it into a `mat`
* Using `opencv2` to convert it to a JPEG
* [Creating the tensor](https://github.com/schwink/porch/blob/main/camera/src/camera.rs#L448)
  * Resizing the image down to 224x224, the single most expensive operation
  * Saving a small JPEG of the resized image, a debugging affordance I added early in the process
  * Converting the small image back to RGB
  * Calling `libtorch` to replicate the `torchivision` transform described above in the Python section, producing the `float32` tensor

Fortunately all of this only takes about 30ms on device, comfortably within the 200ms limit for the 5 FPS stream we have selected.

![Trace of multiple frame callbacks while the camera is active]({{site.baseurl}}/static/frame_callback_tracing.png)

## Inference

With the tensor available, running the model with `ort` was fairly straightforward.

I reduced inference time by reusing the `ort` session across runs, which seems like an obvious best practice and is recommended in the documentation. I don't understand why the session needs to be `mut`, nor does the [recent commit from earlier this year making that change](https://github.com/pykeio/ort/commit/bd2aff711ec364db8d41be6a3c4690e5feaf36b4#diff-af36d5fa0ee7f11fecf4482ebfbe7a43c8eeb42769bdcb9f94d8f87e1d5afaf6) give any justification, but putting it inside an uncontested mutex gets it to work easily enough.

And it works! The screenshot below depicts it correctly classifying the gate as 2% likely to be closed and 98% likely to be open, based on a model trained a couple weeks earlier.

![Inference added to the labeling UI]({{site.baseurl}}/static/labeling_ui_with_inference.jpg)

