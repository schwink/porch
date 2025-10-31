

## Build notes

### uvc

It's necessary to build uvc from master, due to some out of date yanked dependencies.

In the project root directory,
`git submodule update --init --recursive`

### opencv

`brew install opencv`

The current version in Homebrew is `v4.12`, while `v4.10` is available in `debian:stable-slim` in our Dockerfile. Unfortunately, `v4.11` changed the signature of `opencv::imgproc::cvt_color`.

In the meantime, I am just commenting out the AlgoHint parameter when building for Docker.

### libtorch

The tch version is pinned to v0.19 instead of latest version v0.22 because our Debian docker image only has libtorch2.6.

For Mac, unfortunately libtorch v2.9 is what's available in homebrew. However, tch v0.19 seems to build fine against libtorch 2.9, though current tch v0.22 does not build against libtorch v2.6. Anyway, the dream of compiling for both platforms without manual setup lives on for now.

```
brew install pytorch

export LIBTORCH=/Users/schwink/homebrew/Cellar/pytorch/2.9.0_1/
```

## Architecture

### Camera

The camera is a shared resource that broadcasts frames at the statically-configured resolution and framerate, as long as somebody is listening. When unused, it shuts down the device.

### Scheduler

The scheduler sleeps until the next wakeup time. Then it gets the camera and receives frames, selecting one closest to the desired time, and saves the frame to disk.

The scheduler then calculates the next capture time, potentially calibrating itself against the system clock. If the next capture time is imminent, it keeps the camera open, otherwise it closes it. The scheduler then loops, sleeping until the desired time.

### Webserver

#### /live.mjpeg

While a request to this resource it open, the webserver keeps the camera awake and transmits its frames to the client.

## Deployment

An arm64 Docker image running on an OrangePi.

The image takes forever to build as a GitHub action. To build and publish it locally:
```
docker build . -t ghcr.io/schwink/porch-camera:local --no-cache
docker push ghcr.io/schwink/porch-camera:local
```

