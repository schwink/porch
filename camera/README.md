

## Build notes

### uvc

It's necessary to build uvc from master, due to some out of date yanked dependencies.

In the project root directory,
`git submodule update --init --recursive`


## Architecture

### Camera

The camera is a shared resource that broadcasts frames at the statically-configured resolution and framerate, as long as somebody is listening. When unused, it shuts down the device.

### Scheduler

The scheduler sleeps until the next wakeup time. Then it gets the camera and receives frames, selecting one closest to the desired time, and saves the frame to disk.

The scheduler then calculates the next capture time, potentially calibrating itself against the system clock. If the next capture time is imminent, it keeps the camera open, otherwise it closes it. The scheduler then loops, sleeping until the desired time.

### Webserver

#### /live.mjpeg

While a request to this resource it open, the webserver keeps the camera awake and transmits its frames to the client.
