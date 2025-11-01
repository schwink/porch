## Build notes

I do local development and testing on my Mac, and periodically build a Docker image to deploy to the device.

### uvc

It's necessary to vendor uvc and build it from a more recent revision than its latest tagged release, due to some out of date yanked dependencies.

In the project root directory,
`git submodule update --init --recursive`

### opencv

We link dynamically against opencv.

`brew install opencv`

The current version in Homebrew is `v4.12`, while `v4.10` is available in `debian:stable-slim` in our Dockerfile. Unfortunately, `v4.11` changed the signature of `opencv::imgproc::cvt_color`.

In the meantime, I am just commenting out the AlgoHint parameter when building for Docker.

### libtorch

We link dynamically against libtorch.

The tch version is pinned to v0.19 instead of latest version v0.22 because our base Debian docker image only has libtorch2.6.

For Mac, unfortunately libtorch v2.9 is what's currently available in homebrew. However, tch v0.19 seems to build fine against libtorch 2.9, though current tch v0.22 does not build against libtorch v2.6. Anyway, the dream of compiling for both platforms without manual setup lives on for now.

```
brew install pytorch

export LIBTORCH=/Users/schwink/homebrew/Cellar/pytorch/2.9.0_1/
```

## Deployment

The program executes as an arm64 Docker image running on an OrangePi.

The image takes forever to build as a GitHub action, so I typically build and publish it locally from my Mac.
```
docker build . -t ghcr.io/schwink/porch-camera:local
docker push ghcr.io/schwink/porch-camera:local
```

