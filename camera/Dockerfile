FROM rust:latest AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY vendor ./vendor

RUN apt update
RUN apt install -y clang cmake libopencv-dev

RUN cargo build --release

FROM debian:stable-slim

RUN apt update
RUN apt install -y curl libopencv-contrib410
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/porch-camera ./porch-camera
COPY var ./var

EXPOSE 8080

CMD ["./porch-camera", "/var/porch/images", "--log-dir", "/var/porch/logs", "--labeling-config", "/etc/porch/labeling-config.json", "--inference-config", "/etc/porch/inference-config.json"]