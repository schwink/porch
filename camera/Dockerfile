FROM rust:latest AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY vendor ./vendor

RUN apt update
RUN apt install -y clang cmake

RUN cargo build --release

FROM debian:stable-slim

RUN apt update
RUN apt install -y curl
RUN rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/porch-camera ./porch-camera

CMD ["./porch-camera", "/var/porch/images"]