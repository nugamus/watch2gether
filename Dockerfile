# syntax=docker/dockerfile:1
FROM rust:1-trixie AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY static static
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked && cp target/release/watch2gether /usr/local/bin/

FROM debian:trixie-slim
# ffmpeg, plus Intel's VAAPI driver (non-free) so transcodes can use Quick Sync when /dev/dri is passed in.
RUN sed -i 's/^Components: main$/Components: main contrib non-free non-free-firmware/' /etc/apt/sources.list.d/debian.sources \
 && apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg intel-media-va-driver-non-free ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /usr/local/bin/watch2gether /usr/local/bin/
ENV LIBVA_DRIVER_NAME=iHD
EXPOSE 8080
CMD ["watch2gether", "/videos", "--cache-dir", "/cache", "--data-dir", "/data"]
