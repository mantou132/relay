# syntax=docker/dockerfile:1

FROM rust:1.94-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY --from=builder /app/target/release/relay /usr/local/bin/relay

ENV RELAY_BIND=0.0.0.0:39371 \
    RELAY_DATABASE=/data/relay.sqlite3

VOLUME ["/data"]
EXPOSE 39371

ENTRYPOINT ["relay"]
