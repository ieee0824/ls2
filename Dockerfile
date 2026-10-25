FROM rust:1-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN groupadd --gid 10001 ls2 \
    && useradd --uid 10001 --gid ls2 --no-create-home --shell /usr/sbin/nologin ls2 \
    && mkdir /data \
    && chown ls2:ls2 /data

COPY --from=builder /app/target/release/ls2 /usr/local/bin/ls2

# The container is reached through a published port, so ls2 binds every interface.
# There is no authentication: publish the port on loopback (`-p 127.0.0.1:9326:9326`).
ENV LS2_BIND_ADDR=0.0.0.0:9326 \
    LS2_ALLOW_UNAUTHENTICATED_REMOTE=true \
    LS2_DATABASE_PATH=/data/ls2.sqlite

USER ls2
VOLUME ["/data"]
EXPOSE 9326
CMD ["ls2"]
