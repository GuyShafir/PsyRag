# ---- build stage ----
FROM rust:slim AS build
WORKDIR /src
COPY Cargo.toml ./
COPY psyrag-graph ./psyrag-graph
COPY psyrag-core ./psyrag-core
COPY psyrag ./psyrag
# build the release binary (minimal deps: serde/serde_json/tiny_http)
RUN cargo build --release -p psyrag && cp target/release/psyrag /usr/local/bin/psyrag

# ---- runtime stage ----
FROM debian:stable-slim
RUN useradd -m psyrag && mkdir -p /data && chown psyrag /data
COPY --from=build /usr/local/bin/psyrag /usr/local/bin/psyrag
USER psyrag
WORKDIR /data
EXPOSE 8080
# WAL + sidecar + durable traces persist under /data (mount a volume to keep them)
ENV PSYRAG_WAL=/data/psyrag.wal
ENTRYPOINT ["psyrag"]
CMD ["--wal", "/data/psyrag.wal", "serve", "--addr", "0.0.0.0:8080"]
