# Build a static musl binary so the runtime image needs no libc at all.
FROM rust:1.98-alpine AS builder
RUN apk add --no-cache musl-dev perl make cmake g++
WORKDIR /build

# Dependencies change far less often than sources: copying manifests first
# keeps the dependency layer cached across code-only rebuilds.
COPY Cargo.toml Cargo.lock ./
# One line per workspace member: a member missing here fails the stub build
# below with "failed to load manifest for workspace member", which is exactly
# how adding crates/tls broke this image without anyone noticing.
COPY crates/core/Cargo.toml crates/core/
COPY crates/tls/Cargo.toml crates/tls/
COPY crates/source/Cargo.toml crates/source/
COPY crates/source-mysql/Cargo.toml crates/source-mysql/
COPY crates/sink/Cargo.toml crates/sink/
COPY crates/engine/Cargo.toml crates/engine/
COPY crates/bin/Cargo.toml crates/bin/
# BuildKit cache mounts survive the layer cache: when a Cargo.lock change
# invalidates this layer, the dependencies already compiled are still in the
# mount and only what actually changed is rebuilt.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    mkdir -p crates/core/src crates/tls/src crates/source/src crates/source-mysql/src \
             crates/sink/src crates/engine/src crates/bin/src \
    && echo "" > crates/core/src/lib.rs \
    && echo "" > crates/tls/src/lib.rs \
    && echo "" > crates/source/src/lib.rs \
    && echo "" > crates/source-mysql/src/lib.rs \
    && echo "" > crates/sink/src/lib.rs \
    && echo "" > crates/engine/src/lib.rs \
    && echo "fn main() {}" > crates/bin/src/main.rs \
    && cargo build --release --locked -p pg2osync \
    && rm -rf crates/*/src

COPY crates crates
# touch so cargo rebuilds the crates themselves, not their dependencies. The
# final copy is what makes the binary survive: target/ is a cache mount, so it
# is gone the moment the RUN ends and only /build/pg2osync reaches a layer the
# runtime stage can copy from.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p pg2osync \
    && strip target/release/pg2osync \
    && cp target/release/pg2osync /build/pg2osync

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 pg2osync
COPY LICENSE /usr/share/licenses/pg2osync/LICENSE
COPY --from=builder /build/pg2osync /usr/local/bin/pg2osync

LABEL org.opencontainers.image.title="pg2osync" \
      org.opencontainers.image.description="Real-time PostgreSQL/MySQL to OpenSearch, Elasticsearch and Meilisearch sync" \
      org.opencontainers.image.source="https://github.com/kennywillbe/pg2osync" \
      org.opencontainers.image.licenses="Apache-2.0"

USER 10001:10001
# the metrics endpoint doubles as the liveness probe target
EXPOSE 9100
ENTRYPOINT ["pg2osync"]
CMD ["run", "-c", "/etc/pg2osync/pg2osync.toml"]
