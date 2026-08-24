# Build a static musl binary so the runtime image needs no libc at all.
FROM rust:1.90-alpine AS builder
RUN apk add --no-cache musl-dev perl make cmake g++
WORKDIR /build

# Dependencies change far less often than sources: copying manifests first
# keeps the dependency layer cached across code-only rebuilds.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml crates/core/
COPY crates/source/Cargo.toml crates/source/
COPY crates/source-mysql/Cargo.toml crates/source-mysql/
COPY crates/sink/Cargo.toml crates/sink/
COPY crates/engine/Cargo.toml crates/engine/
COPY crates/bin/Cargo.toml crates/bin/
RUN mkdir -p crates/core/src crates/source/src crates/source-mysql/src \
             crates/sink/src crates/engine/src crates/bin/src \
    && echo "" > crates/core/src/lib.rs \
    && echo "" > crates/source/src/lib.rs \
    && echo "" > crates/source-mysql/src/lib.rs \
    && echo "" > crates/sink/src/lib.rs \
    && echo "" > crates/engine/src/lib.rs \
    && echo "fn main() {}" > crates/bin/src/main.rs \
    && cargo build --release --locked -p pg2osync \
    && rm -rf crates/*/src

COPY crates crates
# touch so cargo rebuilds the crates themselves, not their dependencies
RUN find crates -name '*.rs' -exec touch {} + \
    && cargo build --release --locked -p pg2osync \
    && strip target/release/pg2osync

FROM alpine:3.24
RUN apk add --no-cache ca-certificates tzdata \
    && adduser -D -u 10001 pg2osync
COPY LICENSE /usr/share/licenses/pg2osync/LICENSE
COPY --from=builder /build/target/release/pg2osync /usr/local/bin/pg2osync

LABEL org.opencontainers.image.title="pg2osync" \
      org.opencontainers.image.description="Real-time PostgreSQL/MySQL to OpenSearch, Elasticsearch and Meilisearch sync" \
      org.opencontainers.image.source="https://github.com/kennywillbe/pg2osync" \
      org.opencontainers.image.licenses="Apache-2.0"

USER 10001:10001
# the metrics endpoint doubles as the liveness probe target
EXPOSE 9100
ENTRYPOINT ["pg2osync"]
CMD ["run", "-c", "/etc/pg2osync/pg2osync.toml"]
