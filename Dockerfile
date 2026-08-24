# Multi-stage: static musl binary on scratch-like minimal base
FROM rust:1-alpine AS builder
RUN apk add --no-cache musl-dev perl make
WORKDIR /build
COPY . .
ENV CARGO_NET_RETRY=10
RUN cargo build --release -p pg2osync

FROM alpine:3.20
RUN adduser -D -u 10001 pg2osync
COPY --from=builder /build/target/release/pg2osync /usr/local/bin/pg2osync
USER pg2osync
ENTRYPOINT ["pg2osync"]
