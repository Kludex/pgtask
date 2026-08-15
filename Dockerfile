FROM rust:1.94-bookworm AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --locked --release --bin pgtask --bin pgtask-bench --bin pgtask-smoke --bin pgtask-web

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/pgtask /usr/local/bin/pgtask
COPY --from=builder /build/target/release/pgtask-bench /usr/local/bin/pgtask-bench
COPY --from=builder /build/target/release/pgtask-smoke /usr/local/bin/pgtask-smoke
COPY --from=builder /build/target/release/pgtask-web /usr/local/bin/pgtask-web
USER 65532:65532
CMD ["pgtask", "health"]
