# Until distroless updates glibc to bookworm, with glibc 2.33, we need to use
# bullseye. See: https://github.com/GoogleContainerTools/distroless/issues/1337
FROM rust:1-slim-bullseye as builder
WORKDIR /app

# Build deps
COPY Cargo.toml Cargo.lock ./
RUN mkdir src/
RUN echo "fn main() {}" > dummy.rs
RUN sed -i 's#src/main.rs#dummy.rs#' Cargo.toml
RUN cargo build --release --bin payload-archiver

# Build main executable
RUN sed -i 's#dummy.rs#src/main.rs#' Cargo.toml
COPY src ./src
RUN cargo build --release --bin payload-archiver

FROM gcr.io/distroless/cc AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/payload-archiver /payload-archiver
ENTRYPOINT ["/payload-archiver"]
