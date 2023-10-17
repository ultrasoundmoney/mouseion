FROM rust as builder
WORKDIR /app

# Build deps. We replace our main.rs with a dummy.rs to avoid rebuilding the
# main executable, creating a cached layer for the dependencies.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src/
RUN echo "fn main() {}" > dummy.rs
RUN sed -i 's#src/bin/archive-submissions.rs#dummy.rs#' Cargo.toml
RUN cargo build --release

# Build executable.
RUN sed -i 's#dummy.rs#src/bin/archive-submissions.rs#' Cargo.toml
COPY src ./src
RUN cargo build --release --bin archive-submissions

# Build runtime image.
FROM gcr.io/distroless/cc-debian12 AS runtime
WORKDIR /app
COPY --from=builder /app/target/release/archive-submissions /app/archive-submissions
ENTRYPOINT ["/app/archive-submissions"]
