# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:1.50.0@sha256:d327c18396a9c468a7fdf9f43224ca06164b7ee86f1b5f88fed8c9ef89a23d8b as cargo-build

RUN apt-get update

#RUN apt-get install musl-tools -y
RUN apt-get install

#RUN rustup target add x86_64-unknown-linux-musl

WORKDIR /app

COPY Cargo.toml Cargo.toml

RUN mkdir src/

RUN echo "fn main() {println!(\"if you see this, the build broke\")}" > src/main.rs

#RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl
RUN cargo build --release

RUN rm -f target/release/deps/tmu*

COPY ./src src/

RUN cargo build --release
#RUN RUSTFLAGS=-Clinker=musl-gcc cargo build --release --target=x86_64-unknown-linux-musl

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM debian:stable-slim@sha256:fd01b9ba8de6559c3848da4adc8a258541c4433b3a49713cffc200743e84335e
#
#RUN addgroup -g 1000 myapp
#
#RUN adduser -D -s /bin/sh -u 1000 -G myapp myapp
#
#WORKDIR /app
#
COPY --from=cargo-build /app/target/release/tmu /usr/local/bin
#
#RUN chown myapp:myapp myapp
#
#USER myapp
#
ENTRYPOINT ["tmu"]