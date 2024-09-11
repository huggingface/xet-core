FROM rust:1.58 as builder

RUN USER=root rustup component add rustfmt
RUN USER=root cargo new --bin cas_client

WORKDIR ./cas_client
ADD ./utils ../utils
COPY ./cas_client/Cargo.toml ./Cargo.toml
RUN cargo build --release
RUN rm src/*.rs

COPY ./cas_client .
RUN rm ./target/release/deps/cas_client*

RUN cargo build --release

FROM debian:buster-slim
ARG APP=/usr/src/app

RUN apt-get update \
    && apt-get install -y ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Etc/UTC \
    APP_USER=appuser

RUN groupadd $APP_USER \
    && useradd -g $APP_USER $APP_USER \
    && mkdir -p ${APP}

COPY --from=builder /cas_client/target/release/cas_client ${APP}/cas_client
RUN mkdir ${APP}/config

RUN chown -R $APP_USER:$APP_USER ${APP}

USER $APP_USER
WORKDIR ${APP}
