FROM rust:1.88.0-bookworm AS builder

WORKDIR /source
COPY . .
RUN cargo build --locked --release -p impossible-inferences-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libgomp1 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 10001 impossible \
    && mkdir /data \
    && chown impossible:impossible /data

COPY --from=builder /source/target/release/impossible-inferences-server /usr/local/bin/impossible-inferences
COPY LICENSE-APACHE LICENSE-MIT NOTICE.md THIRD_PARTY_NOTICES.md THIRD_PARTY_LICENSES.txt /licenses/

USER impossible
VOLUME ["/data"]
ENTRYPOINT ["/usr/local/bin/impossible-inferences"]
CMD ["serve", "--artifact-root", "/data"]
