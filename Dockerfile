FROM rust:1.74.0-bullseye
WORKDIR /usr/src/raft 

COPY . .
RUN cargo build -p raft-main
CMD ["cargo", "run", "-p", "raft-main"]
