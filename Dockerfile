FROM rust
COPY . .


RUN cargo install --path .

cmd ["chalk-rust"]
