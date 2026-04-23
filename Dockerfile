FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    git \
    librdkafka-dev \
    libssl-dev \
    zlib1g-dev \
    libboost-all-dev \
    && rm -rf /var/lib/apt/lists/*

# Default working dir (will be overridden by mount)
WORKDIR /app

CMD ["/bin/bash"]
