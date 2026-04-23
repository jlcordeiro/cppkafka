docker build -t cppkafka-dev .

docker run -it \
  --network kafka-net \
  -u $(id -u):$(id -g) \
  -v $(pwd):/app \
  -e KAFKA_DEBUG=all \
  cppkafka-dev \
  bash -c "
    cd /app &&
    rm -rf build &&
    mkdir build &&
    cd build &&
    cmake .. \
      -DCMAKE_CXX_STANDARD=20 \
      -DKAFKA_TEST_INSTANCE=kafka:9092 && \
    make -j$(nproc) &&
    make examples &&
    ctest -V

  "
