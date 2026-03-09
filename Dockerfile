FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

# Только базовые тулзы для сборки (никакого jemalloc)
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    curl \
    zip \
    unzip \
    tar \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Устанавливаем vcpkg глобально в /opt
WORKDIR /opt
RUN git clone https://github.com/microsoft/vcpkg.git \
    && ./vcpkg/bootstrap-vcpkg.sh -disableMetrics

# Добавляем vcpkg в переменные окружения
ENV VCPKG_ROOT=/opt/vcpkg
ENV CMAKE_TOOLCHAIN_FILE=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /app
CMD ["/bin/bash"]
