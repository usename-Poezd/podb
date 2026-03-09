# Конфигурация
BUILD_DIR := build
PROJECT_NAME := db_engine
BUILD_TYPE := Release

# Авто-определение VCPKG_ROOT если не задан
ifndef VCPKG_ROOT
  ifneq ($(wildcard /opt/vcpkg/scripts/buildsystems/vcpkg.cmake),)
    VCPKG_ROOT := /opt/vcpkg
  else
    $(warning VCPKG_ROOT не установлена и vcpkg не найден в /opt/vcpkg.)
  endif
endif

VCPKG_TOOLCHAIN := $(VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake

# Цели, которые не являются файлами
.PHONY: all configure build clean docker-build docker-run

# Цель по умолчанию
all: build

# 1. Локальная конфигурация CMake через vcpkg
configure:
	@echo "==> Конфигурация CMake (Type: $(BUILD_TYPE))..."
	cmake -B $(BUILD_DIR) -S . \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_TOOLCHAIN) \
		-DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON
	@ln -sf $(BUILD_DIR)/compile_commands.json compile_commands.json

# 2. Локальная сборка проекта (параллельная, на всех ядрах)
build:
	@if [ ! -f $(BUILD_DIR)/CMakeCache.txt ]; then $(MAKE) configure; fi
	@echo "==> Сборка проекта..."
	cmake --build $(BUILD_DIR) -j $$(nproc) -- --no-print-directory

# 3. Очистка локальных артефактов
clean:
	@echo "==> Удаление директории $(BUILD_DIR)..."
	rm -rf $(BUILD_DIR)

# 4. Сборка базового Docker-образа (окружения)
docker-build:
	@echo "==> Сборка Docker-образа $(PROJECT_NAME)_env..."
	docker build -t $(PROJECT_NAME)_env .

# 5. Запуск Docker-контейнера для разработки
# Флаг --privileged нужен для корректной работы pthread_setaffinity_np (CPU Affinity)
# Флаг -v монтирует текущую папку внутрь контейнера
docker-run:
	@echo "==> Запуск контейнера..."
	docker run --rm -it \
		--privileged \
		-v $(PWD):/app \
		$(PROJECT_NAME)_env

proto: configure
	@echo "==> Запуск генерации .proto файлов..."
	cmake --build $(BUILD_DIR) --target generate_proto
	@echo "==> Файлы успешно сгенерированы в src/ !"
