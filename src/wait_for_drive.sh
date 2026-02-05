#!/bin/sh
# wait_for_drive.sh
# Скрипт затримує запуск контейнера, доки не з'являться файли у змонтованій папці.

# Шлях до папки (беремо з ENV або дефолтний)
TARGET_DIR="${INTEGRATOR_MOUNT_PATH:-/mnt/drive}"

echo "[Init] Waiting for mount at: $TARGET_DIR"

# Перевірка: чи існує папка
if [ ! -d "$TARGET_DIR" ]; then
    echo "[Error] Directory $TARGET_DIR does not exist inside container!"
    exit 1
fi

# Цикл очікування (поки папка пуста)
# ls -A повертає список файлів (включно з прихованими). Якщо пуста - рядок пустий.
while [ -z "$(ls -A "$TARGET_DIR")" ]; do
    echo "[Init] $TARGET_DIR is empty. Waiting for Google Drive mount... (sleeping 5s)"
    sleep 5
done

echo "[Init] Mount detected! Files found."
echo "[Init] Starting KDV Integrator..."

# Виконуємо команду, яку передали аргументом (gunicorn ...)
exec "$@"