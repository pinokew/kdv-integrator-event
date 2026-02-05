FROM python:3.11-slim

WORKDIR /app

# Вимикаємо буферизацію логів (щоб бачити їх одразу)
ENV PYTHONUNBUFFERED=1

# Встановлюємо системні залежності:
# - poppler-utils: КРИТИЧНО для генерації обкладинок з PDF (використовується в covers.py)
# - curl: для перевірок здоров'я контейнера (healthcheck)
RUN apt-get update && apt-get install -y \
    poppler-utils \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Встановлюємо Python залежності
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо код
COPY . .

# --- НАЛАШТУВАННЯ АВТОЗАПУСКУ ПІСЛЯ МОНТУВАННЯ ---
# 1. Робимо скрипт очікування виконуваним
# (Переконайтеся, що скрипт фізично знаходиться в папці src/)
RUN chmod +x src/wait_for_drive.sh

# 2. Вказуємо його як точку входу (ENTRYPOINT)
# Тепер контейнер спочатку запустить цей скрипт, дочекається файлів,
# і тільки потім виконає команду CMD.
ENTRYPOINT ["/app/src/wait_for_drive.sh"]
# -------------------------------------------------

# Запускаємо веб-сервер на порту 8000
# Ця команда передається як аргумент ($@) у wait_for_drive.sh
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]