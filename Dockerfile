FROM python:3.11-slim

WORKDIR /app

# Вимикаємо буферизацію логів (щоб бачити їх одразу)
ENV PYTHONUNBUFFERED=1

# Встановлюємо залежності
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо код
COPY . .

# Запускаємо веб-сервер на порту 8000
# host 0.0.0.0 означає "слухати всіх" (потрібно для Docker)
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]