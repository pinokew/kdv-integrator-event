import threading
import uuid
import time
import logging

# Налаштування логера для цього модуля
logger = logging.getLogger("KDV-Tasks")

# Глобальний словник для зберігання задач у пам'яті (In-Memory DB)
# Структура: { "task_uuid": { "status": "queued", "created_at": time, ... } }
TASKS = {}

class TaskManager:
    def __init__(self):
        pass

    def start_task(self, func, *args):
        """
        Запускає нову фонову задачу.
        :param func: Функція, яку треба виконати (бізнес-логіка)
        :param args: Аргументи для цієї функції (наприклад, biblionumber)
        :return: task_id (UUID string)
        """
        task_id = str(uuid.uuid4())
        
        # Ініціалізація стану задачі
        TASKS[task_id] = {
            "status": "queued",          # queued -> processing -> success / error
            "created_at": time.time(),
            "progress": "Task initialized",
            "result": None,              # Тут буде результат (наприклад, handle посилання)
            "error": None
        }
        
        logger.info(f"🚀 [Task {task_id}] Created and Queued.")

        # Запуск окремого потоку
        # daemon=True означає, що потік завершиться, якщо впаде основна програма
        thread = threading.Thread(target=self._wrapper, args=(task_id, func, args))
        thread.daemon = True 
        thread.start()
        
        return task_id

    def _wrapper(self, task_id, func, args):
        """
        Обгортка, яка виконується всередині потоку.
        Вона керує статусами та перехоплює помилки.
        """
        try:
            logger.info(f"▶️ [Task {task_id}] Started execution...")
            TASKS[task_id]["status"] = "processing"
            TASKS[task_id]["progress"] = "Starting logic..."
            
            # ВИКОНАННЯ ОСНОВНОЇ ЛОГІКИ
            # Ми передаємо task_id першим аргументом, щоб функція могла (опціонально) оновлювати прогрес
            result = func(task_id, *args)
            
            # Успішне завершення
            TASKS[task_id]["status"] = "success"
            TASKS[task_id]["result"] = result
            TASKS[task_id]["progress"] = "Completed successfully"
            logger.info(f"✅ [Task {task_id}] Finished successfully.")
            
        except Exception as e:
            # Критична помилка під час виконання
            logger.error(f"❌ [Task {task_id}] FAILED: {str(e)}")
            TASKS[task_id]["status"] = "error"
            TASKS[task_id]["error"] = str(e)
            TASKS[task_id]["progress"] = "Failed"

    def get_status(self, task_id):
        """Повертає словник зі станом задачі або None"""
        return TASKS.get(task_id)

    def cleanup_old_tasks(self, max_age_seconds=3600):
        """Очищення пам'яті від старих задач (можна викликати періодично)"""
        now = time.time()
        to_delete = [tid for tid, data in TASKS.items() if now - data['created_at'] > max_age_seconds]
        for tid in to_delete:
            del TASKS[tid]
        if to_delete:
            logger.info(f"🧹 Cleaned up {len(to_delete)} old tasks.")

# Створюємо єдиний екземпляр менеджера для імпорту
task_manager = TaskManager()