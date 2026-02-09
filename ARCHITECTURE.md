Архітектура та Workflow (v6.5)

Цей документ описує логіку роботи KDV Integrator v6.5, включаючи асинхронність, паралелізацію та протоколи взаємодії.

🔄 Загальний Workflow (Fork-Join Pattern)

Процес обробки однієї книги розділений на Послідовну фазу (підготовка) та Паралельну фазу (виконання).

graph TD
    User((Koha UI)) -->|POST /integrate| API[Integrator API]
    API -->|Return task_id| User
    API -->|Start Thread| Core[Async Core Logic]
    
    subgraph "Serial Phase (Blocking)"
        Core -->|Check 956$u| FileCheck{File Exists?}
        FileCheck -->|No| Error[Exit & Log Error]
        FileCheck -->|Yes| Rename[Rename & Move to /Processed]
    end

    subgraph "Parallel Phase (ThreadPoolExecutor)"
        Rename --> Fork((Fork))
        
        Fork -->|Thread A| CoverService[Cover Service]
        CoverService -->|1. Generate JPG| PDF2IMG[pdf2image]
        PDF2IMG -->|2. Upload (CGI)| KohaCGI[Koha Staff (HTML)]
        KohaCGI -->|3. Scrape ID| Scraper[HTML Parser]
        
        Fork -->|Thread B| DSpaceWorkflow[DSpace Workflow]
        DSpaceWorkflow -->|1. Parse MARC| Parser[MARCXML Parser]
        Parser -->|2. Check Duplicates| DSpaceREST[DSpace REST API]
        DSpaceREST -->|3. Create Item & Upload PDF| DSpaceREST
    end

    subgraph "Finalize Phase (Join)"
        Scraper --> Join((Join))
        DSpaceREST --> Join
        Join -->|Update 956 field| KohaREST[Koha REST API]
        KohaREST -->|Write: Handle URL + Cover URL| DB[(Koha DB)]
    end


⚡ Деталі Реалізації

1. Асинхронність (Async Core)

Щоб уникнути помилки Cloudflare 524 Timeout (яка виникає при запитах довших за 100с), ми не чекаємо завершення обробки.

Request: Клієнт шле запит і миттєво отримує UUID задачі.

Processing: Задача додається в глобальний словник TASKS (In-Memory) і запускається в окремому потоці Python threading.

Polling: JS-клієнт в Koha опитує статус кожні 2 секунди.

2. Паралелізація (Concurrency)

Файл src/app.py використовує concurrent.futures.ThreadPoolExecutor(max_workers=2).

Thread A (Bonus Task): Генерує обкладинку. Якщо падає — логується WARNING, але процес не зупиняється.

Thread B (Critical Task): Інтеграція з DSpace. Якщо падає — весь процес отримує статус ERROR, файл переміщується в папку Error.

3. Протокол "Hybrid CGI" (Cover Upload)

REST API Koha не дозволяє повноцінно працювати з локальними обкладинками. Ми емулюємо дії людини:

Auth: Логін через POST-форму на mainpage.pl.

AJAX Spoofing: Завантаження файлу на upload-file.pl з обов'язковим заголовком X-Requested-With: XMLHttpRequest (інакше Koha не віддасть JSON).

Scraping: Парсинг HTML-відповіді сторінки інструментів для знаходження внутрішнього imagenumber, щоб сформувати публічне посилання.

4. Data Warehouse (Збагачення даних)

Інтегратор не просто передає файли, а й збагачує запис у Koha:

956$y — Статус (imported, error).

956$z — Лог помилки або попередження.

956$3 — UUID елемента в DSpace (для дедуплікації).

956$c — Пряме посилання на обкладинку (.../opac-image.pl?imagenumber=...).

856$u — Handle-посилання на репозиторій.

🛡 Безпека та Відмовостійкість

Retry Policy: 3 спроби зчитування PDF та 3 спроби отримання URL обкладинки (з паузою 1с).

Rename-First: Файл спочатку перейменовується (v01, v02), щоб гарантувати унікальність і стабільність шляху.

Rollback: При критичній помилці файл переміщується в папку Error для ручного аналізу.