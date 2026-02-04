"""
Модуль конфігурації мапування метаданих (Koha MARC -> DSpace Dublin Core).
Тут визначаються правила, за якими поля з Koha потрапляють у DSpace.
"""

# 1. СЛОВНИК МАПУВАННЯ ПОЛІВ (Field Mapping)
METADATA_RULES = {
    # --- НАЗВА ---
    "dc.title": {
        "tag": "245",
        "subfield": "a",
        "multivalue": False
    },

    # --- АВТОРИ ---
    # 100$a -> Основний автор (Людина)
    "dc.contributor.author": {
        "tag": "100",
        "subfield": "a",
        "multivalue": False
    },
    # 700$a -> Додаткові автори (Співавтори)
    "dc.contributor.other": {
        "tag": "700",
        "subfield": "a",
        "multivalue": True  # Збираємо всіх співавторів у список
    },
    # 110$a -> Корпоративний автор (Організація)
    # Раджу 'dc.contributor', щоб відділити організації від людей
    "dc.contributor": {
        "tag": "110",
        "subfield": "a",
        "multivalue": False
    },

    # --- РІК ВИДАННЯ (Складна логіка пріоритетів) ---
    # Ми вказуємо список джерел. Програма перевірятиме їх по черзі.
    "dc.date.issued": {
        "sources": [
            {"tag": "264", "subfield": "c"}, # Пріоритет 1: RDA (264)
            {"tag": "260", "subfield": "c"}  # Пріоритет 2: Старий MARC (260)
        ],
        "regex": r"(\d{4})" # Витягуємо тільки 4 цифри року
    },

    # --- ТИП ДОКУМЕНТА ---
    "dc.type": {
        "tag": "942",        # Koha Item Type Code
        "subfield": "c",
        "conversion": "type" # Використовувати словник TYPE_CONVERSION
    }
}

# 2. СЛОВНИКИ ПЕРЕКЛАДУ ЗНАЧЕНЬ (Value Conversion)
# Перетворення кодів Koha (942$c) у зрозумілі назви для DSpace

TYPE_CONVERSION = {
    # Код Koha : Значення DSpace
    "BK": "Book",
    "MP": "Map",
    
    # Значення за замовчуванням (якщо код не знайдено в списку)
    "DEFAULT": "Book"
}