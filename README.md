# Building Materials ETL

Локальный ETL для анализа продаж строительных материалов и недельных макропоказателей за 2019–2024 годы. Реализация использует Python 3.10+ и SQLite без внешних зависимостей. Подробные проектные решения и ограничения описаны в [TASK.md](TASK.md).

## Структура

- `building_materials_transactions.csv` — исходные продажи.
- `macro_drivers_weekly.csv` — недельные макропоказатели.
- `data_dictionary_materials.csv` — словарь полей.
- `etl.py` — извлечение, валидация, трансформация, загрузка.
- `TASK.md` — анализ источников и обоснование архитектуры по заданию.
- `tests/test_etl.py` — базовые автоматические проверки.
- `data/warehouse.sqlite` — созданное аналитическое хранилище (не включается в Git).

## Схема хранилища

`fact_transactions` хранит меры продаж и ключи измерений. Внешние ключи ведут к календарю `dim_date`, географии `dim_region`, товару `dim_product`, каналу `dim_channel` и типу клиента `dim_customer_type`. `macro_weekly` содержит внешние индикаторы на неделю. Ошибочные входные строки и причину отклонения содержит `rejected_rows`.

## Быстрый запуск

Из корня проекта:

```powershell
python etl.py
```

По умолчанию создается `data/warehouse.sqlite`. Для других файлов:

```powershell
python etl.py --transactions path/to/sales.csv --macro path/to/macro.csv --database data/warehouse.sqlite
```

Выводится JSON-сводка принятых и отклоненных строк и итоговых счетчиков. Повторный запуск безопасен. Идентификатор строки формируется из имени источника и номера CSV-строки: он позволяет обработать исправление истории на прежнем месте и не склеивать разные продажи с одинаковым набором измерений. Поэтому при инкрементальном обновлении сохраняйте имя файла и порядок уже загруженных строк. Для просмотра БД подойдет DB Browser for SQLite или команда `python -c "import sqlite3; c=sqlite3.connect('data/warehouse.sqlite'); print(c.execute('select count(*) from fact_transactions').fetchone())"`.

## Проверки

```powershell
python -m unittest discover -s tests -v
```

Схема и бизнес-правила описаны в `TASK.md`. При критической ошибке SQLite отменяет транзакцию; ошибочные строки попадают в таблицу `rejected_rows` с исходным JSON и причиной.
