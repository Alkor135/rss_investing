# rss_investing

Скрипты для работы с новостями и котировками с сайта investing.com, сохранёнными в SQLite базах данных.

## Описание

Проект предназначен для:
- Загрузки и хранения новостей и котировок фьючерсов в базы данных SQLite.
- Удаления дубликатов новостей.
- Извлечения новостей за определённые периоды и сохранения их в текстовые файлы для последующего анализа.

## Структура проекта

- `main.py` — основной скрипт для работы с базой новостей, удаления дубликатов и оптимизации БД.
- `dublicates_db_delete.py` — альтернативный скрипт для удаления дубликатов с использованием оконных функций.
- `read_bd_quote.py` — скрипт для чтения котировок и новостей из БД и сохранения новостей в текстовые файлы.
- `data_quote_db/` — директория с базами данных котировок.
- `data_rss_db/` — директория с базами данных новостей.
- `news/` — директория, куда сохраняются текстовые файлы с новостями.

## Требования

- Python 3.8+
- pandas
- sqlite3

Установить зависимости:
pip install pandas

## Использование

### Чтение котировок и новостей, сохранение новостей в файлы

Запустите:
python read_bd_quote.py

В результате в папке `news` появятся текстовые файлы с заголовками новостей за выбранные периоды.

### Удаление дубликатов новостей

В `main.py` реализована функция удаления дубликатов и оптимизации базы:

python main.py

## Настройки

Пути к базам данных указываются в начале скриптов. Измените их при необходимости под свою структуру каталогов.

## Лицензия

MIT License