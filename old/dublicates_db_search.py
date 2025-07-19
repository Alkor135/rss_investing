"""
Поиск дубликатов новостей (в течение дня) в базе данных SQLite.
"""
import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('C:\\Users\\Alkor\\gd\\data_rss_db\\rss_news_investing.db')
cursor = conn.cursor()

try:
    # Выполнение запроса
    cursor.execute("""
        SELECT
            DATE(date) AS news_date,
            title,
            COUNT(*) AS duplicate_count
        FROM
            news
        GROUP BY
            news_date,
            title
        HAVING
            duplicate_count > 1;
    """)

    # Получение результатов
    results = cursor.fetchall()

    # Вывод результатов
    if results:
        print("Обнаружены дубликаты:")
        for row in results:
            news_date, title, duplicate_count = row
            print(f"Дата: {news_date}, Заголовок: '{title}', Количество: {duplicate_count}")
    else:
        print("Дубликатов не обнаружено.")

except sqlite3.Error as e:
    print(f"Ошибка при работе с базой данных: {e}")

finally:
    # Закрытие соединения
    conn.close()
