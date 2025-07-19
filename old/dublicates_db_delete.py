"""
Удаление дубликатов новостей (в течение дня) из базы данных SQLite.
"""
import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('C:\\Users\\Alkor\\gd\\data_rss_db\\rss_news_investing.db')
cursor = conn.cursor()

try:
    # Выполнение запроса на удаление дубликатов
    cursor.execute("""
        DELETE FROM news
        WHERE rowid NOT IN (
            SELECT rowid
            FROM (
                SELECT
                    rowid,
                    DATE(date) AS news_date,
                    title,
                    ROW_NUMBER() OVER (PARTITION BY DATE(date), title ORDER BY date ASC) AS rn
                FROM
                    news
            ) AS subquery
            WHERE rn = 1
        );
    """)

    # Подтверждение изменений
    conn.commit()  # Очень важно!

    print("Дубликаты удалены.")

except sqlite3.Error as e:
    print(f"Ошибка при работе с базой данных: {e}")
    conn.rollback() # Откат транзакции в случае ошибки
finally:
    # Закрытие соединения
    conn.close()
