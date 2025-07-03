"""
Скрипт создает текстовые файлы из rss ленты новостей (investing.com) БД SQLite
"""
import pandas as pd
from pathlib import Path
import sqlite3

def read_db_quote(db_path_quote: Path) -> pd.DataFrame:
    """
    Читает SQLite базу данных и возвращает DataFrame.
    """
    with sqlite3.connect(db_path_quote) as conn:
        return pd.read_sql_query("SELECT * FROM Futures", conn)
    
def read_db_news(db_path_news, date_max, date_min) -> pd.DataFrame:
    """
    Читает SQLite базу данных и возвращает DataFrame с новостями между date_min и date_max.
    """
    with sqlite3.connect(db_path_news) as conn:
        query = """
            SELECT * FROM news
            WHERE date > ? AND date < ?
        """
        return pd.read_sql_query(query, conn, params=(date_min, date_max))

def save_titles_to_file(df_news: pd.DataFrame, file_path: Path) -> None:
    """
    Сохраняет все строки поля title из датафрейма df_news в файл с указанным путем.
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        # for title in df_news['title']:
        #     file.write(f"{title}\n")
        # Сохраняет в файл не только поле title, но и поле date
        for index, row in df_news.iterrows():
            date = str(row['date'])  # Преобразуем дату в строку для записи в файл
            title = row['title']
            file.write(f"{date}\t{title}\n")

def main(path_db_quote: Path, path_db_news: Path) -> None:
    
    df = read_db_quote(path_db_quote)

    # Перебираем пары строк начиная с последней
    for i in range(len(df) - 1, 0, -1):  # Начинаем с последнего индекса и шагаем на 1 назад
        row1 = df.iloc[i]
        row2 = df.iloc[i-1]

        file_name = f"{row1['TRADEDATE']}.txt"
        # Для МСК времени в БД
        # date_max = f'{row1['TRADEDATE']} 18:45:00'
        # date_min = f'{row2['TRADEDATE']} 18:45:00'
        # Для GMT времени в БД
        date_max = f'{row1['TRADEDATE']} 15:45:00'
        date_min = f'{row2['TRADEDATE']} 15:45:00'

        # print(f"Пара строк: {i}, {i-1}")
        print(f"{file_name} Дата max: {date_max}, Дата min: {date_min}")
        df_news = read_db_news(path_db_news, date_max, date_min)
        print(df_news)
        if len(df_news) == 0:
            break

        # Создайте директорию 'news', если она еще не существует
        (Path('news')).mkdir(parents=True, exist_ok=True)
        save_titles_to_file(df_news, Path(fr'news/{file_name}'))

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    path_db_quote = Path(fr'c:\Users\Alkor\gd\data_quote_db\RTS_day_2014.db')
    path_db_news = Path(fr'C:\Users\Alkor\gd\data_rss_db\rss_news_investing.db')

    main(path_db_quote, path_db_news)
