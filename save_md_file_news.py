import pandas as pd
from pathlib import Path
import sqlite3

def read_db_quote(db_path_quote: Path) -> pd.DataFrame:
    """
    Читает таблицу Futures из базы данных котировок и возвращает DataFrame.
    """
    with sqlite3.connect(db_path_quote) as conn:
        return pd.read_sql_query("SELECT * FROM Futures", conn)

def read_db_news(db_path_news, date_max, date_min) -> pd.DataFrame:
    """
    Читает новости из базы данных за указанный период времени.
    """
    with sqlite3.connect(db_path_news) as conn:
        query = """
            SELECT * FROM news
            WHERE date > ? AND date < ?
        """
        return pd.read_sql_query(query, conn, params=(date_min, date_max))

def save_titles_to_markdown(df_news: pd.DataFrame, file_path: Path, next_bar: str) -> None:
    """
    Сохраняет заголовки новостей в markdown-файл с метаданными о направлении следующей свечи.
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        # Метаданные в формате markdown front matter
        file.write(f"---\nnext_bar: {next_bar}\n---\n\n")
        for _, row in df_news.iterrows():
            title = row['title']
            # date = str(row['date'])  # Преобразуем дату в строку для записи в файл
            # file.write(f"{date}\t{title}\n")  # Записываем дату и заголовок в файл
            file.write(f"- {title}\n")  # Записываем только заголовок в файл

def main(path_db_quote: Path, path_db_news: Path, md_news_dir: Path) -> None:
    """
    Основная функция: читает котировки и новости, формирует и сохраняет markdown-файлы с новостями и метаданными.
    """
    df = read_db_quote(path_db_quote)
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])
    df.sort_values(by='TRADEDATE', inplace=True)
    df['TRADEDATE'] = df['TRADEDATE'].astype(str)
    df['bar'] = df.apply(lambda x: 'up' if (x['OPEN'] < x['CLOSE']) else 'down', axis=1)
    df['next_bar'] = df['bar'].shift(-1)
    df.dropna(inplace=True)  # Стереть строки с NaN
    # print(df)
    # return

    for i in range(len(df) - 1, 0, -1):
        row1 = df.iloc[i]
        row2 = df.iloc[i-1]

        file_name = f"{row1['TRADEDATE']}.md"
        date_max = f"{row1['TRADEDATE']} 15:45:00"
        date_min = f"{row2['TRADEDATE']} 15:45:00"

        print(f"{file_name} Дата max: {date_max}, Дата min: {date_min}")
        df_news = read_db_news(path_db_news, date_max, date_min)
        print(df_news)
        if len(df_news) == 0:
            break

        # (Path('c:/news')).mkdir(parents=True, exist_ok=True)
        # direction = "up" if row1['OPEN'] < row1['CLOSE'] else "down"
        save_titles_to_markdown(df_news, Path(fr'{md_news_dir}/{file_name}'), row1['next_bar'])

if __name__ == '__main__':
    path_db_quote = Path(fr'c:\Users\Alkor\gd\data_quote_db\RTS_day_rss_2025.db')
    path_db_news = Path(fr'C:\Users\Alkor\gd\data_rss_db\rss_news_investing.db')
    md_news_dir = Path('c:/news')

    if not path_db_quote.exists():
        print("Ошибка: Файл базы данных котировок не найден.")
        exit()

    if not path_db_news.exists():
        print("Ошибка: Файл базы данных новостей не найден.")
        exit()

    (Path(md_news_dir)).mkdir(parents=True, exist_ok=True)

    main(path_db_quote, path_db_news, md_news_dir)