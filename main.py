import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import time
import os

def print_blue(text: str) -> None:
    print(f"\033[94m{text}\033[0m")

def print_red(text: str) -> None:
    print(f"\033[91m{text}\033[0m")

def print_green(text: str) -> None:
    print(f"\033[92m{text}\033[0m")

def get_links(url: str) -> list[str]:
    """
    Получение ссылок на новостные rss
    """
    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        news_section = soup.find('h2', string='Новости')
        if news_section:
            rss_column = news_section.find_parent('div', class_='rssColumn halfSizeColumn float_lang_base_2')
            if rss_column:
                rss_box = rss_column.find('ul', class_='rssBox')
                if rss_box:
                    list_items = rss_box.find_all('li')
                    rss_links = [
                        item.find('a')['href']
                        for item in list_items
                        if item.find('a') and item.find('a').get('href', '').endswith('.rss')
                    ]
                    return rss_links
                else:
                    print_red("Список RSS не найден.")
            else:
                print_red("Контейнер с новостями не найден.")
        else:
            print_red("Раздел 'Новости' не найден.")
    except Exception as e:
        print_red(f"Ошибка при получении ссылок: {e}")
    return []

def parsing_news(rss_links: list[str]) -> pd.DataFrame:
    """
    Парсинг страницы xml с лентой rss
    """
    df = pd.DataFrame(columns=["date", "section", "title", "link"])
    for rss_link in rss_links:
        try:
            response = requests.get(rss_link)
            response.raise_for_status()
            xml_content = response.text
            root = ET.fromstring(xml_content)
            channel = root.find('.//channel')
            channel_name = channel.find('title').text if channel is not None and channel.find('title') is not None else ""
            print(f'Обработка канала: {channel_name}')
            for item in root.findall('.//item'):
                title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else "Нет даты публикации"
                link = item.find('link').text if item.find('link') is not None else "Нет ссылки"
                df.loc[len(df)] = [pub_date, channel_name, title, link]
        except requests.exceptions.RequestException as e:
            print_red(f"Ошибка при запросе к URL: {e}")
        except ET.ParseError as e:
            print_red(f"Ошибка при парсинге XML: {e}")
        time.sleep(1)
    # Преобразуем поле "date" в datetime один раз для всего df
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df

def save_to_sqlite(df: pd.DataFrame, db_path: str) -> None:
    """
    Сохраняет DataFrame в SQLite базу данных.
    """
    if df.empty:
        print_red("DataFrame пустой, нечего сохранять в БД.")
        return
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    date TEXT,
                    title TEXT
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_news_date_title ON news(date, title)")
            df[["date", "title"]].to_sql('news', conn, if_exists='append', index=False)
        except Exception as e:
            print_red(f"Ошибка при сохранении в БД: {e}")

def drop_duplicate_titles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Удаляет дубликаты по полю 'title', оставляя первую строку с самой ранней датой.
    """
    df = df.copy()
    df = (
        df.sort_values(by=["title", "date"])
        .drop_duplicates(subset=["title"], keep="first")
        .reset_index(drop=True)
    )
    return df

def remove_existing_titles_from_df(df: pd.DataFrame, db_path: str) -> pd.DataFrame:
    """
    Удаляет из df строки, у которых title уже есть в БД за последние 3 дня.
    Если база пуста, возвращает исходный df.
    """
    with sqlite3.connect(db_path) as conn:
        try:
            cursor = conn.execute("SELECT COUNT(*) FROM news")
            count = cursor.fetchone()[0]
            if count == 0:
                return df
            three_days_ago = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
            query = """
                SELECT title FROM news
                WHERE date >= ?
            """
            existing_titles = pd.read_sql_query(query, conn, params=(three_days_ago,))
            existing_titles_set = set(existing_titles['title'])
        except Exception as e:
            print_red(f"Ошибка при чтении из БД: {e}")
            return df
    filtered_df = df[~df['title'].isin(existing_titles_set)].reset_index(drop=True)
    return filtered_df

def remove_duplicates_from_db(db_path: str) -> None:
    """
    Удаляет дубликаты из таблицы news по полям date и title, оставляя одно вхождение.
    Также выводит количество удалённых дубликатов и выполняет VACUUM.
    """
    try:
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM news")
            before_count = cursor.fetchone()[0]
            conn.execute("""
                DELETE FROM news
                WHERE rowid NOT IN (
                    SELECT MIN(rowid)
                    FROM news
                    GROUP BY date, title
                )
            """)
            cursor = conn.execute("SELECT COUNT(*) FROM news")
            after_count = cursor.fetchone()[0]
            deleted_count = before_count - after_count
            print_green(f"Дубликаты в базе данных удалены. Удалено строк: {deleted_count}")
    except Exception as e:
        print_red(f"Ошибка при удалении дубликатов из БД: {e}")

    # VACUUM выполняем в отдельном соединении!
    try:
        with sqlite3.connect(db_path) as conn:
            conn.isolation_level = None  # Отключаем транзакции
            conn.execute("VACUUM")
            print_green("VACUUM выполнен: база данных оптимизирована.")
    except Exception as e:
        print_red(f"Ошибка при выполнении VACUUM: {e}")

def main(url: str, db_path: str) -> None:
    rss_links = get_links(url)
    if not rss_links:
        print_red("Не удалось получить ссылки на RSS ленты.")
        return
    print_blue('Ссылки на RSS ленты получены')
    df = parsing_news(rss_links)
    df = drop_duplicate_titles(df)
    df = remove_existing_titles_from_df(df, db_path)
    save_to_sqlite(df, db_path)
    print_green(f"Новости сохранены в базе данных. Сохранено строк: {len(df)}")
    remove_duplicates_from_db(db_path)

if __name__ == '__main__':
    URL = "https://ru.investing.com/webmaster-tools/rss"
    db_path = r'C:\Users\Alkor\gd\data_rss_db\rss_news_investing.db'
    main(URL, db_path)
