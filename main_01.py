import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime, timedelta
import sqlite3
import time

def print_blue(text):
    """ Выводит текст в синем цвете """
    print(f"\033[94m{text}\033[0m")

def print_red(text):
    """ Выводит текст в красном цвете """
    print(f"\033[91m{text}\033[0m")

def print_green(text):
    """ Выводит текст в зеленом цвете """
    print(f"\033[92m{text}\033[0m")

def get_links(url):
    """
    Получение ссылок на новостные rss
    Args:
        url:

    Returns: Список ссылок
    """
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Найти заголовок "Новости"
    news_section = soup.find('h2', string='Новости')

    if news_section:
        # Найти родительский div с нужными классами
        rss_column = news_section.find_parent('div',
                                              class_='rssColumn halfSizeColumn float_lang_base_2')

        if rss_column:
            # Найти ul с классом "rssBox" внутри этого div
            rss_box = rss_column.find('ul', class_='rssBox')

            if rss_box:
                # Найти все элементы <li> внутри ul
                list_items = rss_box.find_all('li')

                # Извлечь ссылки из каждого li и отфильтровать по расширению ".rss"
                rss_links = []
                for item in list_items:
                    a_tag = item.find('a')
                    if a_tag and a_tag.get('href') and a_tag['href'].endswith('.rss'):
                        rss_links.append(a_tag['href'])

                # Вывести результаты
                return rss_links

            else:
                print_red("Список RSS не найден.")
        else:
            print_red("Контейнер с новостями не найден.")
    else:
        print_red("Раздел 'Новости' не найден.")

def parsing_news(rss_links):
    """
    Парсинг страницы xml с лентой rss
    Args:
        rss_links:
    Returns:
    """
    df = pd.DataFrame(columns=["date", "section", "title", "link"])
    for rss_link in rss_links:
        try:
            response = requests.get(rss_link)
            response.raise_for_status()  # Проверка на ошибки HTTP (например, 404)

            xml_content = response.text

            root = ET.fromstring(xml_content)

            channel = root.find('.//channel')  # Находим тег <channel>
            channel_name = ''
            if channel is not None: # Добавляем проверку на None
                # Получаем текст напрямую из найденного канала
                channel_name = channel.find('title').text
                print(channel_name)

            # Теперь вы можете перебирать элементы XML
            for item in root.findall('.//item'): # Находим все теги <item>
                title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else "Нет даты публикации"
                link = item.find('link').text if item.find('link') is not None else "Нет ссылки"

                # print(f'{pub_date} {channel_name} {title} {link}')
                df.loc[len(df)] = [pub_date, channel_name, title, link]

        except requests.exceptions.RequestException as e:
            print_red(f"Ошибка при запросе к URL: {e}")
        except ET.ParseError as e:
            print_red(f"Ошибка при парсинге XML: {e}")
        time.sleep(1)
    return df

def save_to_sqlite(df, db_path):
    """
    Сохраняет DataFrame в SQLite базу данных.
    Если база данных не существует, то она создается.

    Args:
        df: DataFrame с новостями
        db_path: Путь к файлу базы данных
    """
    # Преобразуем поле "date" в datetime для корректной сортировки и сохранения
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Подключаемся к базе данных (создаст файл, если он не существует)
    conn = sqlite3.connect(db_path)

    try:
        # Создаем таблицу, если она еще не существует
        with conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS news (
                    date TEXT,
                    title TEXT
                )
            """)

        # Вставляем данные в таблицу БД
        df[["date", "title"]].to_sql('news', conn, if_exists='append', index=False)
    finally:
        conn.close()

def drop_duplicate_titles(df):
    """
    Удаляет дубликаты по полю 'Заголовок', оставляя первую строку с самой ранней датой.
    """
    # Преобразуем поле "Дата" в datetime для корректной сортировки
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    # Сортируем по "Заголовок" и "Дата" (по возрастанию даты)
    df_sorted = df.sort_values(by=["title", "date"])
    # Оставляем только первое вхождение каждого заголовка
    df_unique = df_sorted.drop_duplicates(subset=["title"], keep="first")
    # Восстанавливаем исходный порядок индексов
    df_unique = df_unique.sort_index()
    return df_unique

def main():
    rss_links = get_links(URL)
    print_blue('Ссылки на RSS ленты получены')
    df = parsing_news(rss_links)
    df = drop_duplicate_titles(df)

    save_to_sqlite(df, db_path)
    print_green("Новости сохранены в базе данных.")

if __name__ == '__main__':
    URL = "https://ru.investing.com/webmaster-tools/rss"  # url для парсинга rss
    db_path = r'C:\Users\Alkor\gd\data_rss_db\rss_news_investing.db'  # Путь к файлу базы данных

    main()
