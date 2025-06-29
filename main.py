import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime


def get_links(url):
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

                # # Вывести результаты
                # for link in rss_links:
                #     print(link)
                return rss_links

            else:
                print("Список RSS не найден.")
        else:
            print("Контейнер с новостями не найден.")
    else:
        print("Раздел 'Новости' не найден.")

def parsing_news(rss_links):
    df = pd.DataFrame(columns=["Дата", "Раздел", "Заголовок", "Ссылка"])
    for link in rss_links:
        try:
            response = requests.get(link)
            response.raise_for_status()  # Проверка на ошибки HTTP (например, 404)

            xml_content = response.text

            root = ET.fromstring(xml_content)

            channel = root.find('.//channel')  # Находим тег <channel>
            channel_name = ''
            if channel is not None: # Добавляем проверку на None
                channel_name = channel.find('title').text # Получаем текст напрямую из найденного канала
                # print(channel_name)

            # Теперь вы можете перебирать элементы XML
            for item in root.findall('.//item'): # Находим все теги <item>
                title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
                pub_date = item.find('pubDate').text if item.find('pubDate') is not None else "Нет даты публикации"
                link = item.find('link').text if item.find('link') is not None else "Нет ссылки"

                print(f'{pub_date} {channel_name} {title} {link}')
                df =

        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе к URL: {e}")
        except ET.ParseError as e:
            print(f"Ошибка при парсинге XML: {e}")

if __name__ == '__main__':
    URL = "https://ru.investing.com/webmaster-tools/rss"

    rss_links = get_links(URL)
    print(rss_links)
    df = parsing_news(rss_links)
    print(df)
