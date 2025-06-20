import requests
import xml.etree.ElementTree as ET

try:
    url = "https://ru.investing.com/rss/news_95.rss"
    response = requests.get(url)
    response.raise_for_status()  # Проверка на ошибки HTTP (например, 404)

    xml_content = response.text

    root = ET.fromstring(xml_content)

    channel = root.find('.//channel')  # Находим тег <channel>
    if channel is not None: # Добавляем проверку на None
        channel_name = channel.find('title').text # Получаем текст напрямую из найденного канала
        print(channel_name)

    # Теперь вы можете перебирать элементы XML
    for item in root.findall('.//item'): # Находим все теги <item>
        title = item.find('title').text if item.find('title') is not None else "Нет заголовка"
        pub_date = item.find('pubDate').text if item.find('pubDate') is not None else "Нет даты публикации"
        link = item.find('link').text if item.find('link') is not None else ""

        print(f'{pub_date} {title}')

except requests.exceptions.RequestException as e:
    print(f"Ошибка при запросе к URL: {e}")
except ET.ParseError as e:
    print(f"Ошибка при парсинге XML: {e}")

