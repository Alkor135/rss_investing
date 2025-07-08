"""
Получение исторических данных по фьючерсам RTS с MOEX ISS API и занесение записей в БД.
Загружать от 2014-01-01
"""
from pathlib import Path
import requests
from datetime import datetime, timedelta
import pandas as pd
import sqlite3
import sqlighter3_RTS_day


def request_moex(session, url, retries=3, timeout=5):
    """Функция запроса данных с повторными попытками"""
    for attempt in range(retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Ошибка запроса {url} (попытка {attempt + 1}): {e}")
            if attempt == retries - 1:
                return None


def get_info_future(session, security):
    """Запрашивает у MOEX информацию по инструменту"""
    # print(security)
    url = f'https://iss.moex.com/iss/securities/{security}.json'
    j = request_moex(session, url)

    if not j:
        return pd.Series(["", "2130-01-01"])  # Гарантируем, что всегда 2 значения

    data = [{k: r[i] for i, k in enumerate(j['description']['columns'])} for r in
            j['description']['data']]
    df = pd.DataFrame(data)

    shortname = df.loc[df['name'] == 'SHORTNAME', 'value'].values[0] if 'SHORTNAME' in df[
        'name'].values else ""
    lsttrade = df.loc[df['name'] == 'LSTTRADE', 'value'].values[0] if 'LSTTRADE' in df[
        'name'].values else \
        df.loc[df['name'] == 'LSTDELDATE', 'value'].values[0] if 'LSTDELDATE' in df[
            'name'].values else "2130-01-01"

    return pd.Series([shortname, lsttrade])  # Гарантируем возврат 2 значений


def get_future_date_results(
        session: requests.Session,
        tradedate: datetime.date,
        ticker: str,
        connection: sqlite3.Connection,
        cursor: sqlite3.Cursor
) -> None:
    """
    Получает данные по фьючерсам с MOEX ISS API и сохраняет их в базу данных.

    :param session: Сессия requests для выполнения HTTP-запросов.
    :param tradedate: Дата начала загрузки данных.
    :param ticker: Тикер инструмента (например, 'RTS').
    :param connection: Соединение с базой данных SQLite.
    :param cursor: Курсор для выполнения SQL-запросов.
    """
    today_date = datetime.now().date()  # Текущая дата и время
    while tradedate < today_date:
        # Нет записи с такой датой
        if not sqlighter3_RTS_day.tradedate_futures_exists(connection, cursor, tradedate):
            url = (
                f'https://iss.moex.com/iss/history/engines/futures/markets/forts/securities.json?'
                f'date={tradedate}&assetcode={ticker}'
            )
            print(url)
            j = request_moex(session, url)
            if not j or 'history' not in j or not j['history'].get('data'):
                print(f"Нет данных для {tradedate}")
                tradedate += timedelta(days=1)
                continue

            data = [{k: r[i] for i, k in enumerate(j['history']['columns'])} for r in
                    j['history']['data']]
            df = pd.DataFrame(data).dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])
            # print(df.to_string(max_rows=20, max_cols=20))

            if len(df) == 0:
                tradedate += timedelta(days=1)
                continue

            df[['SHORTNAME', 'LSTTRADE']] = df.apply(
                lambda x: get_info_future(session, x['SECID']), axis=1, result_type='expand'
            )
            df["LSTTRADE"] = pd.to_datetime(df["LSTTRADE"], errors='coerce').dt.date.fillna(
                '2130-01-01')
            df = df[df['LSTTRADE'] > tradedate].dropna(subset=['OPEN', 'LOW', 'HIGH', 'CLOSE'])
            df = df[df['LSTTRADE'] == df['LSTTRADE'].min()].reset_index(drop=True)

            if len(df) == 1 and not df['OPEN'].isnull().values.any():
                sqlighter3_RTS_day.add_tradedate_future(
                    connection, cursor, df.loc[0]['TRADEDATE'], df.loc[0]['SECID'],
                    float(df.loc[0]['OPEN']), float(df.loc[0]['LOW']),
                    float(df.loc[0]['HIGH']), float(df.loc[0]['CLOSE']),
                    df.loc[0]['LSTTRADE']
                )
                df = df.drop([
                    'OPENPOSITIONVALUE', 'VALUE', 'SETTLEPRICE', 'SWAPRATE', 'WAPRICE',
                    'SETTLEPRICEDAY', 'NUMTRADES', 'SHORTNAME', 'CHANGE', 'QTY'
                ], axis=1)
                print(df.to_string(max_rows=5, max_cols=20))
                print('Строка записана в БД', '\n')
        tradedate += timedelta(days=1)


if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    ticker = 'RTS'
    path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\{ticker}_day_rss_2025.db')
    start_date = datetime.strptime('2025-01-01', "%Y-%m-%d").date()

    connection = sqlite3.connect(path_db, check_same_thread=True)
    cursor = connection.cursor()

    # Если таблица Futures не пустая
    if sqlighter3_RTS_day.non_empty_table_futures(connection, cursor):
        # Удаляем последнюю запись из БД
        cursor.execute("SELECT MAX(TRADEDATE) FROM Futures")
        max_trade_date = cursor.fetchone()[0]
        if max_trade_date:
            cursor.execute("DELETE FROM Futures WHERE TRADEDATE = ?", (max_trade_date,))
            connection.commit()

        # Меняем стартовую дату на дату последней записи плюс 1 день
        start_date = datetime.strptime(sqlighter3_RTS_day.get_max_date_futures(connection, cursor),
                                       "%Y-%m-%d").date() + timedelta(days=1)

    with requests.Session() as session:
        get_future_date_results(session, start_date, ticker, connection, cursor)

    # Выполняем команду VACUUM
    cursor.execute("VACUUM;")

    # Закрываем курсор и соединение
    cursor.close()
    connection.close()
