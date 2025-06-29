import pandas as pd
from pathlib import Path
import sqlite3

def read_sqlite_db(db_path: Path) -> pd.DataFrame:
    """
    Читает SQLite базу данных и возвращает DataFrame.
    """
    with sqlite3.connect(db_path) as conn:
        return pd.read_sql_query("SELECT * FROM Futures", conn)

def main(path_db: Path) -> None:
    
    df = read_sqlite_db(path_db)
    df['TRADEDATE'] = pd.to_datetime(df['TRADEDATE'])  # Преобразуем даты в datetime

    # Перебираем пары строк начиная с последней
    for i in range(len(df) - 1, 0, -1):  # Начинаем с последнего индекса и шагаем на 2 назад
        row1 = df.iloc[i]
        row2 = df.iloc[i-1]
     
        date_max = row1['TRADEDATE']
        date_min = row2['TRADEDATE']

        # Преобразуем в строку с форматом YYYY-MM-DD
        date_str = date_max.strftime('%Y-%m-%d')
        file_name = f"{date_str}.txt"

        # Добавляем время 18:45:00 к датам
        date_max = date_max.replace(hour=18, minute=45, second=0)
        date_min = date_min.replace(hour=18, minute=45, second=0)
        
        print(f"Пара строк: {i}, {i-1}")
        print(f"{file_name} Дата max: {date_max}, Дата min: {date_min}")
        # break

if __name__ == '__main__':  # Точка входа при запуске этого скрипта
    path_db = Path(fr'c:\Users\Alkor\gd\data_quote_db\RTS_day_2014.db')

    main(path_db)
