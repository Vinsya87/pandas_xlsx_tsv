import sqlite3

import pandas as pd


class XLSXProcessor:
    def __init__(self, file_path):
        self.data = pd.read_excel(file_path)

    def process_data(self):
        unique_countries = self.data['COUNTRY'].unique()
        countries_data = [(
            i+1,
            country) for i,
            country in enumerate(unique_countries)]

        unique_isg = self.data[['ID_ISG', 'ISG']].drop_duplicates()
        isg_data = [(
            row[0],
            row[1]) for row in unique_isg.itertuples(index=False)]

        goods_data = []
        id_tovar_set = set()
        for row in self.data.itertuples():
            country_id = next((
                x[0] for x in countries_data if x[1] == row.COUNTRY), None)
            isg_id = next((x[0] for x in isg_data if x[0] == row.ID_ISG), None)
            # Проверим что не пустой, уникальный и без '--'
            if (str(row.ID_TOVAR) and
                    not str(row.ID_TOVAR).startswith("--") and
                    str(row.ID_TOVAR) not in id_tovar_set):
                goods_data.append((
                    row.ID_TOVAR,
                    row.TOVAR,
                    row.BARCOD,
                    country_id,
                    isg_id))
                id_tovar_set.add(str(row.ID_TOVAR))

        # Подсчет количества товаров по каждой стране
        country_counts = self.data['COUNTRY'].value_counts()

        # Запись результатов в файл data.tsv
        with open('data.tsv', 'w') as file:
            for country, count in country_counts.items():
                file.write(f"{country} - {count}\n")

        return goods_data, countries_data, isg_data


class DatabaseManager:
    def __init__(self, db_name):
        self.connection = sqlite3.connect(db_name)
        self.cursor = self.connection.cursor()

    def create_tables(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS GOODS (
                ID_TOVAR INTEGER PRIMARY KEY,
                NAME_TOVAR TEXT,
                BARCOD TEXT,
                ID_COUNTRY INTEGER,
                ID_ISG INTEGER
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS COUNTRY (
                ID_COUNTRY INTEGER PRIMARY KEY,
                NAME_COUNTRY TEXT
            )
        """)
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS ISG (
                ID_ISG INTEGER PRIMARY KEY,
                NAME_ISG TEXT
            )
        """)

    def insert_data(self, table, data):
        if table == 'GOODS':
            for row in data:
                self.cursor.execute(
                    "INSERT INTO GOODS ("
                    "ID_TOVAR,"
                    "NAME_TOVAR,"
                    "BARCOD,"
                    "ID_COUNTRY,"
                    "ID_ISG) VALUES (?, ?, ?, ?, ?)",
                    row
                )
        elif table == 'COUNTRY':
            for row in data:
                self.cursor.execute(
                    "INSERT INTO COUNTRY (ID_COUNTRY, NAME_COUNTRY)"
                    "VALUES (?, ?)",
                    row)
        elif table == 'ISG':
            for row in data:
                self.cursor.execute(
                    "INSERT INTO ISG (ID_ISG, NAME_ISG)"
                    "VALUES (?, ?)",
                    row)

    def close_connection(self):
        self.connection.commit()
        self.connection.close()


if __name__ == "__main__":
    xlsx_processor = XLSXProcessor("data.xlsx")
    goods_data, countries_data, isg_data = xlsx_processor.process_data()

    db_manager = DatabaseManager("base.sqlite")
    db_manager.create_tables()

    db_manager.insert_data('GOODS', goods_data)
    db_manager.insert_data('COUNTRY', countries_data)
    db_manager.insert_data('ISG', isg_data)

    db_manager.close_connection()
