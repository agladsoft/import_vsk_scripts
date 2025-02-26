import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from pandas import DataFrame
from datetime import datetime
from src.scripts.parsed import ParsedDf

headers_eng: dict = {
    "Год": "year",
    "Месяц": "month",
    "Линия": "line",
    "Дата отгрузки": "shipment_date",
    "Порт": "tracking_seaport",
    "Страна": "departure_country",
    "Отправитель": "shipper_name",
    "Получатель": "consignee_name",
    "Экспедитор": "expeditor",
    "Груз": "goods_name",
    "Тип контейнера": "container_type",
    "Размер контейнера": "container_size",
    "Кол-во контейнеров, шт.": "container_count",
    "Терминал": "terminal",
    "TEU": "teu",
    "Номер контейнера": "container_number",
    "КОД ТНВЭД": "tnved",
    "Группа груза по ТНВЭД": "tnved_group_id",
    "Наименование Группы": "tnved_group_name",
    "ИНН": "shipper_inn",
    "УНИ-компания": "shipper_name_unified",
    "Страна КОМПАНИИ": "consignee_country",
    "Направление": "direction",
    "Тип убытия": "departure_type",
    "Коносамент": "consignment",
    "Судно": "ship_name",
    "Рейс": "voyage",
    "Порожний": "is_empty",
    "Агент": "agent",
    "Станция УКП": "station_ukp",
    "Сборный груз": "combined_cargo",
    "Номер ГТД": "gtd_number",
    "Станция назначени (план)": "destination_station",
    "Вес нетто": "goods_weight_with_package",
    "Вес брутто": "goods_weight_brutto"
}


class ImportVSK(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def change_type_and_values(df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['shipment_date'] = df['shipment_date'].dt.date.astype(str)
            df[['gtd_number']] = df[['gtd_number']].apply(lambda x: x.fillna('Нет данных'))

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def read_file(self) -> DataFrame:
        """Read the Excel file and drop rows with all NaN values.

                This function reads the Excel file specified by the input file path,
                and drops any rows where all values are NaN.

                Args:
                    self: Instance of the class.

                Returns:
                    DataFrame: The DataFrame read from the Excel file with rows containing all NaN values dropped.
                """
        df: DataFrame = pd.read_excel(self.input_file_path, dtype={"ИНН": str})
        df = df.dropna(axis=0, how='all')
        return df
    @staticmethod
    def transformation_df(dataframe: DataFrame) -> DataFrame:
        """Rename and process the DataFrame columns.

                This function renames the columns of the input DataFrame according to the headers_eng mapping,
                drops common columns if the renaming changes the set of columns, and strips whitespace from string values.

                Args:
                    dataframe (DataFrame): The input DataFrame.

                Returns:
                    DataFrame: The transformed DataFrame.
                """
        original_columns: list = list(dataframe.columns)
        df: DataFrame = dataframe.rename(columns=headers_eng)
        renamed_columns: list = list(df.columns)
        same_columns: set = set(original_columns) & set(renamed_columns)
        if len(same_columns) != len(original_columns):
            df: DataFrame = df.drop(columns=same_columns)
        df: DataFrame = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        return df

    def get_port(self, df) -> DataFrame:
        df = df.replace({np.nan: None, "NaT": None})
        df["direction"] = df["direction"].replace({"импорт": "import", "экспорт": "export", "каботаж": "cabotage"})
        ParsedDf(df).get_port()
        df = df.replace({np.nan: None, "NaT": None})
        return df

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df_init: DataFrame = self.read_file()
        df_trans: DataFrame = self.transformation_df(df_init)
        self.add_new_columns(df_trans)
        self.change_type_and_values(df_trans)
        df_port: DataFrame = self.get_port(df_trans)
        self.write_to_json(df_port.to_dict('records'))




if __name__ == "__main__":
    import_vsk: ImportVSK = ImportVSK(sys.argv[1], sys.argv[2])
    import_vsk.main()
