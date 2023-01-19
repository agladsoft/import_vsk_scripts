import datetime
import json
import os
import sys
import contextlib
import numpy as np
import pandas as pd

input_file_path = os.path.abspath(sys.argv[1])
output_folder = sys.argv[2]

headers_eng = {
    "Год": "year",
    "Месяц": "month",
    "Период": "period",
    "Линия": "line",
    "Порт": "departure_port",
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
    "Страна КОМПАНИИ": "shipper_country",
    "Направление": "direction",
    "Тип убытия": "departure_type",
    "Коносамент": "consignment",
    "Судно": "ship_name",
    "Рейс": "voyage",
    "Тип парка": "park_type",
    "Агент": "agent",
    "Станция УКП": "station_ukp",
    "Сборный груз": "combined_cargo",
    "Станция назначени (план)": "destination_station",
    "Вес нетто": "goods_weight_netto",
    "Вес брутто": "goods_weight_brutto"
}


def trim_all_columns(df):
    """
    Trim whitespace from ends of each value across all series in dataframe
    """
    trim_strings = lambda x: x.strip() if isinstance(x, str) else x
    return df.applymap(trim_strings)


def convert_to_int(val):
    return int(val) if val.isdigit() else int(val in [True, 'True'])

df = pd.read_csv(input_file_path, dtype=str)
df = df.replace({np.nan: None})
df = df.rename(columns=headers_eng)
df[['combined_cargo']] = df[['combined_cargo']].astype(bool)
df = df.loc[:, ~df.columns.isin(['direction', 'tnved_group_name', 'shipper_inn',
                                 'shipper_name_unified', 'departure_country'])]
df = trim_all_columns(df)
parsed_data = df.to_dict('records')
deleted_index = []
for index, dict_data in enumerate(parsed_data):
    if any(list(dict_data.values())):
        for key, value in dict_data.items():
            with contextlib.suppress(Exception):
                if key in ['year', 'month', 'teu', 'container_size', 'container_count']:
                    dict_data[key] = convert_to_int(value)
                elif key in ['goods_weight_netto', 'goods_weight_brutto']:
                    dict_data[key] = float(value)
                elif key in ['tnved_group_id']:
                    dict_data[key] = f"{int(value)}"
                elif key == 'terminal':
                    dict_data[key] = os.environ.get('XL_VSK_IMPORT')
                elif key == 'combined_cargo':
                    dict_data[key] = value in [1, 'да', 'Да']
        dict_data['original_file_name'] = os.path.basename(input_file_path)
        dict_data['original_file_parsed_on'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    else:
        deleted_index.append(index)
basename = os.path.basename(input_file_path)
output_file_path = os.path.join(output_folder, f'{basename}.json')
for index in sorted(deleted_index, reverse=True):
    del parsed_data[index]
with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
    json.dump(parsed_data, f, ensure_ascii=False, indent=4)