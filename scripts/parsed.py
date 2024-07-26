import os
import sys
import json
import logging
import requests
from dotenv import load_dotenv
from clickhouse_connect import get_client
from clickhouse_connect.driver import Client

LINES = ['СИНОКОР РУС ООО', 'HEUNG-A LINE CO., LTD', 'MSC', 'SINOKOR', 'SINAKOR', 'SKR', 'sinokor',
         'ARKAS', 'arkas', 'Arkas',
         'MSC', 'msc', 'Msc', 'SINOKOR', 'sinokor', 'Sinokor', 'SINAKOR', 'sinakor', 'HUENG-A LINE',
         'HEUNG-A LINE CO., LTD', 'heung']
IMPORT = ['импорт', 'import']


load_dotenv()


def get_my_env_var(var_name: str) -> str:
    try:
        return os.environ[var_name]
    except KeyError as e:
        raise MissingEnvironmentVariable(f"{var_name} does not exist") from e


class MissingEnvironmentVariable(Exception):
    pass


def clickhouse_client():
    try:
        client: Client = get_client(host=get_my_env_var('HOST'), database=get_my_env_var('DATABASE'),
                                    username=get_my_env_var('USERNAME_DB'), password=get_my_env_var('PASSWORD'))
        logging.info('Connection to ClickHouse is successful')
    except Exception as ex_connect:
        logging.info(f"Error connecting to ClickHouse: {ex_connect}")
        sys.exit(1)
    return client


def reel_shipping_list_name():
    reel_shipping = 'REEL SHIPPING'
    client = clickhouse_client()
    query = client.query(f"Select line from default.reference_lines where line_unified = '{reel_shipping}'")
    result = [i[0].upper() for i in query.result_rows] if query.result_rows else None
    return result


class Parsed:
    def __init__(self, df):
        self.df = df
        self.url = f"http://{os.environ['IP_ADDRESS_CONSIGNMENTS']}:8004"
        self.headers = {
            'Content-Type': 'application/json'
        }

    def get_direction(self, direction):
        if direction.lower() in IMPORT:
            return 'import'
        else:
            return 'export'

    @staticmethod
    def get_consignment(consignment: str) -> str:
        lst_consignment: list = consignment.strip().split(',')
        if len(lst_consignment) > 1:
            return lst_consignment[0]
        return consignment

    def body(self, row):
        consignment = self.get_consignment(row.get('consignment'))
        data = {
            'line': row['line'],
            'consignment': consignment,
            'direction': self.get_direction(row['direction'])

        }
        return data

    def get_result(self, row):
        body = self.body(row)
        body = json.dumps(body)
        try:
            answer = requests.post(self.url, data=body, headers=self.headers, timeout=180)
            if answer.status_code != 200:
                return None
            result = answer.json()
        except Exception as ex:
            logging.error(f'Ошибка {ex}')
            return None
        return result

    def get_port(self):
        self.add_new_columns()
        logging.info("Запросы к микросервису")
        data = {}
        lines = reel_shipping_list_name()
        for index, row in self.df.iterrows():
            if row.get('line').upper() not in lines or row.get('tracking_seaport') is not None:
                continue
            if row.get('consignment', False) not in data:
                data[row.get('consignment')] = {}
                if row.get('enforce_auto_tracking', True):
                    port = self.get_result(row)
                    self.write_port(index, port)
                    try:
                        data[row.get('consignment')].setdefault('tracking_seaport',
                                                                self.df.get('tracking_seaport')[index])
                        data[row.get('consignment')].setdefault('is_auto_tracking',
                                                                self.df.get('is_auto_tracking')[index])
                        data[row.get('consignment')].setdefault('is_auto_tracking_ok',
                                                                self.df.get('is_auto_tracking_ok')[index])
                    except KeyError as ex:
                        logging.info(f'Ошибка при получение ключа из DataFrame {ex}')
            else:
                tracking_seaport = data.get(row.get('consignment')).get('tracking_seaport') if data.get(
                    row.get('consignment')) is not None else None
                is_auto_tracking = data.get(row.get('consignment')).get('is_auto_tracking') if data.get(
                    row.get('consignment')) is not None else None
                is_auto_tracking_ok = data.get(row.get('consignment')).get('is_auto_tracking_ok') if data.get(
                    row.get('consignment')) is not None else None
                self.df.at[index, 'tracking_seaport'] = tracking_seaport
                self.df.at[index, 'is_auto_tracking'] = is_auto_tracking
                self.df.at[index, 'is_auto_tracking_ok'] = is_auto_tracking_ok
        logging.info('Обработка закончена')

    def write_port(self, index, port):
        self.df.at[index, 'is_auto_tracking'] = True
        if port:
            self.df.at[index, 'is_auto_tracking_ok'] = True
            self.df.at[index, 'tracking_seaport'] = port
        else:
            self.df.at[index, 'is_auto_tracking_ok'] = False

    @staticmethod
    def check_line(line):
        if line not in LINES:
            return True
        return False

    def add_new_columns(self):
        if "enforce_auto_tracking" not in self.df.columns:
            self.df['is_auto_tracking'] = None
