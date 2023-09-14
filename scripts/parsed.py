import json
import logging

import requests

LINES = ['REEL SHIPPING', 'СИНОКОР РУС ООО', 'HEUNG-A LINE CO., LTD', 'MSC', 'SINOKOR']
IMPORT = ['импорт','import']

class Parsed:
    def __init__(self, df):
        self.df = df
        self.url = "http://service_consignment:8004"
        self.headers = {
            'Content-Type': 'application/json'
        }

    def get_direction(self,direction):
        if direction.lower() in IMPORT:
            return 'import'
        else:return 'export'

    def body(self, row):
        data = {
            'line': row['line'],
            'consignment': row['consignment'],
            'direction': self.get_direction(row['direction'])

        }
        return data

    def get_result(self, row):
        body = self.body(row)
        body = json.dumps(body)
        try:
            answer = requests.post(self.url, data=body, headers=self.headers)
            if answer.status_code != 200:
                return None
            result = answer.json()
        except Exception as ex:
            logging.info(f'Ошибка {ex}')
            return None
        return result

    def get_port(self):
        self.add_new_columns()
        logging.info("Запросы к микросервису")
        data = {}
        for index, row in self.df.iterrows():
            if row.get('consignment',False) not in data:
                data[row.get('consignment')] = {}
                if row.get('enforce_auto_tracking', True):
                    port = self.get_result(row)
                    self.write_port(index,port)
                    data[row.get('consignment')]['tracking_seaport'] = row.get('tracking_seaport')
                    data[row.get('consignment')]['is_auto_tracking'] = row.get('is_auto_tracking')
                    data[row.get('consignment')]['is_auto_tracking_ok'] = row.get('is_auto_tracking_ok')
            else:
                tracking_seaport = data.get(row.get('consignment')).get('tracking_seaport') if data.get(
                    row.get('consignment')) is not None else None
                is_auto_tracking = data.get(row.get('consignment')).get('is_auto_tracking') if data.get(
                    row.get('consignment')) is not None else None
                is_auto_tracking_ok = data.get(row.get('consignment')).get('is_auto_tracking_ok') if data.get(
                    row.get('consignment')) is not None else None
                row.setdefault('tracking_seaport', tracking_seaport)
                row.setdefault('is_auto_tracking', is_auto_tracking)
                row.setdefault('is_auto_tracking_ok', is_auto_tracking_ok)
        logging.info('Обработка закончена')

    def write_port(self, index, port):
        self.df.at[index, 'is_auto_tracking'] = True
        if port:
            self.df.at[index, 'is_auto_tracking_ok'] = True
            self.df.at[index, 'tracking_seaport'] = port
        else:
            self.df.at[index, 'is_auto_tracking_ok'] = False

    def check_line(self, line):
        if line not in LINES:
            return True
        return False

    def add_new_columns(self):
        if "enforce_auto_tracking" not in self.df.columns:
            self.df['is_auto_tracking'] = None