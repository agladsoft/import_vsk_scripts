import json
import logging

import requests

LINES = ['REEL SHIPPING', 'СИНОКОР РУС ООО', 'HEUNG-A LINE CO., LTD', 'MSC', 'SINOKOR']
IMPORT = ['импорт','import']

class Parsed:
    def __init__(self, df):
        self.df = df
        self.url = "http://10.23.4.203:8004"
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
        for index, row in self.df.iterrows():
            if self.check_line(row['line']):
                continue
            print(index)
            port = self.get_result(row)
            self.write_port(index, row, port)

    def write_port(self, index, row, port):
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
        self.df['is_auto_tracking'] = None