from confluent_kafka import Consumer
import json
import psycopg2
from psycopg2.extensions import AsIs
import os

def replace_str(get_string: str):
    return get_string.replace("\\", "").replace("[", "").replace("]", "").replace(":", "_").replace(".", "p")

def add_columns(tablename: str, dictionary: dict, cursor):
    columns = dictionary.keys()
    for column in columns:
        if column != "moment":
            if column[-3:1:1] != ".1":
                cursor.execute('''ALTER TABLE %s ADD COLUMN %s double precision''' % (tablename, replace_str(column)))
            else:
                cursor.execute('''ALTER TABLE %s ADD COLUMN %s integer''' % (tablename, replace_str(column)))
        else:
            cursor.execute('''ALTER TABLE %s ADD COLUMN %s timestamp''' % (tablename, column))



def add_data(tablename: str, dictionary: dict, cursor):
    columns = dictionary.keys()
    values = [dictionary[key] for key in columns]
    if None not in columns and None not in values:
        cursor.execute('''INSERT INTO %s (%s) values %s''' % (tablename, AsIs(replace_str(','.join(columns))), tuple(values)))
        print("addded")

def read_all_messages(cursor, consumer, create_columns=False):
    print("Starting reading data...")
    while True:
        msg = consumer.poll()
        if msg is None:
            print("msg is None!")
            continue
        elif msg.error():
            print("msg.error() occured!")
            continue
        json_statham = json.loads(msg.value())
        if create_columns:
            add_columns("data_all", json_statham, cursor)
            create_columns=False
        add_data("data_all", json_statham, cursor)

def main():
    dbname="predictiondb"
    conn = psycopg2.connect(
        dbname=dbname, user='postgres', password='postgrepassword', 
        host='127.0.0.1', port= '5432'
    )
    conn.autocommit = True
    cursor = conn.cursor()
    current_dir = os.getcwd()
    conf = {'bootstrap.servers': 'rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091',
            'group.id': "CopyPasters",
            'enable.auto.commit': True,
            'auto.offset.reset': 'beginning',
            'security.protocol': 'sasl_ssl',
            'sasl.mechanism': 'SCRAM-SHA-512',
            'ssl.ca.location': os.path.join(current_dir, "CA.txt"),
            'sasl.username': '9433_reader',
            'sasl.password': 'eUIpgWu0PWTJaTrjhjQD3.hoyhntiK',
            'session.timeout.ms': 60000}

    consumer = Consumer(conf)

    consumer.subscribe(['zsmk-9433-dev-01'])


    read_all_messages(cursor, consumer, False)

    consumer.close()
    cursor.close()

class DataReader:
    def __init__(self) -> None:
        dbname="predictiondb"
        self.conn = psycopg2.connect(
            dbname=dbname, user='postgres', password='postgrepassword', 
            host='127.0.0.1', port= '5432'
        )
        self.cursor = self.conn.cursor()
        current_dir = os.getcwd()
        conf = {'bootstrap.servers': 'rc1a-b5e65f36lm3an1d5.mdb.yandexcloud.net:9091',
                'group.id': "CopyPasters",
                'enable.auto.commit': True,
                'auto.offset.reset': 'beginning',
                'security.protocol': 'sasl_ssl',
                'sasl.mechanism': 'SCRAM-SHA-512',
                'ssl.ca.location': os.path.join(current_dir, "CA.txt"),
                'sasl.username': '9433_reader',
                'sasl.password': 'eUIpgWu0PWTJaTrjhjQD3.hoyhntiK',
                'session.timeout.ms': 60000}
        self.consumer = Consumer(conf)
        self.consumer.subscribe(['zsmk-9433-dev-01'])

    def add_columns(self, tablename: str, dictionary: dict, cursor):
        columns = dictionary.keys()
        for column in columns:
            if column != "moment":
                if column[-3:1:1] != ".1":
                    cursor.execute('''ALTER TABLE %s ADD COLUMN %s double precision''' % (tablename, replace_str(column)))
                else:
                    cursor.execute('''ALTER TABLE %s ADD COLUMN %s integer''' % (tablename, replace_str(column)))
            else:
                cursor.execute('''ALTER TABLE %s ADD COLUMN %s timestamp''' % (tablename, column))
    
    def add_data(self, tablename: str, dictionary: dict, cursor):
        columns = dictionary.keys()
        values = [dictionary[key] for key in columns]
        if None not in columns and None not in values:
            cursor.execute('''INSERT INTO %s (%s) values %s''' % (tablename, AsIs(replace_str(','.join(columns))), tuple(values)))
            print("addded")

    def read_data(self, create_columns=False):
        msg = self.consumer.poll()
        if msg is None:
            print("msg is None!")
            return
        elif msg.error():
            print("msg.error() occured!")
            return
        json_data = json.loads(msg.value())
        if create_columns:
            add_columns("data_all", json_data, self.cursor)
            create_columns=False
        add_data("data_all", json_data, self.cursor)
        return json_data


