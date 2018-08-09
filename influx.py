from ksql import KSQLAPI
from datetime import datetime
from influxdb import InfluxDBClient
import json
import requests
import time
import os

HOST = os.environ['ksql_host']
user = os.environ['ksql_user']
pwd = os.environ['ksql_pwd']

def get_request(url, sql, endpoint):
    url = '{}/{}'.format(url, endpoint)
    data = json.dumps({"ksql": sql})
    headers = {"Content-Type": "application/json"}
    stream = True if endpoint == 'query' else False    
    ret = requests.request(method="POST", url=url, data=data, timeout=5, headers=headers, stream=stream)
    return ret


def get_influx_connection():
    return InfluxDBClient(HOST, '8086', user, pwd, 'ksql')


def prepare_json_payload(chunk):
    live_dict = json.loads(str(chunk))
    current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

    json_body=[{"measurement": "goldstream",\
                "tags": {"host": "server01","region": "ap-east"},
                "time": current_time,
                "fields": {"amount": live_dict["row"]["columns"][2]}}]
    return json_body


def get_stream_data_from_ksql(sql):
    request_object = get_request('http://{}:8088'.format(HOST), sql, "query")
    client=get_influx_connection()
    for chunk in request_object.iter_content(chunk_size=128):
        if chunk != b'\n':
            json_body = prepare_json_payload(chunk.decode('utf-8'))
            client.write_points(json_body)

if __name__ == '__main__':
    #sql that will on the ksql client/ assumes there is a stream created manually
    sql = "select * from goldstream;"
    get_stream_data_from_ksql(sql)

