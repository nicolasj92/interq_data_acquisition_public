import os
import argparse
from pathlib import Path
import numpy as np
import pandas as pd
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import matplotlib.pyplot as plt
import h5py
import time as tm
import pickle
import csv
import stumpy
import scipy
from dotenv import load_dotenv
from scipy.signal import find_peaks
import pytz
from datetime import timedelta
from typing import List
import logging

import extract_utils

load_dotenv()
INFLUX_API_TOKEN = os.getenv('INFLUXDB_API_TOKEN')
INFLUX_ORG = os.getenv('INFLUXDB_ORG')
INFLUX_BUCKET = os.getenv('INFLUXDB_BUCKET')
INFLUX_URL = os.getenv('INFLUXDB_URL')
PROJECT_ROOT = os.getenv('PROJECT_ROOT')
MEASUREMENT_NAME = 'cip_bfc_mqtt'

logging.basicConfig(format='[%(levelname)s]%(filename)s line %(lineno)s:%(message)s', level=logging.DEBUG)
logger = logging.getLogger(__name__)


def setup_influx_client(url, token, org):
    logger.info(f"Setting up InfluxDB client with url: {url}, token: {token}, org: {org}")
    client = InfluxDBClient(url=url, token=token, org=org)
    return client


def get_time_range(start_time, end_time):
    start_time = extract_utils.convert_to_rfc3339(str(start_time))
    end_time = extract_utils.convert_to_rfc3339(str(end_time))
    return start_time, end_time


def identify_batches(db_client, start_time, end_time, measurement_name, field_name, bucket_name, org, filters=[]):
    def preprocess_data(df):
        df.sort_values(by='time', inplace=True)
        df.reset_index(drop=True, inplace=True)

    query = extract_utils.assemble_query(
        start_time=start_time,
        stop_time=end_time,
        bucket_name=bucket_name,
        measurement_name=measurement_name,
        field_name=field_name,
        filters=filters
    )
    extract_utils.pretty_print_flux_query(query)
    _start_time = tm.time()
    table = db_client.query_api().query(query, org=org)
    _end_time = tm.time()
    logger.info(f"Query time: {_end_time - _start_time} seconds")

    field_data = {
        'time': [record.get_time() for record in table[0].records],
        field_name: [record.get_value() for record in table[0].records]
    }

    field_data = pd.DataFrame(field_data)
    preprocess_data(field_data)

    # Convert the field data to integer, e.g. for 'NCLine`, the original data look like 'N1', 'N2', 'N3', etc.
    field_data.loc[:, field_name] = field_data[field_name].apply(lambda x: int(x[1:] if x else 0))

    peaks, peaks_properties, troughs, troughs_properties = extract_utils.find_peaks_troughs(field_data['NCLine'],
                                                                                            distance=50, prominence=1)
    batch_identifiers = extract_utils.identify_batch(field_data, peaks, troughs)
    logger.info(f"Identified {len(batch_identifiers)} batches, i.e., {len(batch_identifiers) * 4} products")
    #

def main(args):
    db_client = setup_influx_client(INFLUX_URL, INFLUX_API_TOKEN, INFLUX_ORG)
    start_time, end_time = get_time_range(args.query_start_time, args.query_end_time)
    filters = ['|> group()']
    identify_batches(
        db_client,
        start_time,
        end_time,
        MEASUREMENT_NAME,
        'NCLine',
        INFLUX_BUCKET,
        INFLUX_ORG,
        filters=filters)


def arg_parse():
    parser = argparse.ArgumentParser(description='Split sequence data')
    parser.add_argument('--input', type=str, help='Input file path')
    parser.add_argument('--query_start_time', type=str, default='2022-09-06 08:00:00',
                        help='Start time of query, the format is %Y-%m-%d %H:%M:%S')
    parser.add_argument('--query_end_time', type=str, default='2022-11-04 22:00:00',
                        help='End time of query, the format is %Y-%m-%d %H:%M:%S')

    return parser.parse_args()


if __name__ == '__main__':
    args = arg_parse()
    main(args)
