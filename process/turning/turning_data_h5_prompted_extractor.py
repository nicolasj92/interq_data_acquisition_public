import numpy as np
import pandas as pd
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import matplotlib.pyplot as plt
plt.switch_backend("TkAgg")
import h5py
import time as tm
import math
import csv
import numba
import os

BASEPATH = '/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/data/cip_dmd/piston_rod/cnc_lathe/process_data'
summer_time = True

if summer_time:
    time_zone_offset = 7200 # summer time germany
else:
    time_zone_offset = 3600  # winter time germany


def convert_to_unix(timestamp):
    date, time = timestamp.split(" ")
    day, month, year = date.split("-")
    hour, min, sec = time.split(":")
    dt = datetime(int(year), int(month), int(day), int(hour), int(min), int(sec)).timetuple()
    return tm.mktime(dt)


prompt_file = h5py.File("data/turning_process_data.h5", 'r')
# all keys from 0 to incl. 886 are summer time keys, rest are winter time
# files between 875 and 890 are not yet downloaded, because data in that region is split up in different influxdb tables
# split between 870 and 880 
keys = list(prompt_file.keys())
print(len(keys))
keys = ["202804"]

positions = []

for key_idx, key in enumerate(keys):
    positions.append([prompt_file[key][0][0], prompt_file[key][0][-1]])
print(positions[0][0])
THRESHOLD = 2000
sec_to_nanosec_factor = 1000000000
TIME_OFFSET = 0
influx_stamp_start = str(int(sec_to_nanosec_factor * (positions[0][0] - THRESHOLD - TIME_OFFSET)))
influx_stamp_stop = str(int(sec_to_nanosec_factor * (positions[-1][1] + THRESHOLD - TIME_OFFSET)))
print("downloading data from: " +str(datetime.fromtimestamp(positions[0][0] - THRESHOLD - TIME_OFFSET).strftime('%Y-%m-%d %H:%M:%S')))
print("until: " +str(datetime.fromtimestamp(positions[-1][1] + THRESHOLD - TIME_OFFSET).strftime('%Y-%m-%d %H:%M:%S')))

token = "XXX"
org = "PTW TU Darmstadt"
bucket = "interq_kompaki_prod"

client = InfluxDBClient(url="XXX", token=token, org=org)
query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + influx_stamp_start + "), stop: time(v: " + influx_stamp_stop + ")) |> filter(fn: (r) => r[\"_measurement\"] == \"cip_bfc_mqtt\")  |> filter(fn: (r) => r[\"_field\"] == \"NCLine\") "
tables = client.query_api().query(query, org=org)



table_lengths = []
for table_index, table in enumerate(tables):
    table_lengths.append(len(table.records))

    if table.records[0]["_field"] != "NCLine":
        print("check query, received non-NCLine field in first request")
if(len(table_lengths) > 1):
    print("received multiple tables but expected one")


timestamps = []
nclines = []
for table_length in table_lengths:
    timestamps.append(np.zeros(table_length))
    nclines.append(np.zeros(table_length))
  
# extract ncline data
for table_index, table in enumerate(tables):
    for record_index, record in enumerate(table):
        # we are only interested in NC Line numerical values, stripped of the "N"
        if record["_field"] == "NCLine":
            timestamps[table_index][record_index] = float(tm.mktime(record["_time"].timetuple()) + time_zone_offset)
            ncline = str(record["_value"])
            if len(ncline) < 1 or ncline == "None" or ncline == None:
                nclines[table_index][record_index] = 0
            else:
                nclines[table_index][record_index] = float(ncline[1:])

print("received data from: " + str(datetime.fromtimestamp(timestamps[0][0]).strftime('%Y-%m-%d %H:%M:%S')))
print("until: " + str(datetime.fromtimestamp(timestamps[-1][-1]).strftime('%Y-%m-%d %H:%M:%S')))

for i in range(len(table_lengths)):
    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.set_title("Preview of data in table " + str(i))

    ax.plot(timestamps[i], nclines[i])
    ax.scatter(np.array(positions)[:,0], np.ones(len(positions)), color = "green")
    ax.scatter(np.array(positions)[:,1], np.ones(len(positions)), color = "red")
    plt.show()

table_download_idx = 0
if len(table_lengths) > 1:
    print("enter index [0...n] of the table you want to download data from")
    table_download_idx = input()

print("download data? y/n")
if input() == "y":
    with InfluxDBClient(url="XXX", token=token, org=org) as client:
        for i in range(len(keys)):
            #try:
            if True:

                field_keys = ["timestamp"]
                time_start = str(int(sec_to_nanosec_factor *int(positions[i][0])))
                time_end = str(int(sec_to_nanosec_factor * int(positions[i][1])))
                print("downloading data from " + str(positions[i][0]) + " til " + str(positions[i][1]))
                query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + time_start + "), stop: time(v: " + time_end + "))   |> filter(fn: (r) => r[\"_measurement\"] == \"cip_bfc_mqtt\")"
                tables = client.query_api().query(query, org=org)


                # get the names of all the fields
                for table in tables:
                    field_keys.append(table.records[0]['_field'])

                # make sure there are no duplicate fields in query response
                assert(len(field_keys) == len(set(field_keys)))

                timestamps = np.zeros(len(table.records), dtype = np.float64)

                data = np.zeros((len(field_keys), len(table.records)), dtype = np.float64)

                # extract relevant data
                for table_index, table in enumerate(tables):

                    for record_index, record in enumerate(table):
                        if table_index == 0:
                            #only need one set of timestamps, rest of fields all have same timestamps
                            timestamps[record_index] = float(tm.mktime(record["_time"].timetuple()) + time_zone_offset + record["_time"].microsecond/1e6)


                            # we dont wan't strings mixed into our h5 data set
                        if record["_field"] == "NCLine":
                            ncline = str(record["_value"])
                            if len(ncline) < 1 or ncline == "None" or ncline == None:
                                data[table_index][record_index] = 0
                            else:
                                data[table_index][record_index] = float(ncline[1:]) #so we  convert ncline into a number
                
                        elif record["_field"] == "ProgramName":
                            data[table_index][record_index] = 0 # and leave the program name out (its the same everywhere anyway)

                        else:
                            data[table_index][record_index] = float(record["_value"])

                ts = datetime.fromtimestamp(timestamps[0])
                all_data = np.vstack((timestamps, data))

                plt.plot(all_data[0], all_data[1])
                plt.show()
                name = keys[i] + "_" + str(ts.month) + "_" + str(ts.day) + "_" + str(ts.year) + "_" + str(ts.hour) + "_" + str(ts.minute) + "_" + str(ts.second)
                os.mkdir(os.path.join(BASEPATH, name))
                
                with h5py.File(os.path.join(BASEPATH, name, "internal_machine_signals.h5"), 'w') as hf:
                    hf.create_dataset("data", data = all_data.T)
                    hf["data"].attrs["column_names"] = field_keys
                    hf.close()
                    print("successfully stored data of process with process id: " + keys[i])
        
                print("remaining: " + str(len(positions) - i) + " processes")
                """#except Exception as e:
                print(e)
                with open("error_list.txt", 'a') as el:
                    el.write("--" + str(keys[i]) + " --")"""

else:
    exit()
            

            

