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

field_keys_dict = {
    "CPU_Kuehler_Temp" : "CPU_cooler_temp",
    "CPU_Temp" : "CPU_temp",
    "CutCounter" : "CutCounter",
    "CutTime" : "CutTime",
    "FFT_Anforderung" : "FFT_requirement",
    "FlatstreamCutCounter" : "FlatstreamCutCounter",
    "FlatstreamDone" : "FlatstreamDone",
    "FsMode_1Raw_2FftRaw_3FttHK" : "FsMode_1Raw_2FftRaw_3FttHK",
    "HebenAktiv" : "lift_active",
    "MotorAn" : "motor_on",
    "PData.CosPhi" : "PData.CosPhi",
    "PData.CutEnergy" : "PData.CutEnergy",
    "PData.PEff" : "PData.PEff",
    "P_Vorschub" : "P_feed",
    "Position" : "Position",
    "Position_Band" : "blade_position",
    "TData.T1" : "TData.T1",
    "TData.T2" : "TData.T2",
    "TData.T3" : "TData.T3",
    "TData.T4" : "TData.T4",
    "TData.T_IR" : "TData.T_IR",
    "Vib01.CREST" : "Vib01.CREST",
    "Vib01.Kurtosis" : "Vib01.Kurtosis",
    "Vib01.Peak" : "Vib01.Peak",
    "Vib01.RMS" : "Vib01.RMS",
    "Vib01.Skewness" : "Vib01.Skewness",
    "Vib01.VDI3832" : "Vib01.VDI3832",
    "Vib02.CREST" : "Vib02.CREST",
    "Vib02.Kurtosis" : "Vib02.Kurtosis",
    "Vib02.Peak" : "Vib02.Peak",
    "Vib02.RMS" : "Vib02.RMS",
    "Vib02.Skewness" : "Vib02.Skewness",
    "Vib02.VDI3832" : "Vib02.VDI3832",
    "Vib03.CREST" :  "Vib03.CREST",
    "Vib03.Kurtosis" : "Vib03.Kurtosis",
    "Vib03.Peak" : "Vib03.Peak",
    "Vib03.RMS" : "Vib03.RMS",
    "Vib03.Skewness" : "Vib03.Skewness",
    "Vib03.VDI3832" : "Vib03.VDI3832",
    "ZaehneProBand" : "teeth_per_blade",
    "bCutActive" : "bCutActive",
    "fLichtschranke" : "f_light_barrier",
    "obereMaterialkante" : "top_material_edge",
    "vVorschub" : "v_feed"
}

BASEPATH = 'Z:\cip_dmd\cylinder_bottom\saw\process_data'
summer_time = False

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

#@numba.jit(nopython=True)
def sampleData(values, slave_timestamps, master_timestamps, field_key_name, process_key):
    sampled_data = np.zeros_like(master_timestamps)
    offset = 0
    for slave_timestamp_idx, master_timestamp in enumerate(master_timestamps):
        # counter is just for safety measure
        counter = 0
        # the varying data fields have slightly varying lengths, that could mean that timestamps with same index could not be from same time
        # to sample points at equal timestamps we choose the closest neighbour if a data point is missing
        tolerance = 0.6
        if slave_timestamp_idx + offset >= len(slave_timestamps) - 1:
            offset -= 1
        timestamp_difference = master_timestamp - slave_timestamps[slave_timestamp_idx + offset]
        while(abs(timestamp_difference) > tolerance):
            direction = np.sign(timestamp_difference)
            if direction > 0 and slave_timestamp_idx + offset < len(slave_timestamps) - 1:
                offset += 1
            elif direction < 0 and slave_timestamp_idx + offset > 0:
                offset -= 1
            timestamp_difference = master_timestamp - slave_timestamps[slave_timestamp_idx + offset]

            counter += 1    
            if counter >= 5:
                print("counter higher than 5 encountered in field " + str(field_key_name) + " in process " + str(process_key) + " . That could mean sampling tolerance is impossible to reach")
                break

        sampled_data[slave_timestamp_idx] = values[slave_timestamp_idx + offset]
    return sampled_data


prompt_file = h5py.File("data/sampled_saw_process_data.h5", 'r')
# all keys from 0 to 950 are summer time keys
keys = list(prompt_file.keys())[951:]

positions = []

for key_idx, key in enumerate(keys):
    if key == "124004":
        print("should be here " + str(key_idx))

    positions.append([prompt_file[key][0][0], prompt_file[key][0][-1]])

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
query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + influx_stamp_start + "), stop: time(v: " + influx_stamp_stop + ")) |> filter(fn: (r) => r[\"_measurement\"] == \"sps_saw_conmon_opcua\")  |> filter(fn: (r) => r[\"_field\"] == \"Position\") "
tables = client.query_api().query(query, org=org)


n_tables = 0
data_size = 0
for table_index, table in enumerate(tables):
    if table.records[0]["_field"] == "Position":
        n_tables += 1
        data_size = len(table.records)
if(n_tables > 1):
    print("the field Position is split into different tables in this time range")
elif(n_tables < 1):
    print("didnt receive any table")
    exit()

timestamps = np.array([None for j in range(data_size)])
data = np.array([None for j in range(data_size)])

# extract nclines
for table_index, table in enumerate(tables):
    """if(table_index == 1):
        print("skipping table number " + str(table_index))
        break"""
    for record_index, record in enumerate(table):
        # we are only interested in NC Line numerical values, stripped of the "N"
        if record["_field"] == "Position":
            timestamps[record_index] = record["_time"]
            #ncline = str(record["_value"])
            #if len(ncline) < 1 or ncline == "None" or ncline == None:
            #data[record_index] = 0
            #else:
            data[record_index] = float(record["_value"])

for index, timestamp in enumerate(timestamps):
    if timestamp == None:
        print("None value at idx: " + str(index))
    else:
        timestamps[index] = float(tm.mktime(timestamp.timetuple()) + time_zone_offset)
print("received data from: " + str(datetime.fromtimestamp(timestamps[0]).strftime('%Y-%m-%d %H:%M:%S')))
print("until: " + str(datetime.fromtimestamp(timestamps[len(timestamps)-1]).strftime('%Y-%m-%d %H:%M:%S')))

nclines = np.array([data], dtype = np.float64)[0]



fig = plt.figure()
ax = fig.add_subplot(111)

ax.set_title("Preview of data to be downloaded")

ax.plot(timestamps, nclines)

ax.scatter(np.array(positions)[:,0], np.ones(len(positions)), color = "green")
ax.scatter(np.array(positions)[:,1], np.ones(len(positions)), color = "red")
plt.show()
print("download data? y/n")
if input() == "y":
    field_keys_en = ["timestamp"]
    field_keys_initialized = False
    with InfluxDBClient(url="XXX", token=token, org=org) as client:
        for i in range(len(keys)):
            field_keys = []
            time_start = str(int(sec_to_nanosec_factor *int(positions[i][0])))
            time_end = str(int(sec_to_nanosec_factor * int(positions[i][1])))
            print("downloading data from " + str(positions[i][0]) + " til " + str(positions[i][1]))
            query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + time_start + "), stop: time(v: " + time_end + "))   |> filter(fn: (r) => r[\"_measurement\"] == \"sps_saw_conmon_opcua\")"
            tables = client.query_api().query(query, org=org)


            # get the names of all the fields
            for table in tables:
                field_keys.append(table.records[0]['_field'])

            data_list = []
            # extract relevant data
            for table_index, table in enumerate(tables):
                nanfound = False

                timestamps = np.array([None for j in range(len(table.records))])
                data = np.array([None for j in range(len(table.records))])
                for record_index, record in enumerate(table):
                    try:
                        if math.isnan(float(record["_value"])):
                            print("value with NaN-Value found")
                            nanfound = True
                            break
                        if record["_time"] == None:
                            print("timestamp with None-Value found")
                            nanfound = True
                            break
                        if math.isnan(float(tm.mktime(record["_time"].timetuple()))):
                            print("timestamp with NaN-Value found")
                            nanfound = True
                            break
                    except:
                        nanfound = True
                        break
                    timestamps[record_index] = record["_time"]
                    data[record_index] = float(record["_value"])
                                        
                if nanfound:
                    print("skipping field: " + str(field_keys[table_index]) + " because it has invalid entries")
                    del field_keys[table_index]
                else:
                    for index, timestamp in enumerate(timestamps):
                        if timestamp == None:
                            print("None value at idx: " + str(index))
                            exit()
                        else:
                            timestamps[index] = float(tm.mktime(timestamp.timetuple()) + time_zone_offset + float(timestamp.microsecond)/1e6)
                    data_list.append([data, timestamps])
            if not field_keys_initialized:
                for field_key in field_keys:
                    field_keys_en.append(field_keys_dict[field_key])
                field_keys_initialized = True

            master_sampling_field = 0
            min_len = 1000000000000000
            # choose master timestamp field for sampling
            lens = []
            for a in range(len(data_list)):
                lens.append(len(data_list[a][1]))
            lens = np.array(lens)
            mean_lens = np.array([np.mean(lens) for h in range(len(lens))])
            diff_to_mean_lens = np.abs(lens - mean_lens)
            idx_of_field_with_mean_amount_of_entrys = np.argmin(diff_to_mean_lens)
            master_timestamps = data_list[idx_of_field_with_mean_amount_of_entrys][1]
            sampled_data = [master_timestamps]
            # sample data
            for a in range(len(data_list)):
                data, slave_timestamps = data_list[a]
                field_key_name = field_keys_en[a + 1]
                sampled_data.append(sampleData(np.array(data), np.array(slave_timestamps), np.array(master_timestamps), field_key_name, keys[i]))
            assert(len(sampled_data) == len(field_keys_en))
            sampled_data = np.array(sampled_data, dtype = np.float64)
            ts = datetime.fromtimestamp(master_timestamps[0])
            name = keys[i] + "_" + str(ts.month) + "_" + str(ts.day) + "_" + str(ts.year) + "_" + str(ts.hour) + "_" + str(ts.minute) + "_" + str(ts.second)
            os.mkdir(os.path.join(BASEPATH, name))
        
            with h5py.File(os.path.join(BASEPATH, name, "internal_machine_signals.h5"), 'w') as hf:
                    hf.create_dataset("data", data = np.array(sampled_data.T), dtype = np.float64)
                    hf["data"].attrs["column_names"] = field_keys_en
                    hf.close()
                    print("successfully stored data of process with process id: " + keys[i])
      
            print("remaining: " + str(len(positions) - i) + " processes")

else:
    exit()
            

            

