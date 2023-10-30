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
import pickle
import csv
import stumpy
import scipy

save_field_keys = False


df = pd.read_csv('data/01_Sawing_Machine.csv')
n_scans = df.shape[0]
hf = h5py.File("data/sawing_process_patterns.h5", 'r')
inter_process_pattern = np.array([hf['inter_process_pattern']])[0]
patterns = [inter_process_pattern]
time_zone_offset = 7200 # summer time germany
#time_zone_offset = 3600  # winter time germany


def convert_to_unix(timestamp):
    date, time = timestamp.split(" ")
    day, month, year = date.split("-")
    hour, min, sec = time.split(":")
    dt = datetime(int(year), int(month), int(day), int(hour), int(min), int(sec)).timetuple()
    return tm.mktime(dt)


scan_stamps = []
scan_ids = []
for i in range(n_scans):
    scan_stamps.append(convert_to_unix(str(df.iat[i,1])))
    scan_ids.append(int(df.iat[i,0]))

scan_data = np.array([scan_stamps, scan_ids])

def getPeaks(nclines, timestamps, prominenz):
    gradient = np.gradient(nclines)
    peaks = scipy.signal.find_peaks(gradient, prominence = prominenz)
    listpeaks = [0]
    for i in range(len(peaks[0])):
        listpeaks.append(peaks[0][i])
    return listpeaks

THRESHOLD = 2000
sec_to_nanosec_factor = 1000000000
TIME_OFFSET = 0
influx_stamp_start = str(int(sec_to_nanosec_factor * (scan_stamps[0] - THRESHOLD - TIME_OFFSET)))
influx_stamp_stop = str(int(sec_to_nanosec_factor * (scan_stamps[-1] + THRESHOLD - TIME_OFFSET)))
print("downloading data from: " +str(datetime.fromtimestamp(scan_stamps[0] - THRESHOLD).strftime('%Y-%m-%d %H:%M:%S')))
print("until: " +str(datetime.fromtimestamp(scan_stamps[len(scan_stamps)-1] + THRESHOLD).strftime('%Y-%m-%d %H:%M:%S')))

token = "XXX"
org = "PTW TU Darmstadt"
bucket = "interq_kompaki_prod"

client = InfluxDBClient(url="XXX", token=token, org=org)
query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + influx_stamp_start + "), stop: time(v: " + influx_stamp_stop + ")) |> filter(fn: (r) => r[\"_measurement\"] == \"sps_saw_conmon_opcua\")  |> filter(fn: (r) => r[\"_field\"] == \"Position\") "
tables = client.query_api().query(query, org=org)


n_tables = 0
data_size = 0
for table_index, table in enumerate(tables):
    print(table.records[0]["_field"])
    """if(table_index == 1):
        print("skipping table number " + str(table_index))
        break"""
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

# extract relevant data
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

scan_positions = []
for i in range(len(scan_data[0])):
    pos = np.searchsorted(timestamps, scan_data[0][i])
    scan_positions.append(pos)


positions = []
position_estimates = getPeaks(nclines, timestamps, 5)
open_position_estimates = position_estimates
scan_idx = 0
    
processes = []


ncline_request_timestamps = timestamps

def onKeyPress(event):
    global preview, fig, select_point, current_select, speed, state, start_marker, end_marker, start_point, end_point, mode, assist_pos, ax2, scan_idx, open_position_estimates, position_estimates
    if state == "done" and event.key == "c":
        processes.append({"start" : timestamps[start_marker], "end" : timestamps[end_marker], "scan_id" : scan_ids[scan_idx]})
        scan_idx += 1
        state = "begin"
        mode = "assist"
        if scan_idx == len(scan_ids):
            plt.close(fig)
        else:
            #sample.set_data([i for i in range(len(patterns[pattern_idx]))], patterns[pattern_idx])
            selected_scan.set_xdata(scan_positions[scan_idx])
            open_position_estimate_points.set_data(open_position_estimates, [1 for i in range(len(open_position_estimates))])
            preview.set_data([0, 0])
            start_marker = end_marker
            
            

    if event.key == "up":
        if speed < 10:
            speed += 1
        elif speed >= 10 and speed <= 100:
            speed += 10
    elif event.key == "down":
        if speed > 10:
            speed -= 10
        elif speed <= 10 and speed >= 1:
            speed -= 1

    elif event.key == "b":
        if mode == "free":
            current_select -= speed
        elif mode == "assist":
            if assist_pos  > 0:
                assist_pos -= 1
            current_select = open_position_estimates[assist_pos]
    elif event.key == "n":
        if mode == "free":
            current_select += speed
        elif mode == "assist":
            if assist_pos + 1 < len(open_position_estimates):
                assist_pos += 1
            current_select = open_position_estimates[assist_pos]
        
    elif event.key == "enter":
        if state == "begin":
            print("marker for start has been saved")
            start_marker = current_select
            if len(open_position_estimates) > 0:
                if current_select == open_position_estimates[assist_pos]:
                    del open_position_estimates[assist_pos]
                current_select = open_position_estimates[assist_pos]
            state = "end"
        elif state == "end":
            print("marker for end has been saved")
            end_marker = current_select
            if current_select == open_position_estimates[assist_pos]:
                del open_position_estimates[assist_pos]
            state = "done"
            preview.set_data(([i for i in range(len(nclines[start_marker:end_marker]))], nclines[start_marker:end_marker]))
    elif event.key == "m":
        if mode == "free":
            print("changing select mode to assist")
            mode = "assist"
        elif mode == "assist":
            print("changing mode to free")
            mode = "free"
    elif event.key == "r":
        state = "begin"
        mode = "assist"
        preview.set_data([0, 0])
        start_marker = 0
        end_marker = 0
        open_position_estimates = position_estimates
        current_select = open_position_estimates[0]
    elif event.key == "e":
        exit()

    if start_marker > 0:
        start_point.set_alpha(1)
    else:
        start_point.set_alpha(0)
    start_point.set_xdata(start_marker)

    if end_marker > 0:
        end_point.set_alpha(1)
    else:
        end_point.set_alpha(0)
    end_point.set_xdata(end_marker)

    if current_select > 0:
        select_point.set_alpha(1)
    else:
        select_point.set_alpha(0)
    select_point.set_xdata(current_select)

    if state == "done":
        ax.set_title("begin and end selected. press c to confirm selection, press r to reset")
    else:
        ax.set_title("Select process " + str(state) + " and press enter. Speed in free mode is: "+str(speed))
    #ax2.relim()
    #ax2.autoscale_view(True,True,True)
    fig.canvas.draw()


mode = "assist"
assist_pos = 0
start_marker = -1
end_marker = -1
current_select = open_position_estimates[0]
position_estimates = []
speed = 100
state = "begin"
#plt.ion()
fig = plt.figure()
ax = fig.add_subplot(611)
ax2 = fig.add_subplot(612)
ax3 = fig.add_subplot(613)
ax4 = fig.add_subplot(614)
ax5 = fig.add_subplot(615)
ax6 = fig.add_subplot(616)
#sample, = ax2.plot(patterns[pattern_idx])
preview, = ax2.plot([], color = "red")
preview, = ax2.plot([], color = "black")
preview, = ax2.plot([], color = "black")
preview, = ax2.plot([], color = "black")
preview, = ax2.plot([], color = "black")

#ax.set_autoscale_on(True)
#ax.autoscale_view(True,True,True)
ax2.set_autoscale_on(True)
ax2.autoscale_view(True,True,True)
ax2.set_title("Preview of sample pattern (blue) and selected pattern (red)")

fig.canvas.mpl_connect('key_press_event', onKeyPress)
fig.canvas.get_tk_widget().focus_set()

ax.plot(nclines)
open_position_estimate_points, = ax.plot(open_position_estimates, [1 for i in range(len(open_position_estimates))], color = 'orange', marker = 'o', linestyle='None')
selected_scan, = ax.plot(scan_positions[scan_idx], 1, color = 'black', marker = 'o', markersize = 10)
ax.scatter(scan_positions, [1 for i in range(len(scan_positions))], color = 'black')
"""for i in range(len(positions)):
    if(positions[i][1] == "start" or positions[i][1] == "end"):
        ax.annotate(positions[i][1], (positions[i][0], -300), fontsize = 4)
    else:
        ax.annotate(positions[i][1], (positions[i][0], -500), fontsize = 4)"""


start_point,  = ax.plot(start_marker, 1, color = 'orange', marker = 'o', alpha = 0)
end_point,  = ax.plot(end_marker, 1, color = 'red', marker = 'o', alpha = 0)
select_point,  = ax.plot(current_select, 1, color = 'green', marker = 'o', markersize = 8)

print("please select data range")
print("use arrow keys to move cursor and to change cursor speed when in free")
print("press m to toggle between assist and free mode")
print("press r to reset")
plt.show()

print(processes)

print("correlated " + str(len(processes)) + " complete processes with " + str(n_scans) + " scans. download complete process data and store in h5? (y/n)")
choice = "x"
while not choice == "y":
    if choice == "n":
        exit()
    choice = input().lower()

print(processes)
if choice == "y":
    final_field_keys = []
    field_keys_initialized = False
    with InfluxDBClient(url="XXX", token=token, org=org) as client:
        for i in range(len(processes)):
            field_keys = []
            process = processes[i]
            time_start = str(int(sec_to_nanosec_factor *int(process["start"])))
            time_end = str(int(sec_to_nanosec_factor * int(process["end"])))
            print(time_start)
            print(time_end)
            query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + time_start + "), stop: time(v: " + time_end + "))   |> filter(fn: (r) => r[\"_measurement\"] == \"sps_saw_conmon_opcua\")"
            tables = client.query_api().query(query, org=org)


            # get the names of all the fields
            for table in tables:
                if(table.records[0]["_field"] == "Position"):
                    print("setting datasize: ")
                    data_size = len(table.records)
                #print("table len: "  + str(len(table.records)))
                field_keys.append(table.records[0]['_field'])
                #print("field key: "  + str(table.records[0]['_field']))

            """timestamps = np.array([None for j in range(len(table.records))])

            data = np.array([[None for j in range(len(table.records))] for k in range(len(field_keys))])

            # extract relevant data
            for table_index, table in enumerate(tables):
                for record_index, record in enumerate(table):
                    if table_index == 0:
                        #only need one set of timestamps, rest of fields all have same timestamps
                        timestamps[record_index] = record["_time"]
                    if math.isnan(record["_value"]):
                        print("nan value" + str(record["_value"]) + " in meas " + str(record["_field"]))
                    else:
                        data[table_index][record_index] = float(record["_value"])
                        # we dont wan't strings mixed into our h5 data set
                    if record["_field"] == "Position":
                        ncline = str(record["_value"])
                        if len(ncline) < 1 or ncline == "None" or ncline == None:
                            data[table_index][record_index] = 0
                        else:
                            data[table_index][record_index] = float(ncline[1:]) #so we  convert ncline into a number
            
                    #elif record["_field"] == "ProgramName":
                    #    data[table_index][record_index] = 0 # and leave the program name out (its the same everywhere anyway)

                    #else:
                    #    data[table_index][record_index] = float(record["_value"])

            for index, timestamp in enumerate(timestamps):
                timestamps[index] = float(tm.mktime(timestamp.timetuple()) + time_zone_offset)"""

            data_list = []
            # extract relevant data
            for table_index, table in enumerate(tables):
                nanfound = False
                timestamps = np.array([None for j in range(len(table.records))])
                data = np.array([None for j in range(len(table.records))])
                for record_index, record in enumerate(table):
                    try:
                        if math.isnan(float(record["_value"])):
                            print("none found in value")
                            nanfound = True
                            break
                        if record["_time"] == None:
                            print("none found in time")
                            nanfound = True
                            break
                        if math.isnan(float(tm.mktime(record["_time"].timetuple()) + time_zone_offset)):
                            print("none found in time conversion")
                            nanfound = True
                            break
                    except:
                        nanfound = True
                        break
                    timestamps[record_index] = record["_time"]
                    data[record_index] = float(record["_value"])
                                        
                if nanfound:
                    print("nanfound in field: " + str(field_keys[table_index]))
                    del field_keys[table_index]
                else:
                    for index, timestamp in enumerate(timestamps):
                        if timestamp == None:
                            print("None value at idx: " + str(index))
                            exit()
                        else:
                            print(timestamp)
                            print(timestamp.timetuple())
                            timestamps[index] = float(tm.mktime(timestamp.timetuple()) + time_zone_offset)
                    data_list.append([data, timestamps])
            if not field_keys_initialized:
                final_field_keys = field_keys
                field_keys_initialized = True

            with h5py.File("data/sawing_process_data.h5", 'a') as hf:
                float_array_dt = h5py.vlen_dtype(np.dtype('float64'))

                try:
                    if str(int(process["scan_id"])) in hf:
                        print("deleting dataset " + str(str(int(process["scan_id"]))) + " because it already exists")
                        del hf[str(int(process["scan_id"]))]
                    dset = hf.create_dataset(str(int(process["scan_id"])), (len(field_keys), 2), dtype=float_array_dt)
                    print("saving process with id: " + str(int(process["scan_id"])))

                    for hakan in range(len(field_keys)):
                        try:
                            hakanarray = np.array([data_list[hakan][0], data_list[hakan][1]], dtype=np.dtype('float64'))
                            dset[hakan] = hakanarray
                        except:
                            print("setting dataset failed for: " +str(hakan) + " " + str(len(field_keys)) + " " + str(len(data_list)))
                        
                except:
                    print("couldn't save process with id: " + str(int(process["scan_id"])))
                hf.close()
            print("remaining: " + str(len(processes) - i) + " processes")

    if(save_field_keys):
        with open("field_keys.csv", 'w', newline='') as f:
            write = csv.writer(f)
            write.writerows(final_field_keys)
            

            

