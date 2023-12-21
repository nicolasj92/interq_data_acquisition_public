import numpy as np
import pandas as pd
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import matplotlib.pyplot as plt
plt.switch_backend("TkAgg")
import h5py
import time as tm
import pickle
import csv
import stumpy
import scipy


df = pd.read_csv('data/prompt_scan_timestamps.csv')
print(df)
n_scans = df.shape[0]
hf = h5py.File("data/turning_process_patterns.h5", 'r')
part_1_pattern = np.array([hf['process_part_0_sample']])[0]
part_2_pattern = np.array([hf['process_part_1_sample']])[0]
part_3_pattern = np.array([hf['process_part_2_sample']])[0]
part_4_pattern = np.array([hf['process_part_3_sample']])[0]
patterns = [part_1_pattern, part_2_pattern, part_3_pattern, part_4_pattern]

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


scan_stamps = []
scan_ids = []
for i in range(n_scans):
    print(df.iat[i,1])
    scan_stamps.append(convert_to_unix(str(df.iat[i,1])))
    scan_ids.append(int(df.iat[i,0]))

scan_data = np.array([scan_stamps, scan_ids])

def getPeaks(reference_pattern, nclines, prominenz):
    distance_profile = stumpy.mass(reference_pattern, nclines)
    peaks = scipy.signal.find_peaks(-distance_profile, prominence = prominenz)
    return peaks[0]

THRESHOLD = 800
sec_to_nanosec_factor = 1000000000
influx_stamp_start = str(int(sec_to_nanosec_factor * (scan_stamps[0] - THRESHOLD)))
influx_stamp_stop = str(int(sec_to_nanosec_factor * (scan_stamps[-1] + THRESHOLD)))
print("downloading data from: " +str(datetime.fromtimestamp(scan_stamps[0] - THRESHOLD).strftime('%Y-%m-%d %H:%M:%S')))
print("until: " +str(datetime.fromtimestamp(scan_stamps[len(scan_stamps)-1] + THRESHOLD).strftime('%Y-%m-%d %H:%M:%S')))

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
    scan_positions = []
    for l in range(len(scan_data[0])):
        pos = np.searchsorted(timestamps[i], scan_data[0][l])
        scan_positions.append(pos)

    fig = plt.figure()
    ax = fig.add_subplot(111)

    ax.set_title("Preview of data in table " + str(i))

    ax.plot(nclines[i])
    ax.scatter(scan_positions, np.ones(len(scan_positions)), color = "green")
    plt.show()

table_download_idx = 0
if len(table_lengths) > 1:
    print("enter index [0...n] of the correct table")
    table_download_idx = input()

scan_positions = []
for l in range(len(scan_data[0])):
    pos = np.searchsorted(timestamps[table_download_idx], scan_data[0][l])
    scan_positions.append(pos)

nclines = np.array(nclines[table_download_idx], dtype = np.float64)


positions = None
start_position_estimates = None
end_position_estimates = None
positions_without_info = None
pattern_idx = 0

def setPositionsWithPattern(process_pattern):
    global start_position_estimates, end_position_estimates, positions, positions_without_info

    start_position_estimates = getPeaks(process_pattern, nclines, 10)
    if(len(start_position_estimates) == 0):
        start_position_estimates = [0]
    end_position_estimates = getPeaks(np.flip(process_pattern, 0), np.flip(nclines, 0), 10)
    if(len(end_position_estimates) == 0):
        end_position_estimates = [0]
    end_position_estimates_corrected = []
    for i in range(len(end_position_estimates)):
        end_position_estimates_corrected.append(len(nclines) - 1 - end_position_estimates[len(end_position_estimates) - i - 1])
    end_position_estimates = end_position_estimates_corrected

    positions = []

    positions.extend([(start_position_estimates[i], "start") for i in range(len(start_position_estimates))])
    positions.extend([(end_position_estimates[i], "end") for i in range(len(end_position_estimates))])

    positions = sorted(positions, key = lambda d: d[0])
    positions_without_info = []
    for i in range(len(positions)):
        positions_without_info.append(positions[i][0])

setPositionsWithPattern(patterns[pattern_idx])
processes = []


ncline_request_timestamps = timestamps
timestamps = timestamps[0]
def onKeyPress(event):
    global preview, fig, select_point, current_select, speed, state, start_marker, end_marker, start_point, end_point, mode, assist_pos, ax2, pattern_idx, sample, start_point_estimates, end_point_estimates, position_estimates, timestamps
    if state == "begin":
        position_estimates = start_position_estimates
    elif state == "end":
        position_estimates = end_position_estimates
    elif state == "done" and event.key == "c":
        print(len(timestamps))
        #timestamps = timestamps[0]
        print(start_marker)
        print(end_marker)
        print(timestamps)
        print(scan_ids)
        print(pattern_idx)
        processes.append({"start" : timestamps[start_marker], "end" : timestamps[end_marker], "scan_id" : scan_ids[pattern_idx]})
        pattern_idx += 1
        state = "begin"
        mode = "assist"
        if pattern_idx == len(patterns):
            plt.close(fig)
        else:
            setPositionsWithPattern(patterns[pattern_idx])
            sample.set_data([i for i in range(len(patterns[pattern_idx]))], patterns[pattern_idx])
            selected_scan.set_xdata(scan_positions[pattern_idx])
            start_point_estimates.set_data(start_position_estimates, [1 for i in range(len(start_position_estimates))])
            end_point_estimates.set_data(end_position_estimates, [1 for i in range(len(end_position_estimates))])
            preview.set_data([0, 0])
            start_marker = -1
            end_marker = -1
            position_estimates = start_position_estimates
            assist_pos = 0
            current_select = position_estimates[assist_pos]
            

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

    elif event.key == "left":
        if mode == "free":
            current_select -= speed
        elif mode == "assist":
            if assist_pos > 0:
                assist_pos -= 1
            current_select = position_estimates[assist_pos]
    elif event.key == "right":
        if mode == "free":
            current_select += speed
        elif mode == "assist":
            if assist_pos < len(position_estimates):
                assist_pos += 1
            current_select = position_estimates[assist_pos]
        
    elif event.key == "enter":
        if state == "begin":
            print("marker for start has been saved")
            start_marker = current_select
            state = "end"
            position_estimates = end_position_estimates
            assist_pos = 0
            current_select = end_position_estimates[assist_pos]
        elif state == "end":
            print("marker for end has been saved")
            end_marker = current_select
            state = "done"
            assist_pos = 0
            current_select = -10000
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
        position_estimates = start_position_estimates
        current_select = position_estimates[0]
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
    ax2.relim()
    ax2.autoscale_view(True,True,True)
    fig.canvas.draw()


mode = "assist"
assist_pos = 0
start_marker = -1
end_marker = -1
current_select = start_position_estimates[0]
position_estimates = []
speed = 100
state = "begin"
#plt.ion()
fig = plt.figure()
ax = fig.add_subplot(211)
ax2 = fig.add_subplot(212)
sample, = ax2.plot(patterns[pattern_idx])
preview, = ax2.plot([], color = "red")
ax.set_autoscale_on(True)
ax.autoscale_view(True,True,True)
ax2.set_autoscale_on(True)
ax2.autoscale_view(True,True,True)
ax2.set_title("Preview of sample pattern (blue) and selected pattern (red)")
fig.canvas.mpl_connect('key_press_event', onKeyPress)
fig.canvas.get_tk_widget().focus_set()

ax.plot(nclines)
start_point_estimates, = ax.plot(start_position_estimates, [1 for i in range(len(start_position_estimates))], color = 'orange', marker = 'o', linestyle='None')
end_point_estimates, = ax.plot(end_position_estimates, [1 for i in range(len(end_position_estimates))], color = 'yellow', marker = 'o', linestyle='None')
selected_scan, = ax.plot(scan_positions[pattern_idx], 1, color = 'black', marker = 'o', markersize = 10)
ax.scatter(scan_positions, [1 for i in range(len(scan_positions))], color = 'black')
for i in range(len(positions)):
    if(positions[i][1] == "start" or positions[i][1] == "end"):
        ax.annotate(positions[i][1], (positions[i][0], -300), fontsize = 4)
    else:
        ax.annotate(positions[i][1], (positions[i][0], -500), fontsize = 4)


start_point,  = ax.plot(start_marker, 1, color = 'red', marker = 'o', alpha = 0)
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
while choice not in ["y", "n"]:
    choice = input().lower()

if choice == "y":
    with InfluxDBClient(url="XXX", token=token, org=org) as client:
        for i in range(len(processes)):
            process = processes[i]
            time_start = str(int(sec_to_nanosec_factor *int(process["start"])))
            time_end = str(int(sec_to_nanosec_factor * int(process["end"])))
            print(time_start)
            print(time_end)
            query = "from(bucket: \"" + bucket + "\") |> range(start: time(v: " + time_start + "), stop: time(v: " + time_end + "))   |> filter(fn: (r) => r[\"_measurement\"] == \"cip_bfc_mqtt\")"
            tables = client.query_api().query(query, org=org)
            field_keys = ["timestamp"]

            # get the names of all the fields
            for table in tables:
                field_keys.append(table.records[0]['_field'])

            timestamps = np.array([None for j in range(len(table.records))])

            data = np.array([[None for j in range(len(table.records))] for k in range(len(field_keys))])

            # extract relevant data
            for table_index, table in enumerate(tables):
                for record_index, record in enumerate(table):
                    if table_index == 0:
                        #only need one set of timestamps, rest of fields all have same timestamps
                        timestamps[record_index] = record["_time"]


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

            for index, timestamp in enumerate(timestamps):
                timestamps[index] = float(tm.mktime(timestamp.timetuple()) + time_zone_offset)
            try:
                with h5py.File("data/new_sawing_process_data.h5", 'a') as hf:
                    print("saving process with id: " + str(int(process["scan_id"])))
                    hf.create_dataset(str(int(process["scan_id"])), data = np.array([np.vstack([timestamps, data])], dtype = np.float64)[0])
                    hf.close()
            except:
                print("saving process with process id: " + str(process["scan_id"])  + " was not succesful")

            print("remaining: " + str(len(processes) - i) + " processes")
            

            

