import h5py
import matplotlib.pyplot as plt
import numpy as np
import csv
import datetime
import os
import json
import math
import time
import datetime
import pprint

BASEPATH = 'Z:\cip_dmd\cylinder_bottom\cnc_milling_machine\process_data'

acc_data_field_keys = [
    "timestamp",
    "acc_x",
    "acc_y",
    "acc_z",
    "ae"
]


english_translations = {
    "planfraesen" : "face_milling",
    "aussenkontur_schruppen_schlichten" : "outer_contour_roughing_and_finishing", 
    "nut_seitlich" : "lateral_groove",
    "stufenbohrung" : "stepped_bore",
    "endgraten_aussenkontur_bohrungen" : "outer_contour_deburring_holes",
    "bohren_seitlich" : "lateral_drilling",
    "bohren_senken" : "drilling_countersinking",
    "bohren" : "drilling",
    "gewinde_fraesen" : "thread_miling",
    "kreistasche_fraesen" : "circular_pocket_milling",
    "bauteil_entgraten" : "component_deburring",
    "ringnut" : "ring_groove"
}


spike_filename_translations = {
    "bauteil_1_nut_seitlichspikedata.h5" : "part_2_lateral_groove.h5",
    "bauteil_1_aussenkontur_schruppen_schlichtenspikedata.h5" : "part_1_outer_contour_roughing_and_finishing_spike_data.h5",
    "bauteil_2_kreistasche_fraesenspikedata.h5" : "part_2_circular_pocket_milling_spike_data.h5",
    "bauteil_2_planfraesenspikedata.h5" : "part_2_face_milling_spike_data.h5",
    "bauteil_1_planfraesenspikedata.h5" : "part_1_face_milling_spike_data.h5",
    
    "part_2_lateral_groove.h5" : "backside_lateral_groove.h5",
    "part_1_outer_contour_roughing_and_finishing_spike_data.h5" : "frontside_outer_contour_roughing_and_finishing_spike_data.h5",
    "part_2_circular_pocket_milling_spike_data.h5" : "backside_circular_pocket_milling_spike_data.h5",
    "part_2_face_milling_spike_data.h5" : "backside_face_milling_spike_data.h5",
    "part_1_face_milling_spike_data.h5" : "frontside_face_milling_spike_data.h5",


}
def reformatJsonBfcData(bfc_data, field_key_names):
    initialized = False
    # first get length of data for array initialization
    counter = 0
    for timestep in bfc_data:
        counter += 1

    data = np.zeros((len(field_key_names), counter), dtype = np.float64)
    for timestep_idx, timestep in enumerate(bfc_data):
        field_keys = []
        if not initialized:
            for datapoint in timestep["set"]["datapoints"]:
                if (datapoint["type"] in ["int", "float"]) or datapoint["name"] == "NCLine":
                    field_keys.append(datapoint["name"])
                elif datapoint["type"] == "string":
                    pass
                    #print("value in field key " + str(datapoint["name"] + " is of type string and will be skipped"))
                else:
                    print("found unexpected datatype " + datapoint["type"])
            initialized = True

        ts = datetime.datetime.strptime(timestep["set"]["timestamp"], "%Y-%m-%dT%H:%M:%S.%f")
        timestamp = float(time.mktime(ts.timetuple()) * 1e6 + ts.microsecond)/1e6
        data[0][timestep_idx] = timestamp
        
        for datapoint_idx, datapoint in enumerate(timestep["set"]["datapoints"]):
            if datapoint["name"] == "NCLine":
                if len(datapoint["value"]) > 1:
                    data[1][timestep_idx] = float(datapoint["value"][1:])
                else:
                    data[1][timestep_idx] = 0
            elif datapoint["name"] == "ProgramName":
                # we skip that one because its not convertible to float
                pass
            else:
                data[field_keys_dict[datapoint["name"]]][timestep_idx] = float(datapoint["value"])
    return data

field_key_names = []
field_keys_dict = {}

with open('cnc_field_keys.csv', newline='') as csvfile:
    rd = csv.reader(csvfile)    
    idx = 0
    for row in rd:
        field_keys_dict.update({row[0] : idx})
        field_key_names.append(row[0])
        idx += 1

subdirs = [f.path for f in os.scandir(BASEPATH) if f.is_dir()]

for subdir in subdirs:
    # reformatting of bfc data
    print(subdir)
    frontside_bfc_json = json.load(open(os.path.join(subdir, "part_1_bfc_data.json")))
    backside_bfc_json = json.load(open(os.path.join(subdir, "part_2_bfc_data.json")))
    frontside_bfc_arr = reformatJsonBfcData(frontside_bfc_json, field_key_names)
    backside_bfc_arr = reformatJsonBfcData(backside_bfc_json, field_key_names)

    with h5py.File(os.path.join(subdir, "frontside_internal_machine_signals.h5"), 'a') as frontside_internal:
        frontside_internal.create_dataset("data", data = frontside_bfc_arr.T)
        frontside_internal['data'].attrs["column_names"] = field_key_names
        frontside_internal.close()
        print("successfully wrote file " + subdir + " frontside_internal_machine_signals.h5" )
    # deleting old files:
    os.remove(os.path.join(subdir, "part_1_bfc_data.json"))
    
    with h5py.File(os.path.join(subdir, "backside_internal_machine_signals.h5"), 'a') as backside_internal:
        backside_internal.create_dataset("data", data = backside_bfc_arr.T)
        backside_internal['data'].attrs["column_names"] = field_key_names
        backside_internal.close()
        print("successfully wrote file " + subdir + " backside_internal_machine_signals.h5" )
    # deleting old files:
    os.remove(os.path.join(subdir, "part_2_bfc_data.json"))

    with h5py.File(os.path.join(subdir, "part1.h5"), 'a') as frontside_external:
        frontside_external.create_dataset("data", data = frontside_external['0'][:].copy())
        frontside_external['data'].attrs["column_names"] = acc_data_field_keys
        del frontside_external['0']
    # renaming existing h5 data
    os.rename(os.path.join(subdir, "part1.h5"), os.path.join(subdir, "frontside_external_sensor_signals.h5"))

    with h5py.File(os.path.join(subdir, "part2.h5"), 'a') as backside_external:
        backside_external.create_dataset("data", data = backside_external['0'][:].copy())
        backside_external['data'].attrs["column_names"] = acc_data_field_keys
        del backside_external['0']
    # renaming existing h5 data
    os.rename(os.path.join(subdir, "part2.h5"), os.path.join(subdir, "backside_external_sensor_signals.h5"))


    # reformat frontside timestamp/process pairs
    frontside_process_pairs = json.load(open(os.path.join(subdir, "part_1_timestamp_process_pairs.json"))) 
    frontside_process_pairs_list = []
    for timestamp, process_name in frontside_process_pairs.items():
        frontside_process_pairs_list.append((int(round(float(timestamp))), english_translations[process_name]))
    # store timestamp/process pairs in .csv
    with open(os.path.join(subdir, "frontside_timestamp_process_pairs.csv"), 'w', newline='') as f:
        write = csv.writer(f)
        write.writerows(frontside_process_pairs_list)

    # removing old file:
    os.remove(os.path.join(subdir, "part_1_timestamp_process_pairs.json"))
    
    # reformat backside timestamp/process pairs
    backside_process_pairs = json.load(open(os.path.join(subdir, "part_2_timestamp_process_pairs.json"))) 
    backside_process_pairs_list = []
    for timestamp, process_name in backside_process_pairs.items():
        backside_process_pairs_list.append((int(round(float(timestamp))), english_translations[process_name]))
    # store timestamp/process pairs in .csv
    with open(os.path.join(subdir, "backside_timestamp_process_pairs.csv"), 'w', newline='') as f:
        write = csv.writer(f)
        write.writerows(backside_process_pairs_list)

    # removing old file:
    os.remove(os.path.join(subdir, "part_2_timestamp_process_pairs.json"))


    # renaming spike files
    subsubdirs = [f.path for f in os.scandir(subdir)]
    mode = "only_rename_spikedata"
    contains_spike_data = False
    filenames = []
    for subsubdir in subsubdirs:
        filename = subsubdir.split("\\")[-1]
        if "spike" in filename:

            for german_filename in spike_filename_translations.keys():
                if german_filename == filename:
                    os.rename(os.path.join(subdir, filename),\
                        os.path.join(subdir, spike_filename_translations[filename]))
                    print("renaming " + str(subsubdir) + str(filename))