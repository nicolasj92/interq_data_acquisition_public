import h5py
import matplotlib.pyplot as plt
import numpy as np
import csv
import datetime
import os


# structure of data is:
# h5file[scanids[field_key_idx[data]]]

BASEPATH = 'C:\\Users\\Hannes\\OneDrive\\HiWi ML-Team\\QHS\\cip_dmd\\cylinder_bottom\\saw\\process_data'
hf = h5py.File("data/sampled_sawing_process_data.h5", 'a')
keys = list(hf.keys())
print("entries in dataset: " + str(len(keys)))

field_key_names = []
field_keys_dict = {}

with open('sample_sawing_field_keys.csv', newline='') as csvfile:
    rd = csv.reader(csvfile)    
    idx = 0
    for row in rd:
        field_keys_dict.update({row[0] : idx})
        field_key_names.append(row[0])
        idx += 1

for key_idx, key in enumerate(keys):
    ts = datetime.datetime.fromtimestamp(hf[key][0][0])#, datetime.timezone(datetime.timedelta(hours=1)))
    name = key + "_" + str(ts.month) + "_" + str(ts.day) + "_" + str(ts.year) + "_" + str(ts.hour) + "_" + str(ts.minute) + "_" + str(ts.second)
    os.mkdir(os.path.join(BASEPATH, name))
    with h5py.File(os.path.join(BASEPATH, name, "internal_machine_signals.h5"), 'w') as sample_hf:
        sample_hf.create_dataset("dara", data = hf[key])
        sample_hf.close()
        print("successfully wrote file " + name)

