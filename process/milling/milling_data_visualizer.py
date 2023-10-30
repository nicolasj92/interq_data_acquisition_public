import h5py
import matplotlib.pyplot as plt
import numpy as np
import os

# change this to "process_data" subdirectory path of dataset on your system
BASEPATH = 'Z:\cip_dmd\cylinder_bottom\cnc_milling_machine\process_data'

# read in data
subdirs = [f.path for f in os.scandir(BASEPATH) if f.is_dir()]
data = []
column_names = [0]
counter = 0
for subdir in subdirs:
    counter += 1
    if counter > 10:
        break
    with h5py.File(os.path.join(BASEPATH, subdir, 'frontside_external_sensor_signals.h5'), 'r') as hf:
        data.append(hf['data'][:])

        # get column names
        if len(column_names) == 1:
            column_names = hf['data'].attrs["column_names"]

# show column names
print("column names are:")
for idx, column_name in enumerate(column_names):
    print(str(idx) + " : "  + str(column_name))

# visualise data
column_name_index = 4
fig, axs = plt.subplots(10, 10, figsize = (15, 15))
fig.suptitle(column_names[column_name_index])

for C in range(10):
    for X in range(10):
        for I in range(10):
            try:
                axs[X][I].plot(data[C * 100 + X * 10 + I][:,0], data[C * 100 +  X * 10 + I][:,column_name_index])
            except:
                pass
    plt.show()