import h5py
import matplotlib.pyplot as plt
import numpy as np
import csv
import numba


# structure of data is:
# h5file[scanids[field_key_idx[data, timestamps]]]

hf = h5py.File("data/sawing_process_data.h5", 'a')
keys = list(hf.keys())
#print("entries in dataset: " + str(len(keys)))
#print(keys)

field_key_names = []
field_keys_dict = {}

with open('sawing_field_keys.csv', newline='') as csvfile:
    rd = csv.reader(csvfile)    
    idx = 0
    for row in rd:
        field_keys_dict.update({row[0] : idx})
        field_key_names.append(row[0])
        idx += 1


error_keys = ["105901", "103701"]
for key in error_keys:
    for field_key_idx, field_key_name in enumerate(field_key_names):
        print(len(hf[key][field_key_idx][1]))

@numba.jit(nopython=True)
def sampleData(values, slave_timestamps, master_timestamps, field_key_name, process_key):
    sampled_data = np.zeros_like(sampling_timestamps)
    offset = 0
    for slave_timestamp_idx, master_timestamp in enumerate(master_timestamps):
        # counter is just for safety measure
        counter = 0
        # the varying data fields have slightly varying lengths, that could mean that timestamps with same index could not be from same time
        # to sample points at equal timestamps we choose the closest neighbour if a data point is missing
        tolerance = 0.6
        timestamp_difference = master_timestamp - slave_timestamps[slave_timestamp_idx + offset]
        while(abs(timestamp_difference) > tolerance):
            direction = np.sign(timestamp_difference)
            if direction > 0 and slave_timestamp_idx + offset < len(slave_timestamps) - 1:
                offset -= 1
            elif direction < 0 and slave_timestamp_idx + offset > 0:
                offset += 1
            timestamp_difference = master_timestamp - slave_timestamps[slave_timestamp_idx + offset]

            counter += 1    
            if counter >= 5:
                print("counter higher than 5 encountered that could mean sampling tolerance is impossible to reach")
                break

            sampled_data[slave_timestamp_idx] = values[slave_timestamp_idx + offset]
    return sampled_data

for key_idx, key in enumerate(keys):
    # choose max field size for sampling
    sampling_field = 0
    max_len = 0
    for i in range(len(field_key_names)):
        if len(hf[key][1]) > max_len:
            sampling_field = i
            max_len = len(hf[key][1])

    sampling_timestamps = hf[key][sampling_field][1]
    sampled_data = [sampling_timestamps]

    for field_key_idx, field_key_name in enumerate(field_key_names):
        data, errors = sampleData(hf[key][field_key_idx][0], hf[key][field_key_idx][1], sampling_timestamps, field_key_name, key)
        sampled_data.append(data)
        for error in errors:
            print(error)
    try:
        with h5py.File("data/sampled_sawing_process_data.h5", 'a') as sample_hf:
            #print("saving process with id: " + key)
            sample_hf.create_dataset(key, data = np.array(sampled_data, dtype = np.float64))
            sample_hf.close()
    except:
        print("saving process with process id: " + key  + " was not successful")

