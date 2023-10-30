import csv
from pprint import pprint
import os
import numpy as np
import h5py
import json

DATASET_PATH = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/data/cip_dmd"
BASE_DIR = "/home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/interq_data_aquisition"


def getAllBounds(quality_data):
    sawing_bounds, cylinder_bounds = calculateBounds(quality_data)
    milling_bounds = {}
    lathing_bounds = {}
    bounds_path = os.path.join(BASE_DIR, "meta", "data", "spezifikationsgrenze.csv")
    with open(bounds_path, newline="") as file:
        reader = csv.reader(file, delimiter=";", quotechar="|")
        for row_idx, row in enumerate(reader):
            # cylinder bottom:
            if row_idx == 2:
                milling_bounds["surface_roughness"] = {"lower_bound": row[1]}
                milling_bounds["parallelism"] = {"lower_bound": row[2]}
                milling_bounds["groove_depth"] = {"lower_bound": row[3]}
                milling_bounds["groove_diameter"] = {"lower_bound": row[4]}
            elif row_idx == 3:
                milling_bounds["surface_roughness"]["upper_bound"] = row[1]
                milling_bounds["parallelism"]["upper_bound"] = row[2]
                milling_bounds["groove_depth"]["upper_bound"] = row[3]
                milling_bounds["groove_diameter"]["upper_bound"] = row[4]
            # piston rod
            elif row_idx == 8:
                lathing_bounds["coaxiality"] = {"lower_bound": row[1]}
                lathing_bounds["diameter"] = {"lower_bound": row[2]}
                lathing_bounds["length"] = {"lower_bound": row[3]}
            elif row_idx == 9:
                lathing_bounds["coaxiality"]["upper_bound"] = row[1]
                lathing_bounds["diameter"]["upper_bound"] = row[2]
                lathing_bounds["length"]["upper_bound"] = row[3]
    print("lathing bonds: ")
    pprint(lathing_bounds)
    print("sawing bonds: ")
    pprint(sawing_bounds)
    print("milling bonds: ")
    pprint(milling_bounds)
    print("cylinder bonds: ")
    pprint(cylinder_bounds)
    return [lathing_bounds, sawing_bounds, milling_bounds, cylinder_bounds]


def calculateBounds(quality_data):
    bounds = [{}, {}]

    for dataset_idx, quality_dataset in enumerate([quality_data[1], quality_data[3]]):
        values = []
        column_names = []
        for id_idx, entry in enumerate(quality_dataset):
            if id_idx == 0:
                # initialize value list and column names
                for column_name in list(quality_dataset[entry].keys()):
                    values.append([])
                    column_names.append(column_name)
                    bounds[dataset_idx][column_name] = {}
            # collect values
            for column_idx, column_name in enumerate(column_names):
                values[column_idx].append(quality_dataset[entry][column_name])
        for column_idx, value_column in enumerate(values):
            if "n" in value_column:
                continue
            value_column = np.array(value_column, dtype=np.float64)
            mean = np.mean(value_column)
            standard_deviation = np.std(value_column)
            bounds[dataset_idx][column_names[column_idx]]["upper_bound"] = (
                mean + 3 * standard_deviation
            )
            bounds[dataset_idx][column_names[column_idx]]["lower_bound"] = (
                mean - 3 * standard_deviation
            )
    return bounds


def checkBounds(bounds, value):
    if value == "n":
        return True
    if value == "y":
        return False
    if float(value) > float(bounds["lower_bound"]) and float(value) < float(
        bounds["upper_bound"]
    ):
        return True
    else:
        return False


def readPartIDs():
    # get piston rod ids from scan timestamp list
    # get cylinder bottom ids from dataset directly
    # get cylinder ids from measurement file
    piston_rod_ids_path = os.path.join(
        BASE_DIR, "process", "turning", "data", "all_scan_timestamps.csv"
    )
    cylinder_bottom_ids_path = os.path.join(
        BASE_DIR, "process", "sawing", "data", "sawing_machine_scan_timestamps.csv"
    )
    cylinder_componenent_ids_path = os.path.join(
        BASE_DIR, "meta", "data", "pneumatic_cylinders_qc_data.csv"
    )
    # piston rods in 0, cylinder bottoms in 1, cylinders in 2
    ids = [[], [], []]

    for idx, path in enumerate([piston_rod_ids_path, cylinder_bottom_ids_path]):
        with open(path, newline="") as ids_file:
            ids_reader = csv.reader(ids_file, delimiter=",", quotechar="|")
            for row_idx, row in enumerate(ids_reader):
                if row_idx == 0:
                    continue
                ids[idx].append(row[0])

    with open(cylinder_componenent_ids_path, newline="") as ids_file:
        ids_reader = csv.reader(ids_file, delimiter=";", quotechar="|")
        for row in ids_reader:
            ids[2].append([row[0], row[1]])

    return ids


def createCylinderIds(cylinder_component_ids):
    cylinder_ids = {}
    for idx, cylinder_components in enumerate(cylinder_component_ids):
        cylinder_id = str(300001 + idx)
        cylinder_ids[cylinder_id] = cylinder_components
    return cylinder_ids


def readQualityData():
    quality_data = [{}, {}, {}, {}]
    piston_rods_quality_data_path = os.path.join(
        BASE_DIR, "meta", "data", "piston_rods_qc_data.csv"
    )
    cylinder_bottom_sawing_quality_data_path = os.path.join(
        BASE_DIR, "meta", "data", "sawing_qc_data.csv"
    )
    cylinder_bottom_machining_quality_data_path = os.path.join(
        BASE_DIR, "meta", "data", "cylinder_bottoms_qc_data.csv"
    )
    cylinder_quality_data_path = os.path.join(
        BASE_DIR, "meta", "data", "pneumatic_cylinders_qc_data.csv"
    )

    for idx, path in enumerate(
        [
            piston_rods_quality_data_path,
            cylinder_bottom_sawing_quality_data_path,
            cylinder_bottom_machining_quality_data_path,
        ]
    ):
        with open(path, newline="") as ids_file:
            column_names = []
            ids_reader = csv.reader(ids_file, delimiter=";", quotechar="|")
            for row_idx, row in enumerate(ids_reader):
                if row_idx == 0:
                    column_names = row[1:]
                else:
                    quality_data[idx][row[0]] = {
                        column_names[i]: row[i + 1] for i in range(len(column_names))
                    }

    with open(cylinder_quality_data_path, newline="") as ids_file:
        column_names = []
        ids_reader = csv.reader(ids_file, delimiter=";", quotechar="|")
        for row_idx, row in enumerate(ids_reader):
            if row_idx == 0:
                column_names = row[2:]
            else:
                quality_data[3][str(300000 + row_idx)] = {
                    column_names[i]: row[i + 2] for i in range(len(column_names))
                }

    return quality_data


def readProcessData():
    lathing_process_data_path = os.path.join(
        DATASET_PATH, "piston_rod", "cnc_lathe", "process_data"
    )
    sawing_process_data_path = os.path.join(
        DATASET_PATH, "cylinder_bottom", "saw", "process_data"
    )
    milling_process_data_path = os.path.join(
        DATASET_PATH, "cylinder_bottom", "cnc_milling_machine", "process_data"
    )
    process_data = [{}, {}, {}]
    for idx, path in enumerate(
        [lathing_process_data_path, sawing_process_data_path, milling_process_data_path]
    ):
        subdirs = [f.path for f in os.scandir(path) if f.is_dir()]
        for subdir in subdirs:
            process_id = subdir.split(os.sep)[-1].split("_")[0]
            process_data[idx][process_id] = {"data_paths": []}
            subsubdirs = [f.path for f in os.scandir(subdir)]
            process_start = 10000000000000
            process_end = -1000000000000
            for subsubdir in subsubdirs:
                data_path_list = subsubdir.split(os.sep)[-5:]
                data_path = data_path_list[0]
                for i in range(4):
                    data_path = data_path + "/" + data_path_list[i + 1]
                try:
                    with h5py.File(subsubdir, "r") as hf:
                        # this logic determines earlist timestamp and latest timestamp of all h5 files in a directory
                        ts_start = hf["data"][0, 0]
                        if ts_start > 1e12:
                            ts_start = ts_start / 1e6
                        ts_end = hf["data"][-1, 0]
                        if ts_end > 1e12:
                            ts_end = ts_end / 1e6
                        if ts_start < process_start:
                            process_start = ts_start
                        if ts_end > process_end:
                            process_end = ts_end
                except:
                    # its a spike file
                    #print(data_path)
                    process_data[idx][process_id]["data_paths"].append(data_path)
                    process_data[idx][process_id]["start_time"] = process_start
                    process_data[idx][process_id]["end_time"] = process_end
    return process_data


def readAnomalist():
    anomal_parts_path = os.path.join(
        BASE_DIR, "meta", "data", "anomalous_parts_detailed.csv"
    )
    with open(anomal_parts_path, newline="") as an_file:
        anomalous_parts_dict = {}
        reader = csv.reader(an_file, delimiter=";", quotechar="|")
        for row_idx, row in enumerate(reader):
            if row_idx == 0:
                continue
            else:
                anomalous_parts_dict[row[0]] = row[1]
    return anomalous_parts_dict


anomalist = readAnomalist()
ids = readPartIDs()
cylinder_id_tuples = ids[2].copy()
ids[2] = createCylinderIds(cylinder_id_tuples)
quality_data = readQualityData()
process_data = readProcessData()
bounds = getAllBounds(quality_data)

part_types = ["piston_rod", "cylinder_bottom", "cylinder"]
process_types = ["cnc_lathe", "saw", "cnc_mill"]
for component_type_idx, component_ids in enumerate(ids):
    part_dict_list = []
    for component_id in component_ids:
        part_dict = {}
        part_dict["part_type"] = part_types[component_type_idx]
        part_dict["part_id"] = component_id
        part_dict["component_ids"] = []
        # only cylinders are composites of other components
        if component_type_idx == 2:
            part_dict["component_ids"] = component_ids[component_id]
        part_dict["process_data"] = []
        part_dict["quality_data"] = []

        # piston rods:
        if component_type_idx == 0:
            if component_id in process_data[0].keys():
                dict_process_data = process_data[0][component_id]
                dict_process_data["name"] = "cnc_lathe"
                dict_process_data["anomaly"] = 0
                part_dict["process_data"].append(dict_process_data)
            if component_id in quality_data[0].keys():
                quality_datapoint = quality_data[0][component_id]
                field_names = []
                for quality_data_field in quality_datapoint:
                    field_names.append(quality_data_field)

                part_dict["quality_data"].append(
                    {
                        "process": "cnc_lathe",
                        "measurements": [
                            {
                                "feature": field_names[i],
                                "value": quality_datapoint[field_names[i]],
                                "qc_pass": checkBounds(
                                    bounds[0][field_names[i]],
                                    quality_datapoint[field_names[i]],
                                ),
                            }
                            for i in range(len(field_names))
                        ],
                    }
                )
        # cylinder bottoms:
        if component_type_idx == 1:
            if component_id in process_data[1].keys():
                dict_process_data = process_data[1][component_id]
                if component_id in quality_data[1].keys():
                    dict_process_data["anomaly"] = quality_data[1][component_id][
                        "anomaly"
                    ]
                else:
                    dict_process_data["anomaly"] = 0
                dict_process_data["name"] = "saw"
                part_dict["process_data"].append(dict_process_data)

            if component_id in quality_data[1].keys():
                quality_datapoint = quality_data[1][component_id]
                field_names = []
                for quality_data_field in quality_datapoint:
                    field_names.append(quality_data_field)
                dict_quality_data = {
                    "process": process_types[1],
                    "measurements": [
                        {
                            "feature": "weight",
                            "value": quality_datapoint["weight"],
                            "qc_pass": checkBounds(
                                bounds[1]["weight"], quality_datapoint["weight"]
                            ),
                        }
                    ],
                }
                part_dict["quality_data"].append(dict_quality_data)

            if component_id in process_data[2].keys():
                dict_process_data = process_data[2][component_id]
                if component_id in anomalist.keys():
                    dict_process_data["anomaly"] = anomalist[component_id]
                else:
                    dict_process_data["anomaly"] = 0
                dict_process_data["name"] = "cnc_milling_machine"
                part_dict["process_data"].append(dict_process_data)

            if component_id in quality_data[2].keys():
                quality_datapoint = quality_data[2][component_id]
                field_names = []
                for quality_data_field in quality_datapoint:
                    field_names.append(quality_data_field)

                dict_quality_data = {
                    "process": process_types[2],
                    "measurements": [
                        {
                            "feature": field_names[i],
                            "value": quality_datapoint[field_names[i]],
                            "qc_pass": checkBounds(
                                bounds[2][field_names[i]],
                                quality_datapoint[field_names[i]],
                            ),
                        }
                        for i in range(len(field_names))
                    ],
                }
                part_dict["quality_data"].append(dict_quality_data)

        # cylinders
        if component_type_idx == 2:
            if component_id in quality_data[3].keys():
                quality_datapoint = quality_data[3][component_id]
                field_names = []
                for quality_data_field in quality_datapoint:
                    field_names.append(quality_data_field)

                part_dict["quality_data"].append(
                    {
                        "process": "assembly",
                        "measurements": [
                            {
                                "feature": field_names[i],
                                "value": quality_datapoint[field_names[i]],
                                "qc_pass": checkBounds(
                                    bounds[3][field_names[i]],
                                    quality_datapoint[field_names[i]],
                                ),
                            }
                            for i in range(len(field_names))
                        ],
                    }
                )
        part_dict_list.append(part_dict)

    if component_type_idx == 0:
        with open(
            os.path.join(DATASET_PATH, "piston_rod", "meta_data.json"), "w"
        ) as json_file:
            json_file.write(json.dumps(part_dict_list, indent=4))
    if component_type_idx == 1:
        with open(
            os.path.join(DATASET_PATH, "cylinder_bottom", "meta_data.json"), "w"
        ) as json_file:
            json_file.write(json.dumps(part_dict_list, indent=4))
    if component_type_idx == 2:
        with open(
            os.path.join(DATASET_PATH, "cylinder", "meta_data.json"), "w"
        ) as json_file:
            json_file.write(json.dumps(part_dict_list, indent=4))
