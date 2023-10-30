import pandas as pd
import datetime
import requests
import json
import h5py
import numpy as np
import csv

PATH_CSV = "untransformed_quality_data_cylinder_bottom.csv"

class MillingProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=";", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "measurement_timestamp",
                "surface_roughness",
                "parallelism",
                "groove_depth",
                "groove_diameter",
            ],
            data={
                "id": quality_data.iloc[3:, 1],
                "measurement_timestamp": quality_data.iloc[3:, 2],
                "surface_roughness": quality_data.iloc[3:, 3].str.replace(",", "."),
                "parallelism": quality_data.iloc[3:, 4].str.replace(",", "."),
                "groove_depth": quality_data.iloc[3:, 5].str.replace(",", "."),
                "groove_diameter": quality_data.iloc[3:, 6].str.replace(",", "."),
            },
        )
        quality_data_en.set_index("id", inplace=True)
        self.quality_data = quality_data_en
        self.pwd = "interq"
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.owner = "ptw"


    def writeAllQualityData(self):
        column_names = ["id", "measurement_timestamp", "surface_roughness", "parallelism", "groove_depth", "groove_diameter"]
        quality_data = []
        for index, row in self.quality_data.iterrows():
            quality_data.append([index, *row])
        print("entries in quality data: " + str(len(quality_data)))
        
        with open("milling_quality_data.csv", 'w', newline='') as f:
            quality_data.insert(0, column_names)
            write = csv.writer(f)
            write.writerows(quality_data)
            




transformer = MillingProductData(PATH_CSV)
transformer.writeAllQualityData()

