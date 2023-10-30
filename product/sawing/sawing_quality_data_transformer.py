import pandas as pd
import datetime
import requests
import json
import h5py
import numpy as np
import csv

PATH_CSV = "unformatted_quality_data_sawing.csv"


class TurningProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=",", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "measurement_timestamp",
                "weight",
            ],
            data={
                "id": quality_data.iloc[:, 0],
                "measurement_timestamp": quality_data.iloc[:, 1],
                "weight": quality_data.iloc[:, 2],
            },
        )
        quality_data_en.set_index("id", inplace=True, drop=True)
        self.quality_data = quality_data_en
        self.pwd = "interq"
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.owner = "ptw"



    def writeAllQualityData(self):
        column_names = ["id", "measurement_timestamp", "weight"]
        quality_data = []
        for index, row in self.quality_data.iterrows():
            quality_data.append([index, *row])
            
        with open("sawing_quality_data.csv", 'w', newline='') as f:
            quality_data.insert(0, column_names)
            write = csv.writer(f)
            write.writerows(quality_data)

transformer = TurningProductData(PATH_CSV)
transformer.writeAllQualityData()

