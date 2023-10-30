import pandas as pd
import datetime
import requests
import json
import h5py
import numpy as np
import csv

PATH_CSV = "unformatted_quality_data_piston_rods.csv"


class TurningProductData:
    def __init__(self, path_csv):
        quality_data = pd.read_csv(path_csv, delimiter=";", encoding="latin1")
        quality_data_en = pd.DataFrame(
            columns=[
                "id",
                "coaxiality",
                "diameter",
                "length",
            ],
            data={
                "id": quality_data.iloc[:, 1],
                "coaxiality": quality_data.iloc[:, 2].str.replace(",", "."),
                "diameter": quality_data.iloc[:, 4].str.replace(",", "."),
                "length": quality_data.iloc[:, 6].str.replace(",", "."),
            },
        )
        quality_data_en.set_index("id", inplace=True)
        self.quality_data = quality_data_en
        self.pwd = "interq"
        self.cid = "6LHWRqwyG1jGobMJMyUjsgsA5u52y37dtiu6bPSrXFX1"
        self.owner = "ptw"


    def writeAllQualityData(self):
        column_names = ["id", "coaxiality", "diameter", "length"]
        quality_data = []
        for index, row in self.quality_data.iterrows():
            quality_data.append([index, *row])
        print("entries in quality data: " + str(len(quality_data)))

        with open("turning_quality_data.csv", 'w', newline='') as f:
            quality_data.insert(0, column_names)
            write = csv.writer(f)
            write.writerows(quality_data)
            

transformer = TurningProductData(PATH_CSV)
transformer.writeAllQualityData()

