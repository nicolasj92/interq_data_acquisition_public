# INSTRUCTION for data splitting task
## Setup
### Install dependencies
```bash
conda create -n interq python=3.10

conda activate interq
pip install -r requirements.txt 

# or try with one of the following alternatives when the first one is failed.
# please skip the following command if the first one is successful.
pip install -r requirements_mini.txt
```

### Prepare data
Due to the restricted permission to the original data folder, it is needed to copy the data files to your personal directory.

```Bash
# Need to ssh to the workstation 02 of CiP server. 
mkdir -p /path/to/your_project/data/cip_dmd
cp -r /home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/data/cip_dmd /path/to/your_project/data
```
cp -r /home/mittwollen_h@PTW.Maschinenbau.TU-Darmstadt.de/data/cip_dmd /home/yu_z@PTW.Maschinenbau.TU-Darmstadt.de/interq_data_acquisition_public/data



### Environment variables
Create a `.env` file in the root directory of the project and add the following environment variables:
```bash
INFLUXDB_API_TOKEN="REPLACED WITH YOUR TOKEN"
INFLUXDB_ORG="PTW TU Darmstadt"

INFLUXDB_BUCKET=interq_kompaki_prod

INFLUXDB_URL=http://10.10.151.21:8086

PROJECT_ROOT=path/to/your_project
DATASET_PATH=path/to/your_project/data/cip_dmd
```


## Quick Start
```shell
python process/turning/data_extraction.py
```
