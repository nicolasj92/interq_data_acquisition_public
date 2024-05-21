from datetime import datetime, timedelta
from typing import List

import pandas as pd
import pytz
from matplotlib import pyplot as plt
from scipy.signal import find_peaks


def convert_to_rfc3339(timestamp: str, postpone: int = 0, advance: int = 0):
    format = "%Y-%m-%d %H:%M:%S"
    dt = datetime.strptime(timestamp, format)
    berlin_timezone = pytz.timezone('Europe/Berlin')
    dt = berlin_timezone.localize(dt)
    dt = dt + timedelta(days=postpone)
    dt = dt - timedelta(days=advance)
    return dt.isoformat()


def assemble_query(start_time, stop_time, bucket_name="interq_kompaki_prod", measurement_name="cip_bfc_mqtt",
                   field_name=None, filters: List[str] = []):
    source_txt = f'from (bucket: "{bucket_name}")'
    range_txt = f' |> range(start:{str(start_time)}, stop: {str(stop_time)})'
    measurement_filter_txt = f' |> filter(fn: (r) => r["_measurement"] == "{measurement_name}")'
    if field_name:
        field_filter_txt = f' |> filter(fn: (r) => r["_field"] == "{field_name}")'
    else:
        field_filter_txt = ""

    query_txt = "".join([source_txt, range_txt, measurement_filter_txt, field_filter_txt, *filters])

    return query_txt


def pretty_print_flux_query(query):
    print("Flux Query:")
    parts = query.split("|>")
    cleaned_parts = [part.strip() for part in parts]
    formatted_query = "\n".join("\t|> " + part if i != 0 else part for i, part in enumerate(cleaned_parts))
    print(formatted_query)


def cutoff_data(df, start_time, end_time):
    return df[
        (df['time'] > pd.Timestamp(start_time, tz='UTC'))
        & (df['time'] < pd.Timestamp(end_time, tz='UTC'))
        ]


def plot_labels(df, timestamp_col, label_col):
    fig, ax = plt.subplots(figsize=(15, 6))

    df[label_col] = df[label_col].apply(lambda x: min(x, 2000))
    ax.plot(df[timestamp_col], df[label_col], label='Label over Time', marker='o', linestyle='-')

    ax.set_title('Time Series Plot')
    ax.set_xlabel('Timestamp')
    ax.set_ylabel(label_col)

    # Optimizer the x axis for date
    fig.autofmt_xdate()

    ax.legend(loc='lower right')

    plt.show()


def find_peaks_troughs(data, distance=50, prominence=1, p_width=300, p_height=100):
    peaks, peaks_properties = find_peaks(data, distance=distance, prominence=prominence, width=p_width)
    troughs, troughs_properties = find_peaks(-data, distance=distance, prominence=prominence, width=p_width)
    return peaks, peaks_properties, troughs, troughs_properties


def plot_peaks(df, peaks, peaks_properties):
    fig, ax = plt.subplots(figsize=(15, 6))

    ax.plot(df['time'], df['NCLine'], label='NCLine Data')
    ax.scatter(df['time'][peaks], df['NCLine'][peaks], color='red', label='Peaks', marker='^', s=100)
    for i, peak in enumerate(peaks):
        left_bound = peaks_properties["left_bases"][i]
        right_bound = peaks_properties["right_bases"][i]

        ax.hlines(
            y=peaks_properties["width_heights"][i],
            xmin=df['time'][left_bound],
            xmax=df['time'][right_bound],
            color='red'
        )

    ax.set_title('NCLine Data with Identified Peaks')

    fig.autofmt_xdate()

    ax.legend(loc='lower right')
    plt.show()


def identify_batch(data, peaks, peaks_properties):
    batch = []
    batch_start = 0
    batch_end = 0
    for i, peak in enumerate(peaks):
        if i == 0:
            batch_start = 0
            batch_end = peaks_properties["left_bases"][i]
        else:
            batch_start = peaks_properties["right_bases"][i - 1]
            batch_end = peaks_properties["left_bases"][i]
        batch.append((batch_start, batch_end))
    return batch


def plot_batches(data, batch_identifiers):
    fig, ax = plt.subplots(figsize=(15, 6))

    ax.plot(data['time'], data['NCLine'], label='NCLine Data')
    for batch in batch_identifiers:
        start_time = data['time'][batch[0]]
        end_time = data['time'][batch[1]]
        ax.axvspan(start_time, end_time, color='red', alpha=0.5)

    ax.set_title('NCLine Data with Identified Batches')

    fig.autofmt_xdate()
    ax.legend(loc='lower right')
    plt.show()
