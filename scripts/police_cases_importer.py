#!/usr/bin/env python
"""
Import the Hartford police incidents.
API: https://docs.influxdata.com/influxdb/v2.4/api-guide/client-libraries/python/
Geolocation helper: https://github.com/aaliddell/s2cell
DATASET: https://data.hartford.gov/api/views/889t-nwfu/rows.csv?accessType=DOWNLOAD
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)

We are expecting to handle 12 columns:

| Description       | Type        | Column # |
|-------------------|-------------|----------|
| Case_Number       | Number      | 1        |
| Date              | Date & Time | 2        |
| Time_24HR         | Plain Text  | 3        |
| Address           | Plain Text  | 4        |
| UCR_1_Category    | Plain Text  | 5        |
| UCR_1_Description | Plain Text  | 6        |
| UCR_1_Code        | Number      | 7        |
| UCR_2_Category    | Plain Text  | 8        |
| UCR_2_Description | Plain Text  | 9        |
| UCR_2_Code        | Number      | 10       |
| Neighborhood      | Plain Text  | 11       |
| geom              | Location    | 12       |

Will use S2 level 10 for our conversion:
https://learn.microsoft.com/en-us/azure/data-explorer/kusto/query/geo-point-to-s2cell-function

"""
from argparse import ArgumentParser
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from csv import reader
from itertools import (takewhile, repeat)
from typing import List

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions, WriteType
from rich.console import Console
from rich.traceback import install
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeRemainingColumn
import s2cell

install(show_locals=True)

START_OF_TIME = "1970-01-01T00:00:00Z"
TIMEOUT_IN_MILLIS = 600000

LARGE_SYNCHRONOUS_BATCH = write_options = WriteOptions(
    batch_size=50_000,
    flush_interval=10_000,
    write_type=WriteType.synchronous
)
S2_LEVEL = 10


def count_lines(filename):
    with open(filename, 'rb') as the_file:
        buffer_gen = takewhile(lambda x: x, (the_file.read(1024 * 1024) for _ in repeat(None)))
        return sum(buf.count(b'\n') for buf in buffer_gen if buf)


def import_data(url: str, token: str, org: str, bucket: str, data_file: Path, truncate: bool = True):
    with Console() as console:
        measurement = "policeincidents"

        total_lines = count_lines(data_file)
        console.print(f"[green]Cases read:[/green] {total_lines}")

        @dataclass
        class PoliceCasePoint:
            case_number: str
            address: str
            ucr_1_category: str
            ucr_1_description: str
            ucr_1_code: str
            ucr_2_category: str
            ucr_2_description: str
            ucr_2_code: str
            neighborhood: str
            s2_cell_id_token: str
            lat: float
            lon: float
            date_time: datetime

        police_cases: List[PoliceCasePoint] = []
        with open(data_file, 'r') as data:
            csv_reader = reader(data)
            with InfluxDBClient(url=url, token=token, org=org, timeout=TIMEOUT_IN_MILLIS) as client:
                if truncate:
                    now = datetime.utcnow()
                    delete_api = client.delete_api()
                    delete_api.delete(start=START_OF_TIME, stop=f"{now.isoformat('T')}Z", bucket=bucket, org=org,
                                      predicate=f'_measurement="{measurement}"')
                write_api = client.write_api(write_options=LARGE_SYNCHRONOUS_BATCH)
                count = 0
                with Progress(TextColumn("[progress.description]{task.description}"),
                              BarColumn(),
                              TaskProgressColumn(),
                              TimeRemainingColumn()) as progress:

                    parsing_task = progress.add_task(f"[red]Parsing[/red] (total={total_lines:,} rows)...",
                                                     total=total_lines)
                    for row in csv_reader:
                        try:
                            case_number = str(row[0])
                            if case_number == "Case_Number":
                                continue
                            date_only = row[1]
                            time_24HR = f"{row[2]}:00.0"
                            if time_24HR == "2400:00.0":
                                time_24HR = "2359:59.0"
                            # date = Date + Time_24HR -> 05/10/2021 1641 -> 2021-10-05 16:41:00
                            date_time = datetime.strptime(f"{date_only} {time_24HR}", "%m/%d/%Y %H%M:%S.%f")
                            address = row[3]
                            ucr_1_category = row[4]
                            ucr_1_description = row[5]
                            ucr_1_code = str(row[6])
                            ucr_2_category = row[7]
                            ucr_2_description = row[8]
                            ucr_2_code = str(row[9])
                            neighborhood = row[10]
                            """
                            https://docs.influxdata.com/flux/v0.x/stdlib/experimental/geo/
                            "(41.780238042803745, -72.68497435174203)"                            
                            """
                            geom = row[11].replace("(", "").replace(")", "").split(",")
                            lat = float(geom[0])
                            lon = float(geom[1])
                            s2_cell_id_token = s2cell.lat_lon_to_token(lat, lon, S2_LEVEL)
                            police_case = PoliceCasePoint(
                                case_number=case_number,
                                date_time=date_time,
                                ucr_1_code=ucr_1_code,
                                ucr_2_code=ucr_2_code,
                                ucr_1_category=ucr_1_category,
                                ucr_2_category=ucr_2_category,
                                neighborhood=neighborhood,
                                ucr_1_description=ucr_1_description,
                                ucr_2_description=ucr_2_description,
                                address=address,
                                lat=lat,
                                lon=lon,
                                s2_cell_id_token=s2_cell_id_token
                            )
                            police_cases.append(police_case)
                            progress.update(
                                parsing_task, advance=1,
                                description=f"Parsed case={police_case.case_number.ljust(9)}, "
                                f"ucr={police_case.ucr_1_code.ljust(5)}"
                            )
                        except ValueError as ve:
                            console.print(f"ERROR: Cannot process {row}, error: {ve}.")
                            raise
                    sorting_task = progress.add_task(f"[red]Sorting[/red] (total={total_lines:,} rows)...", total=1)
                    police_cases.sort(key=lambda p: p.date_time, reverse=True)
                    progress.update(sorting_task, advance=1, description=f"Fully sorted")
                    insert_task = progress.add_task(f"[red]Inserting[/red] (total={total_lines:,} rows)...", total=total_lines)
                    for police_case in police_cases:
                        police_incident_point = Point(measurement) \
                            .field("case_number", police_case.case_number) \
                            .tag("address", police_case.address) \
                            .tag("ucr_1_category", police_case.ucr_1_category) \
                            .tag("ucr_1_description", police_case.ucr_1_description) \
                            .field("ucr_1_code", police_case.ucr_1_code) \
                            .tag("ucr_2_category", police_case.ucr_2_category) \
                            .tag("ucr_2_description", police_case.ucr_2_description) \
                            .field("ucr_2_code", police_case.ucr_2_code) \
                            .tag("neighborhood", police_case.neighborhood) \
                            .tag("s2_cell_id", police_case.s2_cell_id_token) \
                            .field("lat", police_case.lat) \
                            .field("lon", police_case.lon) \
                            .time(police_case.date_time, WritePrecision.S)
                        write_api.write(bucket, org, police_incident_point)
                        progress.update(
                            insert_task,
                            advance=1,
                            description=f"Inserted case={police_case.case_number.ljust(9)}, ucr={police_case.ucr_1_code.ljust(5)}")
                        count += 1
                    write_api.flush()
            console.print(f"Imported {count} records ...")


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--data_file', action='store', required=True, type=Path, help=f"File with the code")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('police_cases', 'org')
    BUCKET = CFG.get('police_cases', 'bucket')
    TOKEN = CFG.get('police_cases', 'api_token')
    URL = CFG.get('police_cases', 'url')

    try:
        import_data(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, data_file=ARGS.data_file)
    except KeyboardInterrupt:
        pass
