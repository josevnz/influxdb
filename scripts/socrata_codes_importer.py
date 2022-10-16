#!/usr/bin/env python
"""
Import the Hartford police Socrata UCR codes.
API: https://docs.influxdata.com/influxdb/v2.4/api-guide/client-libraries/python/
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)
"""
from argparse import ArgumentParser
from configparser import ConfigParser
from datetime import datetime
from pathlib import Path
from csv import reader

from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from rich.console import Console
from rich.table import Table

START_OF_TIME = "1970-01-01T00:00:00Z"


def import_data(url: str, token: str, org: str, bucket: str, data_file: Path, truncate: bool = True):
    with Console() as console:
        screen_table = Table(title=f"Socrata HPD UCR codes")
        screen_table.add_column("UCR code", justify="right", style="cyan", no_wrap=True)
        screen_table.add_column("Primary description", style="magenta")
        screen_table.add_column("Secondary description", justify="right", style="green")
        measurement = "socrata_ucr_codes"
        with open(data_file, 'r') as data:
            csv_reader = reader(data)
            with InfluxDBClient(url=url, token=token, org=org) as client:
                if truncate:
                    now = datetime.utcnow()
                    delete_api = client.delete_api()
                    delete_api.delete(start=START_OF_TIME, stop=f"{now.isoformat('T')}Z", bucket=bucket, org=org, predicate=f'_measurement="{measurement}"')
                write_api = client.write_api(write_options=SYNCHRONOUS)
                now = datetime.utcnow()
                for row in csv_reader:
                    ucr_code = str(row[0])
                    if ucr_code != 'ucr_code':
                        prim_desc = row[1]
                        sec_desc = row[2]
                        socrata_code = Point(measurement) \
                            .field("ucr_code", ucr_code) \
                            .tag("primary_description", prim_desc) \
                            .tag("secondary_description", sec_desc) \
                            .time(now)
                        screen_table.add_row(ucr_code, prim_desc, sec_desc)
                        write_api.write(bucket, org, socrata_code)
                write_api.flush()
                write_api.close()
                query_api = client.query_api()
                query = f"""from(bucket:"{bucket}")
                |> range(start: -10m)
                |> filter(fn:(r) => r._measurement == "{measurement}")
                |> filter(fn:(r) => r._field == "ucr_code")"""
                result = query_api.query(org=org, query=query)
                count = 0
                for table in result:
                    for record in table.records:
                        count += 1
                console.print(screen_table)
                console.print(f"Imported {count} records ...")


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--data_file', action='store', required=True, type=Path, help=f"File with the code")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('police_codes', 'org')
    BUCKET = CFG.get('police_codes', 'bucket')
    TOKEN = CFG.get('police_codes', 'api_token')
    URL = CFG.get('police_codes', 'url')

    import_data(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, data_file=ARGS.data_file)
