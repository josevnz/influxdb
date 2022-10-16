#!/usr/bin/env python
"""
Show COVID19 cases per town in CT, with basic filtering
Author: Jose Vicente Nunez (kodegeek.com@protonmail.com)
"""
from argparse import ArgumentParser
from configparser import ConfigParser
from pathlib import Path

from influxdb_client import InfluxDBClient
from rich.console import Console
from rich.table import Table


def cases_per_town(url: str, token: str, org: str, bucket: str, min_cases: int = 10, start: str = "-3y"):
    with InfluxDBClient(url=url, token=token, org=org) as client:
        query = f"""from(bucket: "{bucket}")
        |> range(start: {start})
        |> filter(fn: (r) => r._measurement == "school" and r._field == "total")
        |> group(columns: ["city"])
        |> drop(columns: ["name", "_start", "_stop"])
        |> sum()
        |> filter(fn: (r) => r._value >= {min_cases})"""
        with Console() as console:
            tables = client.query_api().query(query, org=org)
            screen_table = Table(title=f"COVID19 cases per town (start: {start}")
            screen_table.add_column("#", justify="right", style="cyan", no_wrap=True)
            screen_table.add_column("City", style="magenta")
            screen_table.add_column("Cases", justify="right", style="green")
            for table in tables:
                for record in table.records:
                    table_n = str(record['table'] + 1)
                    city = record['city']
                    value = f"{record['_value']:,}"
                    # print(f"{table_n}, City={city}, value={value}")
                    screen_table.add_row(table_n, city, value)
            console.print(screen_table)


if __name__ == "__main__":
    PARSER = ArgumentParser(__doc__)
    PARSER.add_argument('--start', action='store', default='-3y', help=f"Start time")
    PARSER.add_argument('--cases', action='store', type=int, default=10, help=f"Minimum number of cases")
    PARSER.add_argument('config', type=Path, help=f"Path to the configuration file")
    CFG = ConfigParser()
    ARGS = PARSER.parse_args()
    CFG.read(ARGS.config)

    ORG = CFG.get('covid19', 'org')
    BUCKET = CFG.get('covid19', 'bucket')
    TOKEN = CFG.get('covid19', 'api_token')
    URL = CFG.get('covid19', 'url')

    cases_per_town(url=URL, token=TOKEN, org=ORG, bucket=BUCKET, min_cases=ARGS.cases, start=ARGS.start)
