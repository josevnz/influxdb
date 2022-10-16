#!/usr/bin/env python3
"""
This script massages CT COVID19 cases by school adding annotations for Influxdb, so they can be imported using the CLI.
"""
import csv
import datetime
import re
import sys
from argparse import ArgumentParser
from enum import Enum
from pathlib import Path

DEFAULT_REPORT = Path.home().joinpath("import_covid_data.csv")
MAX_ERRORS = 10

if __name__ == "__main__":
    PARSER = ArgumentParser("Convert CT school COVID19 data info an annotated CSV format for Influxdb")
    PARSER.add_argument('--destination', type=Path, default=DEFAULT_REPORT, help=f"Destination for massaged data. Default={DEFAULT_REPORT}")
    PARSER.add_argument('--explode', action='store_true',
                        help=f"Destination for massaged data. Default={DEFAULT_REPORT}")
    PARSER.add_argument('--max_errors', default=MAX_ERRORS, help=f"Maximum number of import errors, default {MAX_ERRORS}")
    PARSER.add_argument('source', nargs='+')
    ARGS = PARSER.parse_args()
    with open(ARGS.destination, 'w') as report:
        """
        Data normalization is a must; You can see than the format changed between 2020-2021 and 2021-2022:
        
        2020-2021: District,School ID,School name,City,School total,Report period,Date updated
        2021-2022: District,School Name,City,Report Period,Total Cases,Academic Year,Date Updated
        
        Also, order of total cases per school and report period flipped between years.
        
        To simplify things a little bit, we will drop a few columns from the resulting report
        2020-2021: School ID
        2021-2022: Academic Year
        
        Ending report will be:
        District,School Name,City,Total Cases,Report Period,Date Updated
        
        """
        report.write('''#datatype measurement,ignored,tag,tag,long,time,ignored
school,district,name,city,total,time,updated\n''')
        writer = csv.writer(report, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        num_errors = 0
        for data_file in ARGS.source:
            with open(data_file, 'r') as data:
                school_reader = csv.reader(data, delimiter=',')
                """
                Data format changed between year 2021-2022. Use the headers
                2020-2021: District,School ID,School name,City,School total,Report period,Date updated
                2021-2022: District,School Name,City,Report Period,Total Cases,Academic Year,Date Updated
                """
                original_format = True
                for row in school_reader:
                    try:
                        if row[0] == 'District':
                            original_format = row[1] == "School ID"
                            continue
                        if original_format:
                            class Position(Enum):
                                DISTRICT = 0
                                SCHOOL = 2
                                CITY = 3
                                TOTAL = 4
                                PERIOD = 5
                                UPDATED = 6
                        else:
                            class Position(Enum):
                                DISTRICT = 0
                                SCHOOL = 1
                                CITY = 2
                                TOTAL = 4
                                PERIOD = 3
                                UPDATED = 6
                        # Check for schools with less < 'cases' and take the upper limit (cases - 1)
                        matcher = re.search('<(\\d+)', row[Position.TOTAL.value])
                        if matcher:
                            total = int(matcher.group(1)) - 1
                        else:
                            total = int(row[Position.TOTAL.value])
                        date_ranges = row[Position.PERIOD.value].strip().split('-')
                        # Date in RFC3339
                        start = datetime.datetime.strptime(date_ranges[0].strip(), '%m/%d/%Y')
                        district = row[Position.DISTRICT.value].strip()
                        school_name = row[Position.SCHOOL.value].strip()
                        updated = row[Position.UPDATED.value].strip()
                        city = row[Position.CITY.value].strip()
                        if not ARGS.explode:
                            writer.writerow(
                                ["school", district, school_name, city, total, start.isoformat('T') + 'Z', updated])
                        else:
                            end = datetime.datetime.strptime(date_ranges[1].strip(), '%m/%d/%Y')
                            # We use a date generator
                            date_generated = (start + datetime.timedelta(days=x) for x in range(0, (end - start).days + 1))
                            for date in date_generated:
                                writer.writerow(["school", district, school_name, city, total, date.isoformat('T') + 'Z', updated])
                    except ValueError:
                        print(f"Problem parsing line: {row}, {data_file}", file=sys.stderr)
                        num_errors += 1
                        if num_errors > ARGS.max_errors:
                            print(f"Too many errors ({num_errors}), cannot continue!")
                            raise
