#! /usr/bin/env python

import argparse
import operator
import pathlib
import re
import uuid
from datetime import datetime, timezone
from glob import glob
from typing import List

import Levenshtein
import pendulum
from pyspark.sql import DataFrameWriter, SparkSession
from pyspark.sql.types import (IntegerType, StringType, StructField,
                               StructType, TimestampType)


class Configuration:
    load_id = str(uuid.uuid4())
    _load_date = pendulum.now()
    load_date = datetime(
        _load_date.in_timezone('UTC').year,
        _load_date.in_timezone('UTC').month,
        _load_date.in_timezone('UTC').day,
        _load_date.in_timezone('UTC').hour,
        _load_date.in_timezone('UTC').minute,
        _load_date.in_timezone('UTC').second,
        _load_date.in_timezone('UTC').microsecond,
        tzinfo=timezone.utc,
    )


class SCDailyJailPopulationReport:
    line_expectations = {
        1: 'Office of the Sheriff',
        2: 'Department of Correction',
        3: 'Daily Jail Population Statistics',
        14: 'Felony Sentenced',
        15: 'Misdemeanor Sentenced',
        16: 'Felony Unsentenced',
        17: 'Misdemeanor Unsentenced'
    }
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    field_combos = {
        14: ('size_population_sentenced_felony_men', 'size_population_sentenced_felony_women'),
        15: ('size_population_sentenced_misdemeanor_men', 'size_population_sentenced_misdemeanor_women'),
        16: ('size_population_unsentenced_felony_men', 'size_population_unsentenced_felony_women'),
        17: ('size_population_unsentenced_misdemeanor_men', 'size_population_unsentenced_misdemeanor_women')
    }

    def __init__(self, fn: str) -> None:
        self.fn = fn
        self.databag = {}

    def check_line(self, line_number: int, s: str) -> bool:
        if Levenshtein.ratio(s, SCDailyJailPopulationReport.line_expectations[line_number]) < 0.75:
            return False
        return True

    def get_most_likely_month(self, month_name: str) -> (int, str):
        index, r = max(enumerate(map(lambda m: Levenshtein.ratio(
            month_name, m), SCDailyJailPopulationReport.month_names)), key=operator.itemgetter(1))
        if r < 0.75:
            raise ValueError(f'Unlikely month name: {month_name}')
        return (index+1, SCDailyJailPopulationReport.month_names[index])

    def determine_date(self, dtstr: str) -> datetime:
        dtmre = re.compile(
            r'^[^,.]+.\s*(\S+)\s*([0-9]{1,2}).\s*(....)\s*\S+?\s*\S+?\s*([0-9:;apm]+)$')
        m_datetime = dtmre.match(dtstr)
        if m_datetime is None:
            raise ValueError(f'Cannot detect date in {dtstr}')

        month_no, _ = self.get_most_likely_month(m_datetime.group(1))
        try:
            day_number = int(m_datetime.group(2))
        except ValueError as e:
            raise ValueError(
                f'Cannot convert {m_datetime.group(2)} to a day number') from e

        try:
            year_number = int(m_datetime.group(3))
        except ValueError as e:
            raise ValueError(
                f'Cannot convert {m_datetime.group(3)} into a numeric year') from e

        m_time = re.match(r'^(\d+):(\d+)\s*(am|pm)$', m_datetime.group(4))
        if m_time is None:
            raise ValueError(f'Unlikely time of day: {m_datetime.group(4)}')

        hour = int(m_time.group(1))
        minute = int(m_time.group(2))
        if m_time.group(3) == 'pm' and hour < 12:
            hour += 12

        _tt = pendulum.create(year_number, month_no, day_number,
                              hour, minute, tz='America/Los_Angeles')

        return datetime(
            _tt.in_timezone('UTC').year,
            _tt.in_timezone('UTC').month,
            _tt.in_timezone('UTC').day,
            _tt.in_timezone('UTC').hour,
            _tt.in_timezone('UTC').minute,
            tzinfo=timezone.utc,
        )

    def determine_sheriff(self, s: str) -> str:
        m = re.match(r'^(.*)[,.]\s+(\S+)$', s)
        if m is None:
            raise ValueError(f'Unlikely sheriff line: {s}')
        if Levenshtein.ratio(m.group(2), 'Sheriff') < 0.75:
            raise ValueError(f'Unlikely match for Sheriff: {m.group(2)}')
        return m.group(1)

    def parse_int(self, s: str) -> int:
        return int(re.sub(r'[,.]', '', s))

    def process(self) -> None:
        re_merge_blanks = re.compile(r'\s{2,}')
        line_counter = 0
        databag = {}
        with open(self.fn, mode='r+t', encoding='utf-8') as fd:
            for line in fd:
                # if the line is empty, move on to the next
                if line.strip() == '':
                    continue
                line_counter += 1
                # Remove leading and trailing whitespace, merge delimiters
                line = re_merge_blanks.sub(' ', line.strip())
                if line_counter in range(1, 4):
                    if not self.check_line(line_counter, line):
                        raise ValueError(
                            f'Expected line {line_counter}: "{SCDailyJailPopulationReport.line_expectations[line_counter]}". Actual: "{line}"')
                elif line_counter == 4:
                    databag['report_date'] = self.determine_date(line)
                elif line_counter == 5:
                    databag['sheriff'] = self.determine_sheriff(line)
                elif line_counter == 6:
                    # Add test for none 11, 14, 16
                    m = re.match(r'^[^:;]*[:;]\s+([0-9,.]+)\s+.*$', line)
                    databag['size_population_total'] = self.parse_int(
                        m.group(1))
                elif line_counter == 8:
                    m = re.match(r'^[^:;]*[:;]\s+([0-9,.]+)\s+.*$', line)
                    databag['size_population_total_men'] = self.parse_int(
                        m.group(1))
                elif line_counter == 9:
                    m = re.match(r'^[^:;]*[:;]\s+([0-9,.]+)\s+.*$', line)
                    databag['size_population_total_women'] = self.parse_int(
                        m.group(1))
                elif line_counter in range(14, 18):
                    m = re.match(
                        r'^([^:;]*)[:;]\s+([0-9,.]+)\s+[0-9]+\s*%\s*[0-9]+\s+([0-9]+)\s+[0-9]+\s*%.*$', line)
                    if m is None:
                        raise ValueError(
                            f'Line is unlikely to carry {SCDailyJailPopulationReport.line_expectations[line_counter]} info: {line}')
                    if not self.check_line(line_counter, m.group(1)):
                        raise ValueError(
                            f'Capture group is unlikely to carry {SCDailyJailPopulationReport.line_expectations[line_counter]} info: {m.group(1)}')
                    databag[SCDailyJailPopulationReport.field_combos[line_counter]
                            [0]] = self.parse_int(m.group(2))
                    databag[SCDailyJailPopulationReport.field_combos[line_counter]
                            [1]] = self.parse_int(m.group(3))
        self.databag = databag


def validateFileList(l: List[str]) -> bool:
    for fn in l:
        if not pathlib.Path(fn).is_file():
            return False
    return True


def process_file(file: str):
    sc_dpcs = SCDailyJailPopulationReport(file)
    sc_dpcs.process()
    r = []
    r.append(Configuration.load_id)
    r.append(Configuration.load_date)
    for field in ['report_date',
                  'sheriff',
                  'size_population_total',
                  'size_population_total_men',
                  'size_population_total_women',
                  'size_population_sentenced_felony_men',
                  'size_population_sentenced_felony_women',
                  'size_population_sentenced_misdemeanor_men',
                  'size_population_sentenced_misdemeanor_women',
                  'size_population_unsentenced_felony_men',
                  'size_population_unsentenced_felony_women',
                  'size_population_unsentenced_misdemeanor_men',
                  'size_population_unsentenced_misdemeanor_women']:
        r.append(sc_dpcs.databag[field])
    yield r


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Spark Job ETL sc-jail-project')
    parser.add_argument(
        '-g', '--glob', help='Process files selected with this pattern', required=True)
    parser.add_argument(
        '-s', '--schema', help='Schema in the database', required=True)
    parser.add_argument(
        '-u', '--user', help='User for the database', required=True)
    parser.add_argument(
        '-p', '--password', help='Password for the database', required=True)
    parser.add_argument(
        '-i', '--load_id', help=f'UUID identifier for the load (default: random)', default=Configuration.load_id)

    cleanup_infile = parser.add_mutually_exclusive_group(required=True)
    cleanup_infile.add_argument(
        '--archive-infile', action='store_true', help="Don't clean up the input file")
    cleanup_infile.add_argument(
        '--delete-infile', action='store_true', help="Clean up the input file")

    args = parser.parse_args()
    Configuration.load_id = args.load_id
    spark = SparkSession \
        .builder \
        .appName('ETL sc-jail-project') \
        .getOrCreate()
    files = glob(args.glob)
    # TODO
    # implement a function to validateFileList(files). Should signal if
    # the list of files contain anything but regular files or symlinks to
    # regular files. Research if Hadoop and s3 have api for this.
    if not validateFileList(files):
        raise ValueError(
            f'At least one of the files in {args.glob} is not a regular file')

    rdd = spark.sparkContext \
        .parallelize(files) \
        .flatMap(process_file)

    schema = StructType(
        [
            StructField('LOAD_ID_STR', StringType(), nullable=True),
            StructField('LOAD_TIME', TimestampType(), nullable=True),
            StructField('REPORT_DATE', TimestampType(), nullable=True),
            StructField('SHERIFF', StringType(), nullable=True),
            StructField('COUNT_POPULATION_TOTAL',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_TOTAL_MEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_TOTAL_WOMEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_SENTENCED_FELONY_MEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_SENTENCED_FELONY_WOMEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_SENTENCED_MISDEMEANOR_MEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_SENTENCED_MISDEMEANOR_WOMEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_UNSENTENCED_FELONY_MEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_UNSENTENCED_FELONY_WOMEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_UNSENTENCED_MISDEMEANOR_MEN',
                        IntegerType(), nullable=True),
            StructField('COUNT_POPULATION_UNSENTENCED_MISDEMEANOR_WOMEN',
                        IntegerType(), nullable=True),
        ]
    )
    df = spark.createDataFrame(rdd, schema)

    df.show()
    url_connect = f'jdbc:postgresql://database:5432/{args.schema}'
    table = "staging"
    mode = "append"
    properties = {"user": args.user, "password": args.password}

    df.write.jdbc(
        url=url_connect,
        table=table,
        mode=mode,
        properties=properties
    )

    print(f'load_id:{Configuration.load_id}')
