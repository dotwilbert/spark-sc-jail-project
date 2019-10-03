#! /usr/bin/env python

import argparse
import operator
import pathlib
import re
from datetime import datetime
from glob import glob
from typing import List

import pytz

import Levenshtein
from pyspark.sql import SparkSession
from pyspark.sql.types import *


class SCDailyJailPopulationReport:
    line_expectations = {
        1: 'Office of the Sheriff',
        2: 'Department of Correction',
        3: 'Daily Jail Population Statistics',
        13: 'Felony Sentenced',
        14: 'Misdemeanor Sentenced',
        15: 'Felony Unsentenced',
        16: 'Misdemeanor Unsentenced'
    }
    month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                   'July', 'August', 'September', 'October', 'November', 'December']
    field_combos = {
        13: ('size_population_sentenced_felony_men', 'size_population_sentenced_felony_women'),
        14: ('size_population_sentenced_misdemeanor_men', 'size_population_sentenced_misdemeanor_women'),
        15: ('size_population_unsentenced_felony_men', 'size_population_unsentenced_felony_women'),
        16: ('size_population_unsentenced_misdemeanor_men', 'size_population_unsentenced_misdemeanor_women')
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
            r'^[^,.]+.\s*(\S+)\s*([^,.]*).\s*(....)\s*\S+\s*\S+\s*(\S+)$')
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

        return pytz.timezone('America/Los_Angeles').localize(datetime(year_number, month_no, day_number, hour, minute))

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
        line_counter = 0
        databag = {}
        with open(self.fn, mode='r+t', encoding='utf-8') as fd:
            for line in fd:
                # if the line is empty, move on to the next
                if line.strip() == '':
                    continue
                line_counter += 1
                if line_counter in range(1, 4):
                    if not self.check_line(line_counter, line):
                        raise ValueError(
                            f'Expected line {line_counter}: "{SCDailyJailPopulationReport.line_expectations[line_counter]}". Actual: "{line}"')
                elif line_counter == 4:
                    databag['report_date'] = self.determine_date(line).astimezone(
                        pytz.timezone('UTC')).strftime("%Y-%m-%dT%H:%M:%S%z")
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
                elif line_counter in range(13, 17):
                    m = re.match(
                        r'^([^:;]*)[:;]\s+([0-9,.]+)\s+[^%]+%\s.*?([0-9,.]+)\s+[0-9,.]+%.*$', line)
                    if m is None:
                        raise ValueError(
                            f'Line is unlikely to carry {SCDailyJailPopulationReport.line_expectations[line_counter]} info: {line}')
                    if not self.check_line(line_counter, m.group(1)):
                        raise ValueError(
                            f'Line is unlikely to carry {SCDailyJailPopulationReport.line_expectations[line_counter]} info: {m.group(1)}')
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

    cleanup_infile = parser.add_mutually_exclusive_group(required=True)
    cleanup_infile.add_argument(
        '--archive-infile', action='store_true', help="Don't clean up the input file")
    cleanup_infile.add_argument(
        '--delete-infile', action='store_true', help="Clean up the input file")

    args = parser.parse_args()
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

    df = spark.sparkContext \
        .parallelize(files) \
        .flatMap(process_file) \
        .toDF(['REPORTDATE',
               'SHERIFF',
               'COUNT_POPULATION_TOTAL',
               'COUNT_POPULATION_TOTAL_MEN',
               'COUNT_POPULATION_TOTAL_WOMEN',
               'COUNT_POPULATION_SENTENCED_FELONY_MEN',
               'COUNT_POPULATION_SENTENCED_FELONY_WOMEN',
               'COUNT_POPULATION_SENTENCED_MISDEMEANOR_MEN',
               'COUNT_POPULATION_SENTENCED_MISDEMEANOR_WOMEN',
               'COUNT_POPULATION_UNSENTENCED_FELONY_MEN',
               'COUNT_POPULATION_UNSENTENCED_FELONY_WOMEN',
               'COUNT_POPULATION_UNSENTENCED_MISDEMEANOR_MEN',
               'COUNT_POPULATION_UNSENTENCED_MISDEMEANOR_WOMEN'])

    df.show()
# >>> from pyspark.sql import SparkSession
# >>> from glob import glob
# >>> spark = SparkSession.builder.appName('ETL sc-jail-project').getOrCreate()
# >>> files = glob('/bigdata/*-santa-clara-daily-population-sheet.txt')
# >>> files
# ['/bigdata/20191001-santa-clara-daily-population-sheet.txt', '/bigdata/20190920-santa-clara-daily-population-sheet.txt', '/bigdata/20190921-santa-clara-daily-population-sheet.txt', '/bigdata/20190922-santa-clara-daily-population-sheet.txt', '/bigdata/20190923-santa-clara-daily-population-sheet.txt', '/bigdata/20190924-santa-clara-daily-population-sheet.txt', '/bigdata/20190925-santa-clara-daily-population-sheet.txt', '/bigdata/20190926-santa-clara-daily-population-sheet.txt', '/bigdata/20190928-santa-clara-daily-population-sheet.txt']
# >>> from pyspark.sql import SparkSession
# >>> from glob import glob
# >>> spark = SparkSession.builder.appName('ETL sc-jail-project').getOrCreate()
# >>> files = glob('/bigdata/*-santa-clara-daily-population-sheet.txt')
# >>> files
# ['/bigdata/20191001-santa-clara-daily-population-sheet.txt', '/bigdata/20190920-santa-clara-daily-population-sheet.txt', '/bigdata/20190921-santa-clara-daily-population-sheet.txt', '/bigdata/20190922-santa-clara-daily-population-sheet.txt', '/bigdata/20190923-santa-clara-daily-population-sheet.txt', '/bigdata/20190924-santa-clara-daily-population-sheet.txt', '/bigdata/20190925-santa-clara-daily-population-sheet.txt', '/bigdata/20190926-santa-clara-daily-population-sheet.txt', '/bigdata/20190928-santa-clara-daily-population-sheet.txt']
# >>> sc = spark.sparkContext
# >>> sc.parallelize(files,2).glom().collect()
# [['/bigdata/20191001-santa-clara-daily-population-sheet.txt', '/bigdata/20190920-santa-clara-daily-population-sheet.txt', '/bigdata/20190921-santa-clara-daily-population-sheet.txt', '/bigdata/20190922-santa-clara-daily-population-sheet.txt'], ['/bigdata/20190923-santa-clara-daily-population-sheet.txt', '/bigdata/20190924-santa-clara-daily-population-sheet.txt', '/bigdata/20190925-santa-clara-daily-population-sheet.txt', '/bigdata/20190926-santa-clara-daily-population-sheet.txt', '/bigdata/20190928-santa-clara-daily-population-sheet.txt']]
# >>> from datetime import datetime
# >>> def hoopla(file: str) -> [datetime, str]:
# ...     el = file.split(sep='-', maxsplit=1)
# ...     dt, val = el[0], el[1]
# ...     yield [dt, val]
# ...
# >>> r = sc.parallelize(files).flatMap(hoopla).toDF(['RECORD_DATE', 'FILENAME']).show()
# +-----------------+--------------------+
# |      RECORD_DATE|            FILENAME|
# +-----------------+--------------------+
# |/bigdata/20191001|santa-clara-daily...|
# |/bigdata/20190920|santa-clara-daily...|
# |/bigdata/20190921|santa-clara-daily...|
# |/bigdata/20190922|santa-clara-daily...|
# |/bigdata/20190923|santa-clara-daily...|
# |/bigdata/20190924|santa-clara-daily...|
# |/bigdata/20190925|santa-clara-daily...|
# |/bigdata/20190926|santa-clara-daily...|
# |/bigdata/20190928|santa-clara-daily...|
# +-----------------+--------------------+
