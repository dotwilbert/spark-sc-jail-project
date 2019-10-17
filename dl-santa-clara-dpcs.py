#! /usr/bin/env python

"""Download Santa Clara County Jail Population Count Sheet
   
   Downloads the population count sheet for the Santa Clara
   County Jail. It opens the container page, locates the
   link and downloads the pdf.
"""
import argparse
import pathlib
from datetime import datetime
from html.parser import HTMLParser

import requests
from requests.exceptions import ConnectionError


class PopCountLinkContainerHTMLParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self._linkFound = False
        self._scratchUrl = ''
        self.populationCountURL = ''
        self._done = False

    def handle_starttag(self, tag, attrs):
        if not self._done and tag == 'a':
            self._linkFound = True
            for k, v in attrs:
                if k == 'href':
                    self._scratchUrl = v

    def handle_endtag(self, tag):
        if not self._done and tag == 'a':
            self._linkFound = False

    def handle_data(self, data):
        if not self._done and isinstance(data, str) and self._linkFound and 'inmate population' in data.lower():
            self.populationCountURL = self._scratchUrl
            self._done = True


def get_population_count_url(url: str):
    assert url != '', 'Please provide a container URL'
    retries = 0
    while True:
        try:
            r = requests.get(url, timeout=10)
            retries += 1
        except ConnectionError as e:
            if retries <= 3:
                continue
            raise RuntimeError(
                'Download container: Remote host disconnected 3 times') from e
        break
    r.raise_for_status()
    assert r.headers.get(
        'content-type').lower() == 'text/html; charset=utf-8', 'Unknown document encoding'
    p = PopCountLinkContainerHTMLParser()
    p.feed(r.content.decode('utf-8'))
    r.close()
    return p.populationCountURL


def download_population_count_sheet(url: str, filename: str):
    assert url != '', 'Please provide a document url to download'
    assert filename != '', 'Please provide a target filename'
    retries = 0
    while True:
        try:
            r = requests.get(url, timeout=10, stream=True)
            retries += 1
        except ConnectionError as e:
            if retries <= 3:
                continue
            raise RuntimeError(
                'Download document: Remote host disconnected 3 times') from e
        break
    r.raise_for_status()
    with open(filename, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=128):
            fd.write(chunk)
    r.close()


if __name__ == '__main__':
    default_filename = datetime.now().strftime(
        '%Y%m%d') + '-santa-clara-daily-population-sheet.pdf'
    container_url = 'https://www.sccgov.org/sites/sheriff/Pages/custody.aspx'

    parser = argparse.ArgumentParser(
        description='Download Santa Clara Daily Inmate Population Count sheet ')
    parser.add_argument(
        '-o', '--outfile', help=f'filename to store the sheet (default: {default_filename})', default=default_filename)
    parser.add_argument('-v', '--version', action='store_true', help='display program version')
    args = parser.parse_args()

    if pathlib.Path(args.outfile).exists():
        print(f'File {args.outfile} exists. Will not overwrite.')
    else:
        download_population_count_sheet(get_population_count_url(container_url), args.outfile)
