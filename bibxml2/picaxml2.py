#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 18:59:15 2021

@author: jiemakel
"""

from unicodedata import normalize
from typing import Iterator

import click as click
import lxml.etree
from .lib import convert


def convert_record(record: lxml.etree._ElementIterator) -> Iterator[tuple[int, int, str, str, str]]:
    for field_number, field in enumerate(record, start=1):
        tag = field.attrib['tag']
        for subfield_number, subfield in enumerate(field, start=1):
            yield field_number, subfield_number, tag, subfield.attrib['code'], normalize(subfield.text, 'NFC')

@click.command
@click.option("-o", "--output", help="Output CSV/TSV (gz) / parquet file", required=True)
@click.argument('input', nargs=-1)
def convert_picaxml(input: list[str], output: str):
    """Convert from PICAXML (compressed) INPUT files (actually glob patterns) into (compressed) CSV/TSV/parquet"""
    convert(
        ('{info:srw/schema/5/picaXML-v1.0}record', 'record'),
        convert_record,
        input,
        output
    )


if __name__ == '__main__':
    convert_picaxml()
