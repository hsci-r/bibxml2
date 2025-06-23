#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Feb 15 18:59:15 2021

@author: jiemakel
"""

from typing import Iterator
from unicodedata import normalize
import click as click
import lxml.etree
from .lib import convert

def convert_record(record: lxml.etree._ElementIterator) -> Iterator[tuple[int, int, str, str, str]]:
    for field_number, field in enumerate(record, start = 1):
        if field.tag == '{http://www.loc.gov/MARC21/slim}leader' or field.tag == 'leader':
            yield field_number, 1, 'LDR', '', field.text
        elif field.tag == '{http://www.loc.gov/MARC21/slim}controlfield' or field.tag == 'controlfield':
            yield field_number, 1, field.attrib['tag'], '', field.text
        elif field.tag == '{http://www.loc.gov/MARC21/slim}datafield' or field.tag == 'datafield':
            tag = field.attrib['tag']
            sf = 1
            if field.attrib['ind1'] != ' ':
                yield field_number, sf, tag, 'Y', normalize(field.attrib['ind1'], 'NFC')
                sf += 1
            if field.attrib['ind2'] != ' ':
                yield field_number, sf, tag, 'Z', normalize(field.attrib['ind2'], 'NFC')
                sf += 1
            for subfield_number, subfield in enumerate(field, start=sf):
                yield field_number, subfield_number, tag, subfield.attrib['code'], normalize(subfield.text, 'NFC')
        else:
            print(f'Unknown field {field.tag} in record.')

@click.command
@click.option("-o", "--output", help="Output CSV/TSV (gz) / parquet file", required=True)
@click.argument('input', nargs=-1)
def convert_marcxml(input: list[str], output: str):
    """Convert from MARCXML (compressed) INPUT files (actually glob patterns) into (compressed) CSV/TSV/parquet"""
    convert(
        ('{http://www.loc.gov/MARC21/slim}record', 'record'),
        convert_record,
        input,
        output
    )

if __name__ == '__main__':
    convert_marcxml()
