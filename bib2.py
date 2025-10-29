import glob
from io import BytesIO
import os
import re
import shutil
from unicodedata import normalize
import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.compute as pc
import csv
from functools import reduce
from typing import Iterable, Iterator, Literal, Optional, TextIO, cast

import click as click
import lxml.etree
import fsspec
from fsspec.core import OpenFile, compr, infer_compression
import pymarc
from tqdm import tqdm

schema: pa.Schema = pa.schema([ # R compatibility schema
            pa.field('record_number', pa.int32()),
            pa.field('field_number', pa.int32()),
            pa.field('subfield_number', pa.int32()),
            pa.field('field_code', pa.string()),
            pa.field('subfield_code', pa.string()),
            #pa.field('field_code', pa.dictionary(pa.int32(), pa.string())),
            #pa.field('subfield_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('value', pa.string()) 
])

def parquet_writer(output: str, parquet_compression: str, parquet_compression_level: Optional[int], schema: pa.Schema = schema) -> pq.ParquetWriter:
    return pq.ParquetWriter(output, 
            schema=schema, 
            compression=parquet_compression, 
            compression_level=parquet_compression_level,
        ) 

def convert_marc_record(record: pymarc.record.Record) -> Iterator[tuple[int, int, str, str, str]]:
    yield 1, 1, 'LDR', '', str(record.leader)
    for field_number, field in enumerate(record, start=2):
        if field.control_field:
            yield field_number, 1, field.tag, '', field.value()
        else:
            sf = 1
            if field.indicator1 != ' ':
                yield field_number, sf, field.tag, 'Y', normalize('NFC', field.indicator1)
                sf += 1
            if field.indicator2 != ' ':
                yield field_number, sf, field.tag, 'Z', normalize('NFC', field.indicator2)
                sf += 1
            for subfield_number, subfield in enumerate(filter(lambda subfield: subfield.value is not None, field.subfields), start=sf):
                yield field_number, subfield_number, field.tag, subfield.code, normalize('NFC', subfield.value)

def convert_marcxml_record(record: lxml.etree._ElementIterator) -> Iterator[Iterator[tuple[int, int, str, str, str]]]:
    fields = []
    for field_number, field in enumerate(record, start = 1):
        if field.tag.endswith('leader'):
            fields.append((field_number, 1, 'LDR', '', field.text))
        elif field.tag.endswith('controlfield'):
            fields.append((field_number, 1, field.attrib['tag'], '', field.text))
        elif field.tag.endswith('datafield'):
            tag = field.attrib['tag']
            sf = 1
            if field.attrib['ind1'] != ' ':
                fields.append((field_number, sf, tag, 'Y', normalize('NFC', field.attrib['ind1'])))
                sf += 1
            if field.attrib['ind2'] != ' ':
                fields.append((field_number, sf, tag, 'Z', normalize('NFC', field.attrib['ind2'])))
                sf += 1
#            for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=sf): # type: ignore
            for subfield_number, subfield in enumerate(filter(lambda subfield: subfield.text is not None, field), start=sf): # type: ignore
                if len(subfield) > 0: # Found an embedded MARC record
                    fields.append((field_number, subfield_number, tag, 'X', subfield.attrib['id']))
                    yield from convert_marcxml_record(subfield)
                else:
                    fields.append((field_number, subfield_number, tag, subfield.attrib['code'], normalize('NFC', subfield.text)))
        else:
            print(f'Unknown field {field.tag} in record.')
    yield iter(fields)

def convert_picaxml_record(record: Iterable[lxml.etree._Element]) -> Iterator[tuple[int, int, str, str, str]]:
    for field_number, field in enumerate(record, start=1):
        tag = field.attrib['tag']
        #for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=1): # type: ignore
        for subfield_number, subfield in enumerate(filter(lambda subfield: subfield.text is not None, field), start=1): # type: ignore
            yield field_number, subfield_number, tag, subfield.attrib['code'], normalize('NFC', subfield.text)

def yield_from_marc_file(inf: BytesIO) -> Iterator[Iterator[tuple[int, int, str, str, str]]]:
    reader = pymarc.MARCReader(inf, to_unicode=True)
    for record in reader:
        if record:
            yield convert_marc_record(record)
        else:
            # data file format error
            # reader will raise StopIteration if it is fatal
            print(reader.current_exception)
            print(reader.current_chunk)

def yield_from_marcxml_file(inf: OpenFile) -> Iterator[Iterator[tuple[int, int, str, str, str]]]:
    tags = ('{http://www.loc.gov/MARC21/slim}record', 'record', '{info:lc/xmlns/marcxchange-v1}record', '{info:lc/xmlns/marcxchange-v2}record')
    context = lxml.etree.iterparse(inf, events=('end',), tag=tags)
    for _, elem in context:
        yield from convert_marcxml_record(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

def yield_from_picaxml_file(inf: OpenFile) -> Iterator[Iterator[tuple[int, int, str, str, str]]]:
    tags = ('{info:srw/schema/5/picaXML-v1.0}record', 'record')
    context = lxml.etree.iterparse(inf, events=('end',), tag=tags)
    for _, elem in context:
        yield convert_picaxml_record(elem)
        elem.clear()
        while elem.getprevious() is not None:
            del elem.getparent()[0]

def yield_rows(input: list[str], format: Literal['marc','pica']) -> Iterator[tuple[int, int, int, str, str, str]]:
    record_number = 1
    input_files = fsspec.open_files(input, 'rb')
    tsize = reduce(lambda tsize, inf: tsize + inf.fs.size(inf.path), input_files, 0)
    pbar = tqdm(total=tsize, unit='b', smoothing=0, unit_scale=True, unit_divisor=1024, dynamic_ncols=True)
    processed_files_tsize = 0
    for input_file in input_files:
        pbar.set_description(f"Processing {input_file.path}")
        with input_file as oinf:
                compression = infer_compression(input_file.path)
                if compression is not None:
                    inf = compr[compression](oinf, mode='rb') # type: ignore
                else:
                    inf = oinf
                if format == 'pica':
                    yield_records = yield_from_picaxml_file(inf)
                elif not '.xml' in input_file.path and not '.mrcx' in input_file.path:
                    yield_records = yield_from_marc_file(inf)
                else:
                    yield_records = yield_from_marcxml_file(inf)
                for record in yield_records:
                    for row in record:
                        yield record_number, *row
                    record_number += 1
                    pbar.n = processed_files_tsize + oinf.tell()
                    pbar.update(0)
        processed_files_tsize += input_file.fs.size(input_file.path)

def yield_batches(input: list[str], format: Literal['marc','pica'], parquet_batch_size: int, schema: pa.Schema) -> Iterator[pa.RecordBatch]:
    batch = []
    for row in yield_rows(input, format):
        batch.append(row)
        if len(batch) == parquet_batch_size:
            yield pa.record_batch(list(zip(*batch)), schema=schema)
            batch = []
    if batch:
        yield pa.record_batch(list(zip(*batch)), schema=schema)

@click.command
@click.option("-f", "--format", help="Input format (marc or pica)", type=click.Choice(['marc', 'pica'], case_sensitive=False), default='marc')
@click.option("-o", "--output", help="Output CSV/TSV (gz) / parquet file", required=True)
@click.option("-s", "--max-file-size", help="Maximum size of parquet files in bytes (default 4,000,000,000)", type=int, default=4_000_000_000)
@click.argument('input', nargs=-1)
def convert(input: list[str], format: Literal['marc','pica'], output: str, max_file_size: int) -> None:
    if output.endswith('.parquet'):
        shutil.rmtree(output + ".tmp", ignore_errors=True)
        print("Writing to temporary parquet dataset to split data by field_code:")
        ds.write_dataset(yield_batches(input, format, 1024*1024, schema), output+".tmp", format='parquet', partitioning_flavor="hive", partitioning=["field_code"], schema=schema, min_rows_per_group=2**16, file_options=ds.ParquetFileFormat().make_write_options(compression='zstd'))
        duckdb.query("SET enable_progress_bar_print=TRUE")
        duckdb.query("SET progress_bar_time=0")
        duckdb.query("SET threads=1")
        print("Coalescing and optimising into unified parquet(s):")
        shutil.rmtree(output, ignore_errors=True)
        duckdb.query(f"COPY (SELECT * FROM parquet_scan('{output}.tmp/*/*.parquet', hive_partitioning=TRUE)) TO '{output}.tmp.2' (FORMAT 'parquet', COMPRESSION 'zstd', COMPRESSION_LEVEL 22, STRING_DICTIONARY_PAGE_SIZE_LIMIT 100_000, FILE_SIZE_BYTES {max_file_size})")
        shutil.rmtree(output+".tmp", ignore_errors=True)
        print("Renaming final parquet files:")
        for file in tqdm(glob.glob(f"{output}.tmp.2/data_*.parquet")):
            part = re.search(r'data(_\d+).parquet', file).group(1)
            if part == "_0":
                part = ""
            shutil.move(file, output.replace('.parquet', part+'.parquet'))
        shutil.rmtree(output+".tmp.2", ignore_errors=True)
    else:
        with fsspec.open(output, 'wt', compression="infer") as of:
            cw = csv.writer(cast(TextIO, of), delimiter='\t' if '.tsv' in output else ',')
            cw.writerow(['record_number', 'field_number', 'subfield_number', 'field_code', 'subfield_code', 'value'])
            for row in yield_rows(input, format):
                cw.writerow(row)

if __name__ == '__main__':
    convert()
    