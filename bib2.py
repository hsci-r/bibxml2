from io import BytesIO
import os
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

def parquet_writer(output: str, parquet_compression: str, parquet_compression_level: Optional[int], sort_by_field_code: bool) -> pq.ParquetWriter:
    return pq.ParquetWriter(output, 
            schema=schema, 
            compression=parquet_compression, 
            compression_level=parquet_compression_level,
#            use_byte_stream_split=['record_number', 'field_number', 'subfield_number'], # type: ignore pyarrow import complains: BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data
#            write_page_index=True, 
#            use_dictionary=['field_code', 'subfield_code'], # type: ignore
#            store_decimal_as_integer=True,
#            sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1), pq.SortingColumn(2)] if not sort_by_field_code else [pq.SortingColumn(3), pq.SortingColumn(0), pq.SortingColumn(1), pq.SortingColumn(2)],
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
        for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=1): # type: ignore
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
@click.option("-pc", "--parquet-compression", help="Parquet compression codec", default='zstd')
@click.option("-pcl", "--parquet-compression-level", help="Parquet compression level", type=int, default=22)
@click.option("-prgs", "--parquet-row-group-size", help="Parquet row group size in bytes", type=int, default=1024*1024)
@click.option("-pmrpf", "--parquet-max-rows-per-file", help="Maximum number of rows per parquet file", type=int, default=1_000_000_000)
@click.option("-ps", "--parquet-sort", flag_value=True, default=False, help="Sort parquet output by field_code")
@click.option("-pd", "--parquet-use-duckdb", flag_value=True, default=False, help="Write final parquet files using DuckDB for better parquet statistics")
@click.argument('input', nargs=-1)
def convert(input: list[str], format: Literal['marc','pica'], output: str, parquet_compression: str, parquet_compression_level: int, parquet_row_group_size: int, parquet_max_rows_per_file: int, parquet_sort: bool, parquet_use_duckdb: bool) -> None:
    if output.endswith('.parquet'):
        if parquet_row_group_size > parquet_max_rows_per_file:
            print(f"Parquet row group size {parquet_row_group_size:,} is greater than max rows {parquet_max_rows_per_file:,}, changing it to {parquet_max_rows_per_file:,}.")
            parquet_row_group_size = parquet_max_rows_per_file
        cur_output = output
        pq_rows_written = 0
        pq_file_number = 1
        if not parquet_sort:
            pw = parquet_writer(cur_output, parquet_compression, parquet_compression_level if not duckdb else None, False)
            for batch in yield_batches(input, format, parquet_row_group_size, schema):
                pw.write_batch(batch)
                pq_rows_written += batch.num_rows
                if pq_rows_written >= parquet_max_rows_per_file:
                    pw.close()
                    pq_rows_written = 0
                    pq_file_number += 1
                    if parquet_use_duckdb:
                        duckdb.query(f"COPY '{cur_output}' TO '{cur_output}.duckdbtmp' (FORMAT 'parquet', COMPRESSION '{parquet_compression}', COMPRESSION_LEVEL {parquet_compression_level})")
                        os.remove(cur_output)
                        os.rename(cur_output + ".duckdbtmp", cur_output)
                    cur_output = output.replace(".parquet",f"_{pq_file_number}.parquet")
                    pw = parquet_writer(cur_output, parquet_compression, parquet_compression_level if not duckdb else None, False)
            pw.close()
        else:
            with parquet_writer(output+".tmp", parquet_compression, None, False) as pw:
                for batch in yield_batches(input, format, parquet_row_group_size, schema):
                    pw.write_batch(batch)
            pr = ds.dataset(output+".tmp")
            field_codes = set()
            for batch in pr.to_batches(columns=['field_code']):
                field_codes.update(batch.column('field_code').unique().to_pylist())
            pw = parquet_writer(cur_output, parquet_compression, parquet_compression_level if not duckdb else None, False)
            batches = []
            batch_size = 0
            for field_code in tqdm(sorted(field_codes)):
                for batch in pr.scanner(filter=pc.field('field_code') == field_code).to_batches():
                    if batch.num_rows > 0:
                        batches.append(batch)
                        batch_size += batch.num_rows
                        if batch_size >= parquet_row_group_size:
                            pw.write_batch(pa.concat_batches(batches), row_group_size=parquet_row_group_size)
                            pq_rows_written += batch_size
                            if pq_rows_written >= parquet_max_rows_per_file:
                                pw.close()
                                pq_rows_written = 0
                                pq_file_number += 1
                                if parquet_use_duckdb:
                                    duckdb.query(f"COPY '{cur_output}' TO '{cur_output}.duckdbtmp' (FORMAT 'parquet', COMPRESSION '{parquet_compression}', COMPRESSION_LEVEL {parquet_compression_level})")
                                    os.remove(cur_output)
                                    os.rename(cur_output + ".duckdbtmp", cur_output)
                                cur_output = output.replace(".parquet",f"_{pq_file_number}.parquet")
                                pw = parquet_writer(cur_output, parquet_compression, parquet_compression_level if not duckdb else None, False)
                            batches = []
                            batch_size = 0
            if batches:
                pw.write_batch(pa.concat_batches(batches), row_group_size=parquet_row_group_size)
            os.remove(output+".tmp")
        pw.close()
        if parquet_use_duckdb:
            duckdb.query(f"COPY '{cur_output}' TO '{cur_output}.duckdbtmp' (FORMAT 'parquet', COMPRESSION '{parquet_compression}', COMPRESSION_LEVEL {parquet_compression_level})")
            os.remove(cur_output)
            os.rename(cur_output + ".duckdbtmp", cur_output)            
    else:
        with fsspec.open(output, 'wt', compression="infer") as of:
            cw = csv.writer(cast(TextIO, of), delimiter='\t' if '.tsv' in output else ',')
            cw.writerow(['record_number', 'field_number', 'subfield_number', 'field_code', 'subfield_code', 'value'])
            for row in yield_rows(input, format):
                cw.writerow(row)

if __name__ == '__main__':
    convert()
    