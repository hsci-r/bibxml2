from io import BytesIO
from unicodedata import normalize
import pyarrow as pa
import pyarrow.parquet as pq
import csv
from functools import reduce
from typing import Iterable, Iterator, Literal, TextIO, cast

import click as click
import lxml.etree
import fsspec
from fsspec.core import OpenFile, compr, infer_compression
import pymarc
from pymarc import exceptions as exc
from tqdm import tqdm

schema: pa.Schema = pa.schema([ # R compatibility schema
            pa.field('record_number', pa.int32()),
            pa.field('field_number', pa.int32()),
            pa.field('subfield_number', pa.int32()),
            pa.field('field_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('subfield_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('value', pa.string()) 
])

def parquet_writer(output: str, parquet_compression: str, parquet_compression_level: int) -> pq.ParquetWriter:
    return pq.ParquetWriter(output, 
            schema=schema, 
            compression=parquet_compression, 
            compression_level=parquet_compression_level,
#            use_byte_stream_split=['record_number', 'field_number', 'subfield_number'], # type: ignore pyarrow import complains: BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data
            write_page_index=True, 
            use_dictionary=['field_code', 'subfield_code'], # type: ignore
            store_decimal_as_integer=True,
            sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1), pq.SortingColumn(2)],
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

def convert_marcxml_record(record: lxml.etree._ElementIterator) -> Iterator[tuple[int, int, str, str, str]]:
    for field_number, field in enumerate(record, start = 1):
        if field.tag.endswith('leader'):
            yield field_number, 1, 'LDR', '', field.text
        elif field.tag.endswith('controlfield'):
            yield field_number, 1, field.attrib['tag'], '', field.text
        elif field.tag.endswith('datafield'):
            tag = field.attrib['tag']
            sf = 1
            if field.attrib['ind1'] != ' ':
                yield field_number, sf, tag, 'Y', normalize('NFC', field.attrib['ind1'])
                sf += 1
            if field.attrib['ind2'] != ' ':
                yield field_number, sf, tag, 'Z', normalize('NFC', field.attrib['ind2'])
                sf += 1
#            for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=sf): # type: ignore
            for subfield_number, subfield in enumerate(filter(lambda subfield: subfield.text is not None, field), start=sf): # type: ignore
                 yield field_number, subfield_number, tag, subfield.attrib['code'], normalize('NFC', subfield.text)
        else:
            print(f'Unknown field {field.tag} in record.')

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
        elif isinstance(reader.current_exception, exc.FatalReaderError):
            # data file format error
            # reader will raise StopIteration
            print(reader.current_exception)
            print(reader.current_chunk)
        else:
            # fix the record data, skip or stop reading:
            print(reader.current_exception)
            print(reader.current_chunk)
            # break/continue/raise


def yield_from_marcxml_file(inf: OpenFile) -> Iterator[Iterator[tuple[int, int, str, str, str]]]:
    tags = ('{http://www.loc.gov/MARC21/slim}record', 'record', '{info:lc/xmlns/marcxchange-v1}record')
    context = lxml.etree.iterparse(inf, events=('end',), tag=tags)
    for _, elem in context:
        yield convert_marcxml_record(elem)
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

@click.command
@click.option("-f", "--format", help="Input format (marc or pica)", type=click.Choice(['marc', 'pica'], case_sensitive=False), default='marc')
@click.option("-o", "--output", help="Output CSV/TSV (gz) / parquet file", required=True)
@click.option("-c", "--parquet-compression", help="Parquet compression codec", default='zstd')
@click.option("-l", "--parquet-compression-level", help="Parquet compression level", type=int, default=22)
@click.option("-b", "--parquet-batch-size", help="Parquet batch size in bytes", type=int, default=1024*1024*64)
@click.option("-mr", "--max-rows", help="Maximum number of rows per parquet file", type=int, default=1_000_000_000)
@click.argument('input', nargs=-1)
def convert(input: list[str], format: Literal['marc','pica'], output: str, parquet_compression: str, parquet_compression_level: int, parquet_batch_size: int, max_rows: int) -> None:
    writing_parquet = output.endswith('.parquet')
    if writing_parquet:
        if parquet_batch_size > max_rows:
            print(f"Parquet batch size {parquet_batch_size:,} is greater than max rows {max_rows:,}, changing it to {max_rows:,}.")
            parquet_batch_size = max_rows
        pw = parquet_writer(output, parquet_compression, parquet_compression_level)
        pq_rows_written = 0
        pq_file_number = 1
        batch = []
    else:
        of = cast(TextIO, fsspec.open(output, 'wt' if not writing_parquet else 'wb', compression="infer").__enter__())
        cw = csv.writer(of, delimiter='\t' if '.tsv' in output else ',')
        cw.writerow(['record_number', 'field_number', 'subfield_number', 'field_code', 'subfield_code', 'value'])
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
                        if writing_parquet:
                            batch.append((record_number, *row))
                            if len(batch) == parquet_batch_size:
                                pw.write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=parquet_batch_size)
                                batch = []
                                pq_rows_written += parquet_batch_size
                        else:
                            cw.writerow((record_number, *row))
                    record_number += 1
                    if writing_parquet and pq_rows_written >= max_rows:
                        pw.close()
                        pq_rows_written = 0
                        pq_file_number += 1
                        pw = parquet_writer(output.replace(".parquet",f"_{pq_file_number}.parquet"), parquet_compression, parquet_compression_level)
                    pbar.n = processed_files_tsize + oinf.tell()
                    pbar.update(0)
        processed_files_tsize += input_file.fs.size(input_file.path)
    if writing_parquet and batch:
        pw.write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=parquet_batch_size)
        pw.close()
    else:
        of.close()

if __name__ == '__main__':
    convert()
    