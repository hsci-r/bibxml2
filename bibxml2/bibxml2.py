from contextlib import nullcontext
from unicodedata import normalize
import pyarrow as pa
import pyarrow.parquet as pq
import csv
from functools import reduce
from typing import Iterator, Literal, cast

import click as click
import lxml.etree
import fsspec
from fsspec.core import OpenFile, compr, infer_compression
import _csv
from tqdm.auto import tqdm

schema: pa.Schema = pa.schema([ # R compatibility schema
            pa.field('record_number', pa.int32()),
            pa.field('field_number', pa.int32()),
            pa.field('subfield_number', pa.int32()),
            pa.field('field_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('subfield_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('value', pa.string()) 
])

def convert_marc_record(record: lxml.etree._ElementIterator) -> Iterator[tuple[int, int, str, str, str]]:
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
            for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=sf): # type: ignore
                yield field_number, subfield_number, tag, subfield.attrib['code'], normalize('NFC', subfield.text)
        else:
            print(f'Unknown field {field.tag} in record.')

def convert_pica_record(record: lxml.etree._ElementIterator) -> Iterator[tuple[int, int, str, str, str]]:
    for field_number, field in enumerate(record, start=1):
        tag = field.attrib['tag']
        for subfield_number, subfield in enumerate(filter(lambda subfield: not (subfield.text is None and print(f"No text in subfield {subfield.attrib['code']} of field {tag} in field {lxml.etree.tostring(field, encoding='unicode')}") is None), field), start=1): # type: ignore
            yield field_number, subfield_number, tag, subfield.attrib['code'], normalize('NFC', subfield.text)

@click.command
@click.option("-f", "--format", help="Input format (marc or pica)", type=click.Choice(['marc', 'pica'], case_sensitive=False), default='marc')
@click.option("-o", "--output", help="Output CSV/TSV (gz) / parquet file", required=True)
@click.option("-c", "--parquet-compression", help="Parquet compression codec", default='zstd')
@click.option("-l", "--parquet-compression-level", help="Parquet compression level", type=int, default=22)
@click.option("-b", "--parquet-batch-size", help="Parquet batch size in bytes", type=int, default=1024*1024*64)
@click.argument('input', nargs=-1)
def convert(input: list[str], format: Literal['marc','pica'], output: str, parquet_compression: str, parquet_compression_level: int, parquet_batch_size: int) -> None:
    if format == 'marc':
        tags = ('{http://www.loc.gov/MARC21/slim}record', 'record', '{info:lc/xmlns/marcxchange-v1}record')
        convert_record = convert_marc_record
    else:
        tags = ('{info:srw/schema/5/picaXML-v1.0}record', 'record')
        convert_record = convert_pica_record
    writing_parquet = output.endswith('.parquet')
    with cast(OpenFile, fsspec.open(output, 'wt' if not writing_parquet else 'wb', compression="infer")) as of, pq.ParquetWriter(of, 
            schema=schema, 
            compression=parquet_compression, 
            compression_level=parquet_compression_level,
#            use_byte_stream_split=['record_number', 'field_number', 'subfield_number'], # type: ignore pyarrow import complains: BYTE_STREAM_SPLIT encoding is only supported for FLOAT or DOUBLE data
            write_page_index=True, 
            use_dictionary=['field_code', 'subfield_code'], # type: ignore
            store_decimal_as_integer=True,
            sorting_columns=[pq.SortingColumn(0), pq.SortingColumn(1), pq.SortingColumn(2)],
        ) if writing_parquet else nullcontext(csv.writer(of, delimiter='\t' if '.tsv' in output else ',')) as ow:
        if writing_parquet:
            batch = []
        else:
            cast(_csv.Writer, ow).writerow(['record_number', 'field_number', 'subfield_number', 'field_code', 'subfield_code', 'value'])            
        n = 1
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
                    context = lxml.etree.iterparse(inf, events=('end',), tag=tags)
                    for _, elem in context:
                        for row in convert_record(elem):
                            if writing_parquet:
                                batch.append((n, *row))
                                if len(batch) == parquet_batch_size:
                                    cast(pq.ParquetWriter, ow).write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=parquet_batch_size)
                                    batch = []
                            else:
                                cast(_csv.Writer, ow).writerow((n, *row))
                        n += 1
                        elem.clear()
                        while elem.getprevious() is not None:
                            del elem.getparent()[0]
                        pbar.n = processed_files_tsize + oinf.tell()
                        pbar.update(0)
                    del context
            processed_files_tsize += input_file.fs.size(input_file.path)
        if writing_parquet and batch:
            cast(pq.ParquetWriter, ow).write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=parquet_batch_size)

if __name__ == '__main__':
    convert()
    