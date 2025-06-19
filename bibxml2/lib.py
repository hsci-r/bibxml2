from contextlib import nullcontext
import inspect
import pyarrow as pa
import pyarrow.parquet as pq
import csv
from functools import reduce
from typing import Callable, Iterator, cast

import click as click
import lxml.etree
import fsspec
from fsspec.core import OpenFile, compr, infer_compression
import _csv
from tqdm.auto import tqdm

#batch_size = 1024*1024  # 1 MiB
batch_size = 64*1024*1024  # 64 MiB

schema: pa.Schema = pa.schema([ # R compatibility schema
            pa.field('record_number', pa.int32()),
            pa.field('field_number', pa.int32()),
            pa.field('subfield_number', pa.int32()),
            pa.field('field_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('subfield_code', pa.dictionary(pa.int32(), pa.string())),
            pa.field('value', pa.string()) 
])

def convert(tag: str, convert_record: Callable[[lxml.etree._ElementIterator], Iterator[tuple[int, int, str, str, str]]], input: list[str], output: str) -> None:
    writing_parquet = output.endswith('.parquet')
    with cast(OpenFile, fsspec.open(output, 'wt' if not writing_parquet else 'wb', compression="infer")) as of, pq.ParquetWriter(of, 
            schema=schema, 
            compression='zstd', 
            compression_level=22,
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
                    context = lxml.etree.iterparse(inf, events=('end',), tag=tag)
                    for _, elem in context:
                        for row in convert_record(elem):
                            if writing_parquet:
                                batch.append((n, *row))
                                if len(batch) == batch_size:
                                    cast(pq.ParquetWriter, ow).write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=batch_size)
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
            cast(pq.ParquetWriter, ow).write_batch(pa.record_batch(list(zip(*batch)), schema=schema), row_group_size=batch_size)