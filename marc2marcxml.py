from contextlib import closing
import os
import click
import fsspec

from pymarc import MARCReader, XMLWriter
from pymarc import exceptions as exc
from typing import cast
from tqdm.auto import tqdm

@click.command()
@click.option('--input', '-i', type=str, required=True, help='Input MARC file path')
@click.option('--output', '-o', type=str, required=True, help='Output MARCXML file path')
def convert(input: str, output:str) -> None:
    with cast(fsspec.core.OpenFile, fsspec.open(input, 'rb', compression='infer')) as fh, closing(MARCReader(fh)) as reader, cast(fsspec.core.OpenFile, fsspec.open(output, 'wb', compression='infer')) as out_fh, closing(XMLWriter(out_fh)) as writer, closing(tqdm(
            total=os.path.getsize(input),
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
        )) as pbar:
        for record in reader:
            if record:
                # consume the record:
                writer.write(record)
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
            pbar.n = fh.tell()
            pbar.refresh()

if __name__ == '__main__':
    convert()