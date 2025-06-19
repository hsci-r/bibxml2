# bibxml2

A simple converter of (possibly compressed) MARCXML/PICAXML to (possibly compressed) CSV/TSV/parquet.

The resulting CSV/TSV/parquet has been designed to be easy to use as a data table, but also to retain all ordering informaation in the original when such is needed. The format is as follows:
`record_number,field_number,subfield_number,field_code,subfield_code,value`

Here, `record_number` identifies the MARC/PICA+ record, while `field_number` and `subfield_number` can be used for more exact filtering / reconstructing the original field structure/order if needed.

For MARC data fields, `ind1` and `ind2` values are reported as separate rows with the `subfield_code` being `Y` or `Z`, but only when non-empty (MARC requires subfield codes to be lowercase, so this should be relatively safe). The MARC leader is output with field code `LDR`.

## Installation

Install from pypi with e.g. `pipx install bibxml2`.

## Usage

```sh
Usage: marcxml2 [OPTIONS] [INPUT]...

  Convert from MARCXML (compressed) input files into (compressed) CSV/TSV/parquet

Options:
  -o, --output TEXT  Output CSV/TSV (compressed) / parquet file  [required]
  --help             Show this message and exit.
```

```sh
Usage: picaxml2csv [OPTIONS] [INPUT]...

  Convert from PICAXML (compressed) input files into (compressed) CSV/TSV/parquet

Options:
  -o, --output TEXT  Output CSV/TSV (compressed) / parquet file  [required]
  --help             Show this message and exit.
```

If the output file extension is `.parquet`, the output will be in parquet format, compressed with `zstd`, and with field typings maximally compatible with common R and Python ecosystems. Otherwise, compressed files will be read/written if the filename ends with an identifier recognised by fsspec. TSV format will be used if the output filename contains `.tsv`, otherwise CSV will be used.
