# parquet-arrow

Some basic tools for parquet and arrow files.

## Usage
```sh
Usage: parquet-arrow schema [OPTIONS] --input-file <INPUT_FILE>

Options:
  -i, --input-file <INPUT_FILE>
  -f, --filter <FILTER>
  -t, --tag-filter <TAG_FILTER>  [possible values: true, false]
  -h, --help                     Print help
```

To view the schema of a file:
```sh
    parquet-arrow schema -i <FILENAME>
```

To search for a particular field, use the -f flag, which filters the list
of displayed fields, using the prefix of the provided value. For example,
the following code will only show fields which start with "cpu"
```sh
    parquet-arrow schema -i <FILENAME> -f "cpu"
```

For parquet files which have opaque column names, the --tag-filter flag
searches for prefixes in the field metadata rather than using the column name.
