# parquet-arrow

Some basic tools for parquet and arrow files.

## Schema

Displays the Arrow schema, including metadata, for the file.

### Usage
```sh
Usage: parquet-arrow schema [OPTIONS] --input-file <INPUT_FILE>

Options:
  -i, --input-file <INPUT_FILE>
  -f, --filter <FILTER>
  -t, --tag-filter <TAG_FILTER>  [possible values: true, false]
  -h, --help                     Print help
```

To search for a particular field, use the -f flag, which filters the list
of displayed fields, using the prefix of the provided value. For example,
the following code will only show fields which start with "cpu"
```sh
    parquet-arrow schema -i <FILENAME> -f "cpu"
```

For parquet files which have opaque column names, the --tag-filter flag
searches for prefixes in the field metadata rather than using the column name.

To write the schema out into a JSON file use the -o flag:
```sh
    parquet-arrow schema -i <FILENAME> -o schema.json
```
## Validate

Checks if there are any negative values in columns with type `delta_counter`.
This operates on postprocessed artifacts.

### Usage
```sh
Usage: parquet-arrow validate --input-file <INPUT_FILE>

Options:
  -i, --input-file <INPUT_FILE>
  -h, --help                     Print help
```

## Compare

Compares values from a single column from two different parquet files. Assumes
that the columns are `double` values.

### Usage
```sh
Usage: parquet-arrow compare --left <LEFT> --right <RIGHT> --column <COLUMN>

Options:
  -l, --left <LEFT>
  -r, --right <RIGHT>
  -c, --column <COLUMN>
  -h, --help             Print help
```
