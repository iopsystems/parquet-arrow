# parquet-arrow

Some basic tools for parquet and arrow files.

## Schema

Displays the Arrow schema, including metadata, for the file.

### Usage
```sh
Usage: parquet-arrow <COMMAND>

Commands:
  schema               Schema and metadata for the parquet file
  row-group-info       Row group information: number and size of row groups
  compare-schema       Compare parquet schemas
  compare              Compare parquet data
  validate             Validate delta counters in a postprocessd parquet file
  add-cgroup-metadata  Augment file with additional metadata around cgroups
  help                 Print this message or the help of the given subcommand(s)

Options:
  -h, --help  Print help
```

## Schema
Displays the file-level and field-level metadata for a parquet file. The
output schema can be saved to a JSON file using the `-o` flag:
```sh
    parquet-arrow schema -i <FILENAME> -o schema.json
```

To search for a particular field, use the `-f` flag, which filters the list
of displayed fields, using the prefix of the provided value. The data is
assumed to be of Rezolus v5+ type, and so the field name is matched with
the pattern from the metadata, rather than the column name. Filtered data
is only for stdout; any output JSON file will have the entire schema.

To search for metrics which start with "cpu"
```sh
    parquet-arrow schema -i <FILENAME> -f "cpu"
```

## RowGroupInfo
Displays the number of row groups and the size of each row group.

## CompareSchema
Compares schema between the left and the right parquet file.

## Compare
Compares values from all non-histogram columns from two parquet files. Assumes
that the columns are either `int64`, `uint64`, or `double` values.

## Validate
Checks if there are any negative values in columns with type `delta_counter`.
This operates on postprocessed artifacts.

## AddCgroupMetadata
Augments information about cgroups in the parquet file with additional data
from an external metadata file. The external metadata file is a CSV file
of the format: `<CGROUP_PATTERN>, <TAG_1>, <TAG_2>, .. <TAG_N>`, where the tags
can have any arbitrary name or data. An example of such a CSV file is at
`examples/add-cgroup-metadata.csv`.

The parquet file is parsed and any cgroup metric which has the pattern in its
name has the tags added to its metadata. The tag names are automatically
derived from the CSV header. It is assumed that each cgroup metric matches only
a single pattern from the additional mappings file. If it matches multiple
patterns, the first one wins.
