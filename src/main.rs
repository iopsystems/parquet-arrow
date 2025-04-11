#![allow(dead_code)]
#![allow(clippy::needless_return)]

use arrow::array::*;
use arrow::csv::writer::Writer;
use arrow::datatypes::*;
use arrow::ipc::reader::FileReader;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::ipc::CompressionType;
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::*;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::properties::*;
use rand::*;
use std::collections::{BTreeSet, HashMap};
use std::fs::File;
use std::process::exit;
use std::sync::Arc;

#[derive(Subcommand)]
enum Commands {
    Generate {
        #[arg(short, long)]
        compression: bool,

        #[arg(short, long)]
        list: bool,

        #[arg(short, long)]
        array: bool,

        #[arg(short, long)]
        keepfile: bool,
    },
    Schema {
        #[arg(short, long)]
        input_file: String,

        #[arg(short, long)]
        filter: Option<String>,

        #[arg(short, long)]
        tag_filter: Option<bool>,

        #[arg(short, long)]
        output_file: Option<String>,
    },
    Metadata {
        #[arg(short, long)]
        input_file: String,
    },
    Convert {
        #[arg(short, long)]
        input_file: String,

        #[arg(short, long)]
        compression: bool,
    },
    Add {
        #[arg(short, long)]
        input_file: String,
    },
    Validate {
        #[arg(short, long)]
        input_file: String,
    },
}

#[derive(Parser)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

const STRINGS: &[&str] = &["foo", "bar", "baz"];

fn build_u64_column(
    name: &str,
    data: Vec<u64>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let h = HashMap::from([("type".to_owned(), "u64".to_owned())]);
    let field = Field::new(name, DataType::UInt64, false).with_metadata(h);
    let series = Arc::new(UInt64Array::from(data));
    pq_cols.push(field);
    pq_series.push(series);
}

fn build_f64_column(
    name: &str,
    data: Vec<f64>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let h = HashMap::from([("type".to_owned(), "f64".to_owned())]);
    let field = Field::new(name, DataType::Float64, false).with_metadata(h);
    let series = Arc::new(Float64Array::from(data));
    pq_cols.push(field);
    pq_series.push(series);
}

fn build_string_column(
    name: &str,
    data: Vec<String>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let h = HashMap::from([("type".to_owned(), "string".to_owned())]);
    let field = Field::new(name, DataType::Utf8, false).with_metadata(h);
    let series = Arc::new(StringArray::from(data));
    pq_cols.push(field);
    pq_series.push(series);
}

fn build_u64_list_column(
    name: &str,
    data: Vec<Vec<u64>>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let h = HashMap::from([("type".to_owned(), "u64_array".to_owned())]);
    let mut builder = ListBuilder::new(UInt64Builder::new());
    let field =
        Field::new(name, DataType::new_list(DataType::UInt64, true), false).with_metadata(h);
    for inner in data {
        builder.append_value(inner.iter().map(|x| Some(*x)).collect::<Vec<_>>());
    }
    pq_cols.push(field);
    pq_series.push(Arc::new(builder.finish()));
}

fn build_u64_array_column(
    name: &str,
    data: Vec<Vec<u64>>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let h = HashMap::from([("type".to_owned(), "u64_array".to_owned())]);
    let mut builder = FixedSizeListBuilder::new(UInt64Builder::new(), 3);
    let field = Field::new(
        name,
        DataType::FixedSizeList(Arc::new(Field::new("item", DataType::UInt64, true)), 3),
        false,
    )
    .with_metadata(h);

    for inner in data {
        builder.values().append_slice(&inner);
        builder.append(true);
    }
    pq_cols.push(field);
    pq_series.push(Arc::new(builder.finish()));
}

fn write_parquet(
    output: String,
    list: bool,
    array: bool,
    compression: bool,
    schema: SchemaRef,
    nrows: u8,
    nbatches: u8,
) {
    let file = File::create(output).unwrap();

    let props = match compression {
        false => WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .build(),
        true => WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build(),
    };

    let mut writer = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
    for _ in 0..nbatches {
        let batch = generate_batch(nrows, list, array);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

fn write_ipc(output: String, compression: bool, batch: &RecordBatch) {
    let file = File::create(output).unwrap();

    let opts = match compression {
        false => IpcWriteOptions::default(),
        true => IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap(),
    };

    let mut writer = FileWriter::try_new_with_options(file, &batch.schema(), opts).unwrap();
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn write_csv(output: String, batch: &RecordBatch) {
    let file = File::create(output).unwrap();
    let mut writer = Writer::new(file);
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn generate_data(
    nrows: u8,
) -> (
    Vec<String>,
    Vec<u64>,
    Vec<f64>,
    Vec<Vec<u64>>,
    Vec<Vec<u64>>,
) {
    let mut rng = rand::thread_rng();
    let mut strings: Vec<String> = Vec::new();
    let mut nums: Vec<u64> = Vec::new();
    let mut floats: Vec<f64> = Vec::new();
    let mut lists: Vec<Vec<u64>> = Vec::new();
    let mut arrays: Vec<Vec<u64>> = Vec::new();

    for _ in 0..nrows {
        nums.push(rng.gen_range(0..100));
        floats.push(rng.gen_range(0.0..10.0));
        strings.push(STRINGS[rng.gen_range(0..3)].to_owned());

        let mut inner: Vec<u64> = vec![0; 3];
        for j in 0..3 {
            inner[j] = rng.gen_range(0..25);
        }
        lists.push(inner.clone());
        arrays.push(inner);
    }

    return (strings, nums, floats, lists, arrays);
}

fn generate_batch(nrows: u8, list: bool, array: bool) -> RecordBatch {
    let mut pq_cols: Vec<Field> = Vec::new();
    let mut pq_series: Vec<Arc<dyn Array>> = Vec::new();

    let (strings, nums, floats, lists, arrays) = generate_data(nrows);

    build_u64_column("nums", nums, &mut pq_cols, &mut pq_series);
    build_f64_column("floats", floats, &mut pq_cols, &mut pq_series);
    build_string_column("strings", strings, &mut pq_cols, &mut pq_series);
    if list {
        build_u64_list_column("lists", lists, &mut pq_cols, &mut pq_series);
    }
    if array {
        build_u64_array_column("arrays", arrays, &mut pq_cols, &mut pq_series);
    }

    let schema = Schema::new(pq_cols);
    let batch = RecordBatch::try_new(Arc::new(schema), pq_series).unwrap();

    return batch;
}

fn build_arrow(output: &str, nrows: u8, compression: bool, list: bool, array: bool) {
    let batch = generate_batch(nrows, list, array);
    write_parquet(
        output.to_owned() + ".parquet",
        list,
        array,
        compression,
        batch.schema(),
        nrows,
        2,
    );
}

fn read_parquet_batch(input: &str) -> RecordBatch {
    let file = File::open(input).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    let record_batch = reader.next().unwrap().unwrap();
    return record_batch;
}

fn read_parquet(input: &str) {
    let name = input.to_owned() + ".parquet";
    let record_batch = read_parquet_batch(&name);
    for i in 0..record_batch.num_columns() {
        let col = &record_batch.columns()[i];
        println!("Column {}: {:#?}", i, col);
    }
}

fn read_parquet_schema(input: &str) -> SchemaRef {
    let file = File::open(input).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    return builder.schema().clone();
}

fn read_ipc_schema(input: &str) -> SchemaRef {
    let reader = FileReader::try_new(File::open(input).unwrap(), None).unwrap();
    return reader.schema();
}

fn read_parquet_metadata(input: &str) -> Arc<ParquetMetaData> {
    let file = File::open(input).unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    return builder.metadata().clone();
}

fn read_ipc_metadata(input: &str) -> HashMap<String, String> {
    let reader = FileReader::try_new(File::open(input).unwrap(), None).unwrap();
    return reader.custom_metadata().clone();
}

fn main() {
    let args = CliArgs::parse();

    match &args.command {
        Commands::Generate {
            compression,
            list,
            array,
            keepfile,
        } => {
            let name = "foobar";
            build_arrow(name, 3, *compression, *list, *array);
            read_parquet(name);

            if !keepfile {
                let _ = std::fs::remove_file(name.to_owned() + ".parquet");
            }
        }
        Commands::Schema {
            input_file,
            filter,
            tag_filter,
            output_file,
        } => {
            let schema = match input_file.ends_with(".parquet") {
                true => read_parquet_schema(input_file),
                false => read_ipc_schema(input_file),
            };
            let tf = tag_filter.unwrap_or_default();
            for f in schema.fields() {
                if let Some(x) = filter {
                    let name = if tf {
                        f.metadata().get("metric").cloned().unwrap_or_default()
                    } else {
                        f.name().clone()
                    };

                    if !name.starts_with(x) {
                        continue;
                    }
                }
                println!("{}. {:#?}, {:#?}", f.name(), f.data_type(), f.metadata());
            }

            if let Some(op_file) = output_file {
                println!("Writing schema to {}", &op_file);
                let cloned = (*schema).clone();
                let json = serde_json::to_string(&cloned).unwrap();
                if let Err(e) = std::fs::write(op_file, json) {
                    println!("Error writing schema: {}", e);
                }
            }
        }
        Commands::Metadata { input_file } => {
            match input_file.ends_with(".parquet") {
                true => {
                    let x = read_parquet_metadata(input_file);
                    let m: Vec<_> = x
                        .file_metadata()
                        .key_value_metadata()
                        .unwrap()
                        .iter()
                        .filter(|x| !x.key.starts_with("ARROW"))
                        .collect();
                    println!("{:#?}", m);
                    println!("Num of row groups: {}", x.row_groups().len());
                    println!("Row group: {:#?}", x.row_group(0).num_rows());
                }
                false => {
                    let x = read_ipc_metadata(input_file);
                    println!("{:#?}", x);
                }
            };
        }
        Commands::Convert {
            input_file,
            compression,
        } => {
            let output = input_file.to_owned() + ".arrow";
            let batch = read_parquet_batch(input_file);
            write_ipc(output, *compression, &batch);
        }
        Commands::Add { input_file } => {
            let output = "/home/mihirn/Downloads/foo_meta.parquet";
            let schema = read_parquet_schema(input_file);
            let batch = read_parquet_batch(input_file);
            let metadata: HashMap<&str, &str> = [
                ("grouping_power", "3"),
                ("max_value_power", "64"),
                ("op", "read"),
                ("metric", "syscall_latency"),
                ("unit", "nanoseconds"),
                ("metric_type", "histogram"),
            ]
            .into();
            let m2: HashMap<String, String> = metadata
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            let nf = schema.field(0).clone().with_metadata(m2);
            let s = Schema::new(vec![nf]);
            dbg!("{}", &s);

            let file = File::create(output).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
                .build();

            let mut writer = ArrowWriter::try_new(file, Arc::new(s), Some(props)).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
        }
        Commands::Validate { input_file } => {
            let file = File::open(input_file).unwrap();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
            let schema = builder.schema().clone();
            let counters: BTreeSet<String> = schema
                .fields()
                .iter()
                .filter(|f| f.metadata().get("metric_type") == Some(&"delta_counter".to_string()))
                .map(|f| f.name().clone())
                .collect();

            let reader = builder.build().unwrap();
            reader.for_each(|batch| match batch {
                Ok(b) => {
                    b.columns().iter().zip(schema.fields()).for_each(|(c, f)| {
                        if counters.contains(f.name()) {
                            eprintln!("Validating {}", f.name());

                            if *c.data_type() != DataType::Float64 {
                                eprintln!("Invalid data type for {}", f.name());
                                exit(-1);
                            }

                            let values = c
                                .as_any()
                                .downcast_ref::<Float64Array>()
                                .expect("Failed to downcast");

                            for (i, v) in values.iter().enumerate() {
                                if let Some(x) = v {
                                    if x < 0.0 {
                                        eprintln!(
                                            "Negative value found in {}, row {}",
                                            f.name(),
                                            i
                                        );
                                    }
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Error reading batch: {}", &e);
                }
            });
        }
    }
}
