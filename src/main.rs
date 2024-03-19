#![allow(dead_code)]
#![allow(clippy::needless_return)]

use arrow::array::*;
use arrow::csv::writer::Writer;
use arrow::datatypes::*;
use arrow::ipc::writer::{FileWriter, IpcWriteOptions};
use arrow::ipc::CompressionType;
use clap::Parser;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::*;
use parquet::file::properties::*;
use rand::*;
use std::fs::File;
use std::sync::Arc;

#[derive(Parser)]
struct CliArgs {
    #[arg(short, long)]
    compression: bool,

    #[arg(short, long)]
    listarray: bool,

    #[arg(short, long)]
    keepfile: bool,
}

const STRINGS: &[&str] = &["foo", "bar", "baz"];

fn build_u64_column(
    name: &str,
    data: Vec<u64>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let field = Field::new(name, DataType::UInt64, false);
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
    let field = Field::new(name, DataType::Float64, false);
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
    let field = Field::new(name, DataType::Utf8, false);
    let series = Arc::new(StringArray::from(data));
    pq_cols.push(field);
    pq_series.push(series);
}

fn build_u64_array_column(
    name: &str,
    data: Vec<Vec<u64>>,
    pq_cols: &mut Vec<Field>,
    pq_series: &mut Vec<Arc<dyn Array>>,
) {
    let mut builder = ListBuilder::new(UInt64Builder::new());
    let field = Field::new(name, DataType::new_list(DataType::UInt64, true), false);
    for inner in data {
        builder.append_value(inner.iter().map(|x| Some(*x)).collect::<Vec<_>>());
    }
    pq_cols.push(field);
    pq_series.push(Arc::new(builder.finish()));
}

fn write_parquet(
    output: String,
    listarray: bool,
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
        let batch = generate_batch(nrows, listarray);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

fn write_ipc(
    output: String,
    listarray: bool,
    compression: bool,
    schema: SchemaRef,
    nrows: u8,
    nbatches: u8,
) {
    let file = File::create(output).unwrap();

    let opts = match compression {
        false => IpcWriteOptions::default(),
        true => IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::ZSTD))
            .unwrap(),
    };

    let mut writer = FileWriter::try_new_with_options(file, &schema, opts).unwrap();
    for _ in 0..nbatches {
        let batch = generate_batch(nrows, listarray);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();
}

fn write_csv(output: String, batch: &RecordBatch) {
    let file = File::create(output).unwrap();
    let mut writer = Writer::new(file);
    writer.write(batch).unwrap();
    writer.close().unwrap();
}

fn generate_data(nrows: u8) -> (Vec<String>, Vec<u64>, Vec<f64>, Vec<Vec<u64>>) {
    let mut rng = rand::thread_rng();
    let mut strings: Vec<String> = Vec::new();
    let mut nums: Vec<u64> = Vec::new();
    let mut floats: Vec<f64> = Vec::new();
    let mut arrays: Vec<Vec<u64>> = Vec::new();

    for _ in 0..nrows {
        nums.push(rng.gen_range(0..100));
        floats.push(rng.gen_range(0.0..10.0));
        strings.push(STRINGS[rng.gen_range(0..3)].to_owned());

        let mut inner: Vec<u64> = vec![0; 3];
        for j in 0..3 {
            inner[j] = rng.gen_range(0..25);
        }
        arrays.push(inner);
    }

    return (strings, nums, floats, arrays);
}

fn generate_batch(nrows: u8, listarray: bool) -> RecordBatch {
    let mut pq_cols: Vec<Field> = Vec::new();
    let mut pq_series: Vec<Arc<dyn Array>> = Vec::new();

    let (strings, nums, floats, arrays) = generate_data(nrows);

    build_u64_column("nums", nums, &mut pq_cols, &mut pq_series);
    build_f64_column("floats", floats, &mut pq_cols, &mut pq_series);
    build_string_column("strings", strings, &mut pq_cols, &mut pq_series);
    if listarray {
        build_u64_array_column("arrays", arrays, &mut pq_cols, &mut pq_series);
    }

    let schema = Schema::new(pq_cols);
    let batch = RecordBatch::try_new(Arc::new(schema), pq_series).unwrap();

    return batch;
}

fn build_arrow(output: &str, nrows: u8, compression: bool, listarray: bool) {
    let batch = generate_batch(nrows, listarray);
    write_parquet(
        output.to_owned() + ".parquet",
        listarray,
        compression,
        batch.schema(),
        nrows,
        2,
    );

    // write_ipc(
    //     output.to_owned() + ".ipc",
    //     listarray,
    //     compression,
    //     batch.schema(),
    //     nrows,
    //     2,
    // );
    // write_csv(output.to_owned() + ".csv", &batch);
}

fn read_parquet(input: &str) {
    let file = File::open(input.to_owned() + ".parquet").unwrap();
    let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let mut reader = builder.build().unwrap();
    let record_batch = reader.next().unwrap().unwrap();
    for i in 0..record_batch.num_columns() {
        let col = &record_batch.columns()[i];
        println!("Column {}: {:#?}", i, col);
    }
}

fn main() {
    let args = CliArgs::parse();
    let name = "foobar";
    build_arrow(name, 3, args.compression, args.listarray);
    read_parquet(name);

    if !args.keepfile {
        let _ = std::fs::remove_file(name.to_owned() + ".parquet");
        // let _ = std::fs::remove_file(name.to_owned() + ".ipc");
        // let _ = std::fs::remove_file(name.to_owned() + ".csv");
    }
}
