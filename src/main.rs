#![allow(clippy::needless_return)]

use arrow_array::{Float64Array, Int64Array, PrimitiveArray, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use clap::{Parser, Subcommand};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

#[derive(Subcommand)]
enum Commands {
    /// Schema and metadata for the parquet file
    Schema {
        #[arg(short, long)]
        input: String,

        #[arg(short, long)]
        filter: Option<String>,

        #[arg(short, long)]
        output: Option<String>,
    },
    /// Row group information: number and size of row groups
    RowGroupInfo {
        #[arg(short, long)]
        input: String,
    },
    /// Compare parquet schemas
    CompareSchema {
        #[arg(short, long)]
        left: String,

        #[arg(short, long)]
        right: String,
    },
    /// Compare parquet data
    Compare {
        #[arg(short, long)]
        left: String,

        #[arg(short, long)]
        right: String,
    },
    /// Validate delta counters in a postprocessd parquet file
    Validate {
        #[arg(short, long)]
        input: String,
    },
    /// Augment file with additional metadata around cgroups
    Cgroup {
        #[arg(short, long)]
        input: String,

        #[arg(short, long)]
        metadata: String,

        #[arg(short, long)]
        output: String,

        #[arg(long)]
        tag_1: Option<String>,

        #[arg(long)]
        tag_2: Option<String>,
    },
}

#[derive(Parser)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

fn read_parquet_footer(
    input: impl AsRef<Path>,
) -> Result<(Arc<ParquetMetaData>, SchemaRef, ParquetRecordBatchReader), Box<dyn Error>> {
    let file = File::open(input)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let reader = builder.build()?;
    return Ok((metadata, schema, reader));
}

fn compare_fields(x: &Field, y: &Field) {
    if x.data_type() != y.data_type() {
        eprintln!(
            "Differing data type for {}: {} vs {}",
            x.name(),
            x.data_type(),
            y.data_type()
        );
    }

    // Compare metadata
    let (meta1, meta2) = (x.metadata(), y.metadata());

    // Compare left to right
    meta1.iter().for_each(|(k, v)| {
        if meta2.get(k) != Some(v) {
            let v2 = if let Some(s) = meta2.get(k) {
                s
            } else {
                &"None".to_string()
            };
            eprintln!("Mismatch in {}; key {k}: {v} vs {v2}", x.name());
        }
    });

    // Compare right to left: only for missing keys
    meta2.iter().for_each(|(k, v)| {
        if !meta1.contains_key(k) {
            eprintln!("Mismatch in {}; key {k}: None vs {v}", x.name());
        }
    });
}

fn compare_column<T: arrow_array::ArrowPrimitiveType>(
    column: &str,
    v1: &PrimitiveArray<T>,
    v2: &PrimitiveArray<T>,
) where
    <T as arrow_array::ArrowPrimitiveType>::Native: std::fmt::Display,
{
    v1.into_iter().zip(v2).enumerate().for_each(|(i, (x, y))| {
        if x != y {
            let x_str = x
                .map(|_| v1.value(i).to_string())
                .unwrap_or("None".to_string());
            let y_str = y
                .map(|_| v2.value(i).to_string())
                .unwrap_or("None".to_string());
            eprintln!(
                "Values mismatch for {column}; row {}: {} vs {}",
                i, x_str, y_str
            );
        }
    });
}

fn run(cmd: Commands) -> Result<(), Box<dyn Error>> {
    match cmd {
        Commands::Schema {
            input,
            filter,
            output,
        } => {
            let (_, schema, _) = read_parquet_footer(&input)?;

            // If a filter is specified, select only those fields whose
            // name, as specified by the metric key of the metadata, matches
            // the filter pattern. This will only work with Rezolus v5
            // metrics, since the column name is no longer consider meaningful.
            let json = if let Some(pat) = filter {
                let fields: Vec<&Arc<Field>> = schema
                    .fields()
                    .iter()
                    .filter(|f| {
                        f.metadata()
                            .get("metric")
                            .cloned()
                            .unwrap_or_default()
                            .starts_with(&pat)
                    })
                    .collect();
                serde_json::to_string_pretty(&fields)
            } else {
                serde_json::to_string_pretty(&schema)
            }?;
            println!("{json}");

            if let Some(op) = output {
                let json = serde_json::to_string(&schema)?;
                let res = std::fs::write(&op, json);
                if let Err(e) = res {
                    eprintln!("Error writing schema: {e}");
                } else {
                    println!("Schema written to {}", &op);
                }
            }
        }
        Commands::RowGroupInfo { input } => {
            let (metadata, _, _) = read_parquet_footer(&input)?;
            println!("Num of row groups: {}", metadata.row_groups().len());
            println!(
                "Rows in first group: {:#?}",
                metadata.row_group(0).num_rows()
            );
        }
        Commands::CompareSchema { left, right } => {
            let (_, s1, _) = read_parquet_footer(&left)?;
            let (_, s2, _) = read_parquet_footer(&right)?;

            if s1.fields().len() != s2.fields().len() {
                eprintln!(
                    "Differing field counts: {} vs {}",
                    s1.fields().len(),
                    s2.fields().len()
                );
            }

            // Compare fields from left with right
            for f1 in s1.fields() {
                let Ok(f2) = s2.field_with_name(f1.name()) else {
                    eprintln!("Field {} missing in {right}", f1.name());
                    continue;
                };

                compare_fields(f1, f2);
            }

            // Look for any leftover uncompared fields from the right
            for f2 in s2.fields() {
                if s1.field_with_name(f2.name()).is_ok() {
                    continue;
                }

                eprintln!("Field {} missing in {left}", f2.name());
            }
        }
        Commands::Compare { left, right } => {
            let (_, s1, r1) = read_parquet_footer(&left)?;
            let (_, s2, mut r2) = read_parquet_footer(&right)?;

            // Iterate across record batch from left
            for x in r1 {
                let x = x?;
                let Some(y) = r2.next() else {
                    let e = format!("{} iterator completed earlier", right);
                    return Err(e.into());
                };
                let y = y?;

                // Map every column from left to the corresponding column on right
                for (idx, l) in x.columns().iter().enumerate() {
                    let column = s1.field(idx).name();
                    let Some(r) = y.column_by_name(column) else {
                        eprintln!("{column} missing in {right}");
                        continue;
                    };

                    let (d1, d2) = (l.data_type(), r.data_type());
                    if d1 != d2 {
                        let e = format!("Data type mismatch for {}: {} vs {}", column, d1, d2);
                        return Err(e.into());
                    }

                    if *d1 == DataType::Float64 {
                        let v1 = l
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or("not a valid float64 column")?;

                        let v2 = r
                            .as_any()
                            .downcast_ref::<Float64Array>()
                            .ok_or("not a valid float64 column")?;

                        compare_column(column, v1, v2);
                    } else if *d1 == DataType::UInt64 {
                        let v1 = l
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or("not a valid float64 column")?;

                        let v2 = r
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .ok_or("not a valid float64 column")?;

                        compare_column(column, v1, v2);
                    } else if *d1 == DataType::Int64 {
                        let v1 = l
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or("not a valid float64 column")?;

                        let v2 = r
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or("not a valid float64 column")?;

                        compare_column(column, v1, v2);
                    } else {
                        eprintln!("Skipping histogram column: {}", column);
                    }
                }

                // Display columns only in the right
                for (idx, _) in y.columns().iter().enumerate() {
                    let column = s2.field(idx).name();
                    if x.column_by_name(column).is_none() {
                        eprintln!("{column} missing in {left}");
                    }
                }
            }

            // Fill in data in case right has more record batches
            if r2.next().is_some() {
                let e = format!("{} iterator completed earlier", left);
                return Err(e.into());
            }
        }
        Commands::Validate { input } => {
            let (_, schema, reader) = read_parquet_footer(&input)?;

            for batch in reader {
                let batch = batch?;
                for (idx, col) in batch.columns().iter().enumerate() {
                    let f = schema.field(idx);
                    let n = f.name();

                    if f.metadata().get("metric_type") != Some(&"delta_counter".to_string()) {
                        continue;
                    }

                    if *f.data_type() != DataType::Float64 {
                        let e = format!("Invalid data type for {}: {}", n, f.data_type());
                        return Err(e.into());
                    }

                    let values = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or("failed to downcast to float64 array")?;

                    for (i, v) in values.iter().enumerate() {
                        if let Some(x) = v {
                            if x < 0.0 {
                                eprintln!("Negative value found in {}, row {}", n, i);
                            }
                        }
                    }
                }
            }
        }
        Commands::Cgroup {
            input,
            metadata,
            output,
            tag_1,
            tag_2,
        } => {
            #[derive(Debug, Serialize, Deserialize)]
            struct Record {
                cgroup: String,
                tag_1: String,
                tag_2: String,
            }

            let mut mappings: Vec<Record> = Vec::new();
            let mut names = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_path(metadata)?;
            for record in names.deserialize() {
                let data: Record = record.unwrap();
                mappings.push(data);
            }

            let t1 = tag_1.unwrap_or("tag_1".to_string());
            let t2 = tag_2.unwrap_or("tag_2".to_string());

            let (metadata, schema, reader) = read_parquet_footer(&input)?;

            // Merge field metadata from schema and mappings file
            let mut fields: Vec<Field> = Vec::new();
            for field in schema.fields() {
                let mut f = (*(*field).clone()).clone();
                let meta = field.metadata();

                // Add to the metadata is the metric is of type cgroup
                // and the name contains part of the cgroup name in the
                // external mappings file. This assumes that there will
                // only be a single match for each cgroup.
                if let Some(metric) = meta.get("metric") {
                    if metric.starts_with("cgroup") {
                        if let Some(name) = meta.get("name") {
                            for r in &mappings {
                                if name.contains(&r.cgroup) {
                                    let mut nmeta = meta.clone();
                                    nmeta.insert(t1.clone(), r.tag_1.clone());
                                    nmeta.insert(t2.clone(), r.tag_2.clone());
                                    f = f.with_metadata(nmeta);
                                    break;
                                }
                            }
                        }
                    }
                }

                fields.push(f);
            }
            let nschema = Arc::new(Schema::new(fields));

            let props = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(8)?))
                .set_key_value_metadata(metadata.file_metadata().key_value_metadata().cloned())
                .set_max_row_group_size(metadata.row_group(0).num_rows() as usize)
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&output)?, nschema.clone(), Some(props))?;
            for batch in reader {
                let merged_batch =
                    RecordBatch::try_new(nschema.clone(), batch?.columns().to_vec())?;
                writer.write(&merged_batch)?;
            }
            writer.finish()?;
        }
    }

    return Ok(());
}

fn main() {
    let args = CliArgs::parse();

    if let Err(e) = run(args.command) {
        eprintln!("Error: {e}");
    }
}
