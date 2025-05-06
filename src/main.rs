#![allow(clippy::needless_return)]

use arrow_array::{
    Array, Float64Array, GenericListArray, Int64Array, ListArray, PrimitiveArray, RecordBatch,
    StringArray, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use clap::{Parser, Subcommand};
use core::fmt;
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::basic::ZstdLevel;
use parquet::file::metadata::KeyValue;
use parquet::file::{metadata::ParquetMetaData, properties::WriterProperties};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::error::Error;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

struct MetricsMap {
    pub(crate) data: BTreeMap<String, BTreeMap<String, BTreeSet<String>>>,
}

impl MetricsMap {
    pub fn new() -> Self {
        return Self {
            data: BTreeMap::new(),
        };
    }
}

impl fmt::Display for MetricsMap {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "METRICS ({})\n----------\n", self.data.len())?;
        for (metric, metadata) in &self.data {
            write!(f, "{}\n", metric)?;
            for (k, v) in metadata {
                let x: String = v
                    .into_iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(" | ");
                write!(f, "\t{}: {}\n", k, x)?;
            }
        }

        return Ok(());
    }
}

const IGNORED_METRICS_METADATA: &[&str] = &[
    "metric",
    "metric_type",
    "name",
    "id",
    "grouping_power",
    "max_value_power",
    "unit",
];

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
    AddCgroupMetadata {
        #[arg(short, long)]
        input: String,

        #[arg(short, long)]
        metadata: String,

        #[arg(short, long)]
        output: String,
    },
    /// Convert an event-log CSV to a parquet file
    FromEventCSV {
        #[arg(short, long)]
        input: String,

        #[arg(short, long)]
        config: String,

        #[arg(short, long)]
        output: String,
    },
    /// List of cgroups and all cgroup-associated metrics in the file
    CgroupsInfo {
        #[arg(short, long)]
        input: String,

        #[arg(short, long, default_value_t = false)]
        numeric_only: bool,
    },
}

#[derive(Parser)]
struct CliArgs {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Deserialize, Serialize)]
struct FromEventCSVConfig {
    ts: String,
    discriminator: Vec<String>,
    metrics: Vec<String>,
    filter: Option<HashMap<String, String>>,
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
    x: Option<&PrimitiveArray<T>>,
    y: Option<&PrimitiveArray<T>>,
    row_idx: Option<usize>,
) -> Result<(), Box<dyn Error>>
where
    <T as arrow_array::ArrowPrimitiveType>::Native: std::fmt::Display,
{
    let v1 = x.ok_or("invalid downcast for column")?;
    let v2 = y.ok_or("invalid downcast for column")?;

    v1.into_iter().zip(v2).enumerate().for_each(|(i, (a, b))| {
        if a != b {
            let row = row_idx.unwrap_or(i);
            let x_str = x
                .map(|_| v1.value(i).to_string())
                .unwrap_or("None".to_string());
            let y_str = y
                .map(|_| v2.value(i).to_string())
                .unwrap_or("None".to_string());
            eprintln!(
                "Values mismatch for {column}; row {}: {} vs {}",
                row, x_str, y_str
            );
        }
    });

    return Ok(());
}

fn compare_listarray_column(
    column: &str,
    v1: &GenericListArray<i32>,
    v2: &GenericListArray<i32>,
    data_type: &DataType,
) -> Result<(), Box<dyn Error>> {
    for (idx, (x, y)) in v1.iter().zip(v2.iter()).enumerate() {
        if (x.is_none() && y.is_some()) || (x.is_some() && y.is_none()) {
            eprintln!("Values mismatch for {column}; row {idx}, only one is None");
            continue;
        }

        if let (Some(a), Some(b)) = (x, y) {
            match data_type {
                DataType::UInt32 => {
                    let l = a.as_any().downcast_ref::<UInt32Array>();
                    let r = b.as_any().downcast_ref::<UInt32Array>();
                    compare_column(column, l, r, Some(idx))?;
                }
                DataType::UInt64 => {
                    let l = a.as_any().downcast_ref::<UInt64Array>();
                    let r = b.as_any().downcast_ref::<UInt64Array>();
                    compare_column(column, l, r, Some(idx))?;
                }
                _ => {
                    eprintln!("Unexpected inner data type for {column}: {data_type}");
                }
            };
        }
    }

    return Ok(());
}

fn get_cgroups(schema: &SchemaRef) -> (Vec<String>, MetricsMap) {
    let mut cgroups: BTreeSet<String> = BTreeSet::new();
    let mut metrics: MetricsMap = MetricsMap::new();

    let ignored_metadata: BTreeSet<String> = IGNORED_METRICS_METADATA
        .iter()
        .map(|s| s.to_string())
        .collect();

    schema.fields().iter().for_each(|f| {
        let metadata = f.metadata();
        if let Some(metric) = metadata.get("metric") {
            if metric.starts_with("cgroup") {
                if let Some(name) = metadata.get("name") {
                    cgroups.insert(name.to_string());
                }

                let e = metrics.data.entry(metric.to_string()).or_default();
                for (k, v) in metadata {
                    if ignored_metadata.contains(k) {
                        continue;
                    }
                    e.entry(k.to_string()).or_default().insert(v.to_string());
                }
            }
        }
    });

    return (cgroups.into_iter().collect(), metrics);
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

                    let (l_any, r_any) = (l.as_any(), r.as_any());
                    match d1 {
                        DataType::UInt32 => {
                            let v1 = l_any.downcast_ref::<UInt32Array>();
                            let v2 = r_any.downcast_ref::<UInt32Array>();
                            compare_column(column, v1, v2, None)?;
                        }
                        DataType::UInt64 => {
                            let v1 = l_any.downcast_ref::<UInt64Array>();
                            let v2 = r_any.downcast_ref::<UInt64Array>();
                            compare_column(column, v1, v2, None)?;
                        }
                        DataType::Int64 => {
                            let v1 = l_any.downcast_ref::<Int64Array>();
                            let v2 = r_any.downcast_ref::<Int64Array>();
                            compare_column(column, v1, v2, None)?;
                        }
                        DataType::Float64 => {
                            let v1 = l_any.downcast_ref::<Float64Array>();
                            let v2 = r_any.downcast_ref::<Float64Array>();
                            compare_column(column, v1, v2, None)?;
                        }
                        DataType::List(field_type) => {
                            let v1 = l_any
                                .downcast_ref::<ListArray>()
                                .ok_or("not a list array")?;
                            let v2 = r_any
                                .downcast_ref::<ListArray>()
                                .ok_or("not a list array")?;
                            compare_listarray_column(column, v1, v2, field_type.data_type())?;
                        }
                        _ => {
                            eprintln!("Unexpected type for {column}: {}", d1);
                        }
                    };
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
        Commands::AddCgroupMetadata {
            input,
            metadata,
            output,
        } => {
            type Record = HashMap<String, String>;

            let mut mappings: Vec<Record> = Vec::new();
            let mut names = csv::ReaderBuilder::new().from_path(metadata)?;
            for record in names.deserialize() {
                let data: Record = record?;
                mappings.push(data);
            }

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
                                if let Some(cgroup) = r.get("cgroup") {
                                    if name.contains(cgroup) {
                                        let mut nmeta = meta.clone();

                                        for (k, v) in r {
                                            if k == "cgroup" {
                                                continue;
                                            }
                                            nmeta.insert(k.to_string(), v.to_string());
                                        }

                                        f = f.with_metadata(nmeta);
                                        break;
                                    }
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
        Commands::FromEventCSV {
            input,
            config,
            output,
        } => {
            type Record = HashMap<String, String>;

            let data = std::fs::read_to_string(&config)?;
            let toml: FromEventCSVConfig = toml::from_str(&data)?;

            // Create in-memory vectors to store the event data as columns.
            // This assumes that all the data can be stored in memory and the
            // file does not have to be streamed to a parquet.
            let mut tses = Vec::new();
            let mut ts_nses = Vec::new();
            let mut discriminators: BTreeMap<String, Vec<Option<String>>> = BTreeMap::new();
            let mut metrics: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();

            for d in &toml.discriminator {
                discriminators.insert(d.to_string(), Vec::new());
            }

            for m in &toml.metrics {
                metrics.insert(m.to_string(), Vec::new());
            }

            let mut event_log = csv::ReaderBuilder::new().from_path(input)?;
            for record in event_log.deserialize() {
                let data: Record = record?;

                // If a filtering criteria exists for the log events, apply it
                if let Some(filter) = &toml.filter {
                    let mut skip: bool = true;
                    for (k, v) in filter {
                        if data.get(k) == Some(v) {
                            skip = false;
                            break;
                        }
                    }
                    if skip {
                        continue;
                    }
                }

                // Extract and store the timestamp for the current event
                let Some(str_ts) = data.get(&toml.ts) else {
                    return Err("timestamp missing".into());
                };
                let ts = chrono::DateTime::parse_from_rfc3339(str_ts)?;
                let ts_ns = ts
                    .timestamp_nanos_opt()
                    .ok_or("timestamp cannot be converted to nanoseconds")?
                    as u64;
                tses.push(ts.to_string());
                ts_nses.push(ts_ns);

                // Get all discriminator fields for the current event. If
                // a field is missing, None is stored.
                for d in &toml.discriminator {
                    let Some(e) = discriminators.get_mut(d) else {
                        return Err(format!("{} field missing", d).into());
                    };
                    e.push(data.get(d).cloned());
                }

                // Get all the configured metrics for the current event. If
                // a field is missing, None is stored.
                for m in &toml.metrics {
                    let Some(e) = metrics.get_mut(m) else {
                        return Err(format!("{} field missing", m).into());
                    };
                    let v: Option<f64> = if let Some(x) = data.get(m) {
                        Some(x.parse()?)
                    } else {
                        None
                    };
                    e.push(v);
                }
            }

            // Build schema from timestamps, discriminators, and metrics
            let mut fields: Vec<Field> =
                Vec::with_capacity(2 + discriminators.len() + metrics.len());
            let ts_name = "timestamp_rfc3339";
            let mut columns = vec![
                (ts_name.to_string(), DataType::Utf8, false, ts_name),
                (
                    "timestamp".to_string(),
                    DataType::UInt64,
                    false,
                    "timestamp",
                ),
            ];
            let mut data: Vec<Arc<dyn Array>> = vec![
                Arc::new(StringArray::from(tses)),
                Arc::new(UInt64Array::from(ts_nses)),
            ];
            for (k, v) in discriminators.into_iter() {
                columns.push((k, DataType::Utf8, true, "discriminator"));
                data.push(Arc::new(StringArray::from(v)));
            }
            for (k, v) in metrics.into_iter() {
                columns.push((k, DataType::Float64, true, "gauge"));
                data.push(Arc::new(Float64Array::from(v)));
            }

            for (name, dt, nullable, metric_type) in columns {
                fields.push(
                    Field::new(name.to_string(), dt, nullable).with_metadata(
                        [
                            ("metric".to_string(), name.to_string()),
                            ("metric_type".to_string(), metric_type.to_string()),
                        ]
                        .into(),
                    ),
                );
            }
            let schema = Arc::new(Schema::new(fields));

            // Store discriminators as file metadata and write out the parquet
            let metadata: Vec<KeyValue> = vec![KeyValue {
                key: "discriminator".to_string(),
                value: Some(toml.discriminator.join(", ")),
            }];
            let props = WriterProperties::builder()
                .set_compression(parquet::basic::Compression::ZSTD(ZstdLevel::try_new(8)?))
                .set_key_value_metadata(Some(metadata))
                .build();
            let mut writer =
                ArrowWriter::try_new(File::create_new(&output)?, schema.clone(), Some(props))?;
            let batch = RecordBatch::try_new(schema, data)?;
            writer.write(&batch)?;
            writer.finish()?;
        }
        Commands::CgroupsInfo {
            input,
            numeric_only,
        } => {
            let (_, schema, _) = read_parquet_footer(&input)?;
            let (cgroups, metrics) = get_cgroups(&schema);
            if numeric_only {
                eprintln!("Number of cgroups: {}", cgroups.len());
                eprintln!("Number of metrics: {}", metrics.data.len());
            } else {
                eprintln!("CGROUPS ({})\n----------", cgroups.len());
                cgroups.iter().for_each(|s| eprintln!("{s}"));
                eprintln!("\n{}", &metrics);
            }
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
