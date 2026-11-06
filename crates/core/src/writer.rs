use std::collections::BTreeMap;

use arrow::array::{RecordBatch, UInt32Array};
use arrow::compute::{CastOptions, cast_with_options, concat_batches, take_record_batch};
use arrow::datatypes::SchemaRef;
use futures::future::try_join_all;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::arrow_schema::arrow_schema;
use crate::error::{Result, invalid};
use crate::manifest::{CONTENT_DATA, DataFile};
use crate::partition::PartitionSpec;
use crate::table::Table;
use crate::types::{Field, Schema, Type};
use crate::values::Datum;

const MAX_PARTITION_DIR_CHARS: usize = 128;

// ponytail: buffers the whole append in memory; stream with a rolling writer per partition if
// inserts outgrow RAM.
pub(crate) async fn write_data_files(
    table: &Table,
    batches: &[RecordBatch],
) -> Result<Vec<DataFile>> {
    let schema = table.schema();
    let spec = table.metadata().default_spec();
    let arrow = arrow_schema(&schema.fields);
    let conformed = batches
        .iter()
        .map(|b| conform(b, &arrow))
        .collect::<Result<Vec<_>>>()?;
    let all = concat_batches(&arrow, &conformed)?;
    if all.num_rows() == 0 {
        return Ok(vec![]);
    }

    let groups = group_by_partition(&all, schema, spec)?;
    let writes = groups.into_values().map(|(partition, rows)| {
        let batch = take_record_batch(&all, &UInt32Array::from(rows));
        async move { write_file(table, schema, spec, batch?, partition).await }
    });
    try_join_all(writes).await
}

/// Matches columns by name. A failed cast is an error, because the default of writing null
/// would lose the value without anyone noticing.
fn conform(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != schema.fields().len() {
        return invalid(format!(
            "expected {} columns, got {}",
            schema.fields().len(),
            batch.num_columns()
        ));
    }
    let options = CastOptions {
        safe: false,
        ..Default::default()
    };
    let columns = schema
        .fields()
        .iter()
        .map(|f| match batch.column_by_name(f.name()) {
            Some(c) => Ok(cast_with_options(c, f.data_type(), &options)?),
            None => invalid(format!("missing column {:?}", f.name())),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}

type Partition = Vec<Option<Datum>>;
type PartitionGroups = BTreeMap<Vec<u8>, (Partition, Vec<u32>)>;

fn group_by_partition(
    batch: &RecordBatch,
    schema: &Schema,
    spec: &PartitionSpec,
) -> Result<PartitionGroups> {
    let sources: Vec<(usize, Type)> = spec
        .fields
        .iter()
        .map(|p| {
            let index = schema.fields.iter().position(|f| f.id == p.source_id);
            let index = index.expect("spec checked against schema");
            (index, schema.fields[index].ty)
        })
        .collect();

    let mut groups = PartitionGroups::new();
    for row in 0..batch.num_rows() {
        let mut partition = Vec::with_capacity(spec.fields.len());
        let mut key = Vec::new();
        for (p, (column, ty)) in spec.fields.iter().zip(&sources) {
            let source = Datum::from_array(batch.column(*column), row, *ty);
            let value = match source {
                Some(v) => p.transform.apply(&v)?,
                None => None,
            };
            match &value {
                Some(v) => {
                    let bytes = v.to_bytes();
                    key.push(1);
                    key.extend((bytes.len() as u64).to_be_bytes());
                    key.extend(bytes);
                }
                None => key.push(0),
            }
            partition.push(value);
        }
        groups
            .entry(key)
            .or_insert_with(|| (partition, vec![]))
            .1
            .push(row as u32);
    }
    Ok(groups)
}

async fn write_file(
    table: &Table,
    schema: &Schema,
    spec: &PartitionSpec,
    batch: RecordBatch,
    partition: Partition,
) -> Result<DataFile> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, batch.schema(), Some(properties))?;
    writer.write(&batch)?;
    writer.close()?;

    let path = format!(
        "{}/data/{}{}.parquet",
        table.metadata().location,
        partition_dir(spec, &partition),
        uuid::Uuid::new_v4()
    );
    let file_size_in_bytes = buffer.len() as i64;
    table.catalog().write(&path, buffer).await?;

    let mut file = DataFile {
        content: CONTENT_DATA,
        file_path: path,
        file_format: "PARQUET".to_owned(),
        partition,
        record_count: batch.num_rows() as i64,
        file_size_in_bytes,
        value_counts: BTreeMap::new(),
        null_value_counts: BTreeMap::new(),
        nan_value_counts: BTreeMap::new(),
        lower_bounds: BTreeMap::new(),
        upper_bounds: BTreeMap::new(),
        first_row_id: None,
    };
    for (i, f) in schema.fields.iter().enumerate() {
        collect_metrics(&mut file, f, &batch, i);
    }
    Ok(file)
}

/// Directory names are cosmetic since readers go through manifests, so anything outside a
/// safe set is replaced instead of escaped, and long values are cut to fit key length limits.
fn partition_dir(spec: &PartitionSpec, partition: &[Option<Datum>]) -> String {
    spec.fields
        .iter()
        .zip(partition)
        .map(|(p, v)| {
            let value = v.as_ref().map_or("null".to_owned(), Datum::to_string);
            let safe: String = format!("{}={value}", p.name)
                .chars()
                .take(MAX_PARTITION_DIR_CHARS)
                .map(|c| {
                    if c.is_ascii_alphanumeric() || "-_.=".contains(c) {
                        c
                    } else {
                        '_'
                    }
                })
                .collect();
            safe + "/"
        })
        .collect()
}

// ponytail: walks every value as a Datum; read Parquet column statistics if writes get slow.
fn collect_metrics(file: &mut DataFile, field: &Field, batch: &RecordBatch, column: usize) {
    let (mut nulls, mut nans) = (0, 0);
    let mut bounds: Option<(Datum, Datum)> = None;
    for row in 0..batch.num_rows() {
        let Some(v) = Datum::from_array(batch.column(column), row, field.ty) else {
            nulls += 1;
            continue;
        };
        if matches!(v, Datum::Float(x) if x.is_nan()) || matches!(v, Datum::Double(x) if x.is_nan())
        {
            nans += 1;
            continue;
        }
        bounds = Some(match bounds {
            None => (v.clone(), v),
            Some((lo, hi)) => {
                let lo = if v.compare(&lo).is_some_and(|o| o.is_lt()) {
                    v.clone()
                } else {
                    lo
                };
                let hi = if v.compare(&hi).is_some_and(|o| o.is_gt()) {
                    v
                } else {
                    hi
                };
                (lo, hi)
            }
        });
    }
    file.value_counts.insert(field.id, batch.num_rows() as i64);
    file.null_value_counts.insert(field.id, nulls);
    if matches!(field.ty, Type::Float | Type::Double) {
        file.nan_value_counts.insert(field.id, nans);
    }
    if let Some((lo, hi)) = bounds {
        file.lower_bounds.insert(field.id, lo.to_bytes());
        file.upper_bounds.insert(field.id, hi.to_bytes());
    }
}
