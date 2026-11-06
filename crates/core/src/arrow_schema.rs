use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

use crate::error::{Error, Result};
use crate::types::{Field, Schema, Type};

pub fn arrow_type(ty: Type) -> DataType {
    match ty {
        Type::Boolean => DataType::Boolean,
        Type::Int => DataType::Int32,
        Type::Long => DataType::Int64,
        Type::Float => DataType::Float32,
        Type::Double => DataType::Float64,
        Type::Date => DataType::Date32,
        Type::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        Type::Timestamptz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        Type::String => DataType::Utf8,
        Type::Binary => DataType::Binary,
    }
}

/// `PARQUET:field_id` metadata makes `ArrowWriter` record Iceberg field ids in the file.
pub fn arrow_field(f: &Field) -> ArrowField {
    ArrowField::new(&f.name, arrow_type(f.ty), !f.required).with_metadata(HashMap::from([(
        PARQUET_FIELD_ID_META_KEY.to_owned(),
        f.id.to_string(),
    )]))
}

pub fn arrow_schema(fields: &[Field]) -> SchemaRef {
    Arc::new(ArrowSchema::new(
        fields.iter().map(arrow_field).collect::<Vec<_>>(),
    ))
}

/// Builds a new table schema, assigning field ids from 1 in column order.
pub fn schema_from_arrow(schema: &ArrowSchema) -> Result<Schema> {
    let fields = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let ty = match f.data_type() {
                DataType::Boolean => Type::Boolean,
                DataType::Int32 => Type::Int,
                DataType::Int64 => Type::Long,
                DataType::Float32 => Type::Float,
                DataType::Float64 => Type::Double,
                DataType::Date32 => Type::Date,
                DataType::Timestamp(_, None) => Type::Timestamp,
                DataType::Timestamp(_, Some(_)) => Type::Timestamptz,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Type::String,
                DataType::Binary | DataType::LargeBinary | DataType::BinaryView => Type::Binary,
                other => return Err(Error::Unsupported(format!("arrow type {other}"))),
            };
            Ok(Field {
                id: i as i32 + 1,
                name: f.name().clone(),
                required: !f.is_nullable(),
                ty,
                doc: None,
                extra: Default::default(),
            })
        })
        .collect::<Result<_>>()?;
    Schema::new(fields)
}
