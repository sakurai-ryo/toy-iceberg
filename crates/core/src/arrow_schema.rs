//! Iceberg のスキーマと Arrow のスキーマの変換。
//!
//! Arrow はメモリ上の表現で、Parquet の読み書きと DataFusion との受け渡しに使う。
//! Iceberg の日時はマイクロ秒精度なので、`timestamp` は Arrow でも `Microsecond` にそろえる。

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

/// `PARQUET:field_id` メタデータを付けると、`ArrowWriter` が Iceberg の field id をファイルに記録する。
/// 仕様の「Appendix A」の Parquet の節は、列に field id を持たせることを要求している。
/// 読み手はこの ID で列を探す（[`crate::scan::read_task`]）。
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

/// 新しいテーブルのスキーマを作る。field id は列の順に 1 から割り当てる。
/// 既存のテーブルには使えない。field id は一度割り当てたら変えられないからである。
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
