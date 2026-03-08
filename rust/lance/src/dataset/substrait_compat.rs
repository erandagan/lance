// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Bridge between PyArrow's substrait naming convention and DataFusion's.
//!
//! PyArrow's substrait producer uses a "shallow" convention for
//! `NamedStruct.names`: list element fields do not get name entries.
//! DataFusion expects a "deep" convention where every field — including
//! those nested inside list element types — has a name entry.
//!
//! This module normalizes the shallow convention to deep before the
//! substrait bytes are handed to DataFusion's consumer.

use arrow_schema::{DataType, Schema as ArrowSchema};
use datafusion_substrait::substrait::proto::{
    ExtendedExpression, NamedStruct, Type,
    r#type::Kind,
};
use lance_core::Result;
use prost::Message;

/// Normalize substrait expression bytes so that `NamedStruct.names` uses the
/// deep convention expected by DataFusion.
///
/// If the names are already in the deep convention this returns the input
/// unchanged.
pub fn normalize_for_datafusion(expr: &[u8], arrow_schema: &ArrowSchema) -> Result<Vec<u8>> {
    let mut envelope = ExtendedExpression::decode(expr)?;
    if let Some(base_schema) = envelope.base_schema.as_mut() {
        normalize_names(base_schema, arrow_schema);
    }
    Ok(envelope.encode_to_vec())
}

fn normalize_names(schema: &mut NamedStruct, arrow_schema: &ArrowSchema) {
    let fields = match schema.r#struct.as_ref() {
        Some(s) => &s.types,
        None => return,
    };
    let deep_count: usize = fields.iter().map(count_deep).sum();
    if schema.names.len() >= deep_count {
        return;
    }

    let mut expanded = Vec::with_capacity(deep_count);
    let mut cursor = 0;
    for (substrait_type, arrow_field) in fields.iter().zip(arrow_schema.fields()) {
        expand_names(
            substrait_type,
            arrow_field.data_type(),
            &schema.names,
            &mut cursor,
            &mut expanded,
        );
    }
    schema.names = expanded;
}

/// Count name slots a type occupies in the deep (DataFusion) convention.
fn count_deep(dtype: &Type) -> usize {
    match dtype.kind.as_ref().unwrap() {
        Kind::Struct(st) => st.types.iter().map(count_deep).sum::<usize>() + 1,
        Kind::List(lt) => count_deep(lt.r#type.as_ref().unwrap()),
        _ => 1,
    }
}

/// Walk a substrait type and the shallow names in parallel, producing deep
/// names.  Uses the Arrow type to supply names for list element fields that
/// the producer omitted.
fn expand_names(
    substrait_type: &Type,
    arrow_type: &DataType,
    source: &[String],
    cursor: &mut usize,
    out: &mut Vec<String>,
) {
    match substrait_type.kind.as_ref().unwrap() {
        Kind::Struct(st) => {
            out.push(source[*cursor].clone());
            *cursor += 1;
            if let DataType::Struct(arrow_fields) = arrow_type {
                for (child, af) in st.types.iter().zip(arrow_fields.iter()) {
                    expand_names(child, af.data_type(), source, cursor, out);
                }
            }
        }
        Kind::List(lt) => {
            // Shallow: list consumes 1 name.  Deep: list is transparent,
            // element type names start at this position.
            let list_name = source[*cursor].clone();
            *cursor += 1;
            match arrow_type {
                DataType::List(inner) | DataType::LargeList(inner) => {
                    fill_element_names(
                        lt.r#type.as_ref().unwrap(),
                        inner.data_type(),
                        &list_name,
                        out,
                    );
                }
                _ => out.push(list_name),
            }
        }
        _ => {
            out.push(source[*cursor].clone());
            *cursor += 1;
        }
    }
}

/// Generate deep names for a list element type.  `inherited_name` is the list
/// column's name, which becomes the element type's first name entry.
fn fill_element_names(
    substrait_type: &Type,
    arrow_type: &DataType,
    inherited_name: &str,
    out: &mut Vec<String>,
) {
    match substrait_type.kind.as_ref().unwrap() {
        Kind::Struct(st) => {
            out.push(inherited_name.to_string());
            if let DataType::Struct(arrow_fields) = arrow_type {
                for (child, af) in st.types.iter().zip(arrow_fields.iter()) {
                    fill_element_names(child, af.data_type(), af.name(), out);
                }
            }
        }
        Kind::List(lt) => {
            if let DataType::List(inner) | DataType::LargeList(inner) = arrow_type {
                fill_element_names(
                    lt.r#type.as_ref().unwrap(),
                    inner.data_type(),
                    inherited_name,
                    out,
                );
            } else {
                out.push(inherited_name.to_string());
            }
        }
        _ => {
            out.push(inherited_name.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::Field;
    use datafusion::{
        execution::SessionState,
        logical_expr::{BinaryExpr, Operator},
        prelude::{Expr, SessionContext},
    };
    use datafusion::common::{Column, ScalarValue};
    use lance_datafusion::substrait::{encode_substrait, parse_substrait};
    use std::sync::Arc;

    fn session_state() -> SessionState {
        SessionContext::new().state()
    }

    fn id_filter(value: &str) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column::new_unqualified("id"))),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(
                ScalarValue::Utf8(Some(value.to_string())),
                None,
            )),
        })
    }

    fn list_of_struct(name: &str, fields: Vec<Field>) -> Field {
        Field::new(
            name,
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(fields.into()),
                true,
            ))),
            true,
        )
    }

    /// Convert deep-convention names to shallow (simulating PyArrow output).
    fn to_shallow_names(schema: &NamedStruct) -> Vec<String> {
        let fields = schema.r#struct.as_ref().unwrap();
        let mut result = Vec::new();
        let mut cursor = 0;
        for dtype in &fields.types {
            consume_shallow(dtype, &schema.names, &mut cursor, &mut result);
        }
        result
    }

    fn consume_shallow(dtype: &Type, names: &[String], cursor: &mut usize, out: &mut Vec<String>) {
        match dtype.kind.as_ref().unwrap() {
            Kind::Struct(st) => {
                out.push(names[*cursor].clone());
                *cursor += 1;
                for child in &st.types {
                    consume_shallow(child, names, cursor, out);
                }
            }
            Kind::List(_) => {
                out.push(names[*cursor].clone());
                let deep = count_deep(dtype);
                *cursor += deep;
            }
            _ => {
                out.push(names[*cursor].clone());
                *cursor += 1;
            }
        }
    }

    /// Encode with DataFusion (deep names), convert to shallow, normalize, parse.
    async fn assert_shallow_roundtrip(schema: ArrowSchema, expr: Expr) {
        let schema = Arc::new(schema);
        let bytes = encode_substrait(expr.clone(), schema.clone(), &session_state()).unwrap();
        let mut envelope = ExtendedExpression::decode(bytes.as_slice()).unwrap();

        let base = envelope.base_schema.as_mut().unwrap();
        base.names = to_shallow_names(base);
        let shallow_bytes = envelope.encode_to_vec();

        let normalized = normalize_for_datafusion(&shallow_bytes, &schema).unwrap();
        let decoded = parse_substrait(&normalized, schema, &session_state())
            .await
            .unwrap();
        assert_eq!(decoded, expr);
    }

    #[tokio::test]
    async fn test_shallow_names_list_of_struct() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            list_of_struct(
                "items",
                vec![
                    Field::new("value", DataType::Float32, true),
                    Field::new("label", DataType::Utf8, true),
                ],
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_shallow_roundtrip(schema, id_filter("test-id")).await;
    }

    #[tokio::test]
    async fn test_shallow_names_many_nested() {
        let schema = ArrowSchema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "location",
                DataType::Struct(
                    vec![
                        Field::new("city", DataType::Utf8, true),
                        Field::new("country", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
            list_of_struct(
                "top_previous_companies",
                vec![
                    Field::new("company_id", DataType::Int64, true),
                    Field::new("company_name", DataType::Utf8, true),
                ],
            ),
            list_of_struct(
                "employees_by_month",
                vec![
                    Field::new("date", DataType::Utf8, true),
                    Field::new(
                        "breakdown",
                        DataType::Struct(
                            vec![
                                Field::new("count_owner", DataType::Int64, true),
                                Field::new("count_founder", DataType::Int64, true),
                            ]
                            .into(),
                        ),
                        true,
                    ),
                ],
            ),
            Field::new("name", DataType::Utf8, true),
        ]);

        assert_shallow_roundtrip(schema, id_filter("test-id")).await;
    }
}
