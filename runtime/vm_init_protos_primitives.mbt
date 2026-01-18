///|
fn init_primitive_protos(
  object_proto_obj : ObjectValue,
) -> (
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
) {
  let (string_proto_obj, string_proto_value) = init_string_proto(
    object_proto_obj,
  )
  let (number_proto_obj, number_proto_value) = init_number_proto(
    object_proto_obj,
  )
  let (bool_proto_obj, bool_proto_value) = init_bool_proto(object_proto_obj)
  let (bigint_proto_obj, bigint_proto_value) = init_bigint_proto(
    object_proto_obj,
  )
  let (symbol_proto_obj, symbol_proto_value) = init_symbol_proto(
    object_proto_obj,
  )
  let (regexp_proto_obj, regexp_proto_value) = init_regexp_proto(
    object_proto_obj,
  )
  let (date_proto_obj, date_proto_value) = init_date_proto(object_proto_obj)
  (
    string_proto_obj, string_proto_value, number_proto_obj, number_proto_value, bool_proto_obj,
    bool_proto_value, bigint_proto_obj, bigint_proto_value, symbol_proto_obj, symbol_proto_value,
    regexp_proto_obj, regexp_proto_value, date_proto_obj, date_proto_value,
  )
}
