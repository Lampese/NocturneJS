///|
fn init_error_suite(
  object_proto_obj : ObjectValue,
) -> (Value, Value, Value, Value, Value, Value, Value, Value) {
  let (
    error_proto_obj,
    error_proto_value,
    aggregate_error_proto_obj,
    aggregate_error_proto_value,
    eval_error_proto_obj,
    eval_error_proto_value,
    syntax_error_proto_obj,
    syntax_error_proto_value,
    range_error_proto_obj,
    range_error_proto_value,
    type_error_proto_obj,
    type_error_proto_value,
    reference_error_proto_obj,
    reference_error_proto_value,
    uri_error_proto_obj,
    uri_error_proto_value,
  ) = init_error_protos(object_proto_obj)
  init_error_ctors(
    error_proto_obj, error_proto_value, aggregate_error_proto_obj, aggregate_error_proto_value,
    eval_error_proto_obj, eval_error_proto_value, syntax_error_proto_obj, syntax_error_proto_value,
    range_error_proto_obj, range_error_proto_value, type_error_proto_obj, type_error_proto_value,
    reference_error_proto_obj, reference_error_proto_value, uri_error_proto_obj,
    uri_error_proto_value,
  )
}
