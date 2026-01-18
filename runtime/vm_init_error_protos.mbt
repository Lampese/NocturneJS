///|
fn init_error_protos(
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
  ObjectValue,
  Value,
) {
  let error_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let error_proto_obj = match error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(error_proto_ref, Some(error_proto_obj))
  error_proto_obj.props.set("name", property_data_non_enum(String("Error")))
  error_proto_obj.props.set("message", property_data_non_enum(String("")))
  let aggregate_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let aggregate_error_proto_obj = match aggregate_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(aggregate_error_proto_ref, Some(aggregate_error_proto_obj))
  aggregate_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("AggregateError")),
  )
  aggregate_error_proto_obj.props.set(
    "message",
    property_data_non_enum(String("")),
  )
  let eval_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let eval_error_proto_obj = match eval_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(eval_error_proto_ref, Some(eval_error_proto_obj))
  eval_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("EvalError")),
  )
  eval_error_proto_obj.props.set("message", property_data_non_enum(String("")))
  let syntax_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let syntax_error_proto_obj = match syntax_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(syntax_error_proto_ref, Some(syntax_error_proto_obj))
  syntax_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("SyntaxError")),
  )
  syntax_error_proto_obj.props.set(
    "message",
    property_data_non_enum(String("")),
  )
  let range_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let range_error_proto_obj = match range_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(range_error_proto_ref, Some(range_error_proto_obj))
  range_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("RangeError")),
  )
  range_error_proto_obj.props.set("message", property_data_non_enum(String("")))
  let type_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let type_error_proto_obj = match type_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(type_error_proto_ref, Some(type_error_proto_obj))
  type_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("TypeError")),
  )
  type_error_proto_obj.props.set("message", property_data_non_enum(String("")))
  let reference_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let reference_error_proto_obj = match reference_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(reference_error_proto_ref, Some(reference_error_proto_obj))
  reference_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("ReferenceError")),
  )
  reference_error_proto_obj.props.set(
    "message",
    property_data_non_enum(String("")),
  )
  let uri_error_proto_value = new_object_value_with_proto(
    Some(Object(error_proto_obj)),
  )
  let uri_error_proto_obj = match uri_error_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(error_proto_obj)))
  }
  set_proto_ref(uri_error_proto_ref, Some(uri_error_proto_obj))
  uri_error_proto_obj.props.set(
    "name",
    property_data_non_enum(String("URIError")),
  )
  uri_error_proto_obj.props.set("message", property_data_non_enum(String("")))
  (
    error_proto_obj, error_proto_value, aggregate_error_proto_obj, aggregate_error_proto_value,
    eval_error_proto_obj, eval_error_proto_value, syntax_error_proto_obj, syntax_error_proto_value,
    range_error_proto_obj, range_error_proto_value, type_error_proto_obj, type_error_proto_value,
    reference_error_proto_obj, reference_error_proto_value, uri_error_proto_obj,
    uri_error_proto_value,
  )
}
