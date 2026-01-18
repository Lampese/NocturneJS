///|
fn init_promise_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let promise_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let promise_proto_obj = match promise_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(promise_proto_ref, Some(promise_proto_obj))
  match symbol_to_string_tag_key() {
    Some(key) =>
      promise_proto_obj.props.set(
        key,
        property_data_readonly(String("Promise")),
      )
    None => ()
  }
  promise_proto_obj.props.set(
    "then",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseThen)),
  )
  promise_proto_obj.props.set(
    "catch",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseCatch)),
  )
  promise_proto_obj.props.set(
    "finally",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseFinally)),
  )
  (promise_proto_obj, promise_proto_value)
}
