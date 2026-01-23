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
      props_set(promise_proto_obj.props, 
        key,
        property_data_readonly(String("Promise")),
      )
    None => ()
  }
  props_set(promise_proto_obj.props, 
    "then",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseThen)),
  )
  props_set(promise_proto_obj.props, 
    "catch",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseCatch)),
  )
  props_set(promise_proto_obj.props, 
    "finally",
    property_data_non_enum(new_builtin_value(BuiltinFunction::PromiseFinally)),
  )
  (promise_proto_obj, promise_proto_value)
}
