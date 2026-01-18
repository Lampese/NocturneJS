///|
fn init_number_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let number_proto_value = {
    let obj = register_object_value(ObjectValue::{
      id: alloc_id(),
      props: Map::new(),
      proto: Some(Object(object_proto_obj)),
      string_data: None,
      number_data: Some(0.0),
      bool_data: None,
      bigint_data: None,
      symbol_data: None,
      regexp_data: None,
      generator_data: None,
      async_generator_data: None,
      proxy_data: None,
      map_data: None,
      set_data: None,
      map_iter_data: None,
      set_iter_data: None,
      iterator_wrap_data: None,
      iterator_concat_data: None,
      iterator_helper_data: None,
      weakmap_data: None,
      weakset_data: None,
      weakref_data: None,
      finreg_data: None,
      date_data: None,
      array_buffer_data: None,
      dataview_data: None,
      is_html_dda: false,
      is_error: false,
      is_module_namespace: false,
      realm_env: None,
      immutable_proto: false,
      extensible: true,
    })
    Object(obj)
  }
  let number_proto_obj = match number_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(number_proto_ref, Some(number_proto_obj))
  number_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::NumberToString)),
  )
  number_proto_obj.props.set(
    "toLocaleString",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::NumberToLocaleString),
    ),
  )
  number_proto_obj.props.set(
    "toFixed",
    property_data_non_enum(new_builtin_value(BuiltinFunction::NumberToFixed)),
  )
  number_proto_obj.props.set(
    "toExponential",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::NumberToExponential),
    ),
  )
  number_proto_obj.props.set(
    "toPrecision",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::NumberToPrecision),
    ),
  )
  number_proto_obj.props.set(
    "valueOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::NumberValueOf)),
  )
  (number_proto_obj, number_proto_value)
}

///|
fn init_bool_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let bool_proto_value = {
    let obj = register_object_value(ObjectValue::{
      id: alloc_id(),
      props: Map::new(),
      proto: Some(Object(object_proto_obj)),
      string_data: None,
      number_data: None,
      bool_data: Some(false),
      bigint_data: None,
      symbol_data: None,
      regexp_data: None,
      generator_data: None,
      async_generator_data: None,
      proxy_data: None,
      map_data: None,
      set_data: None,
      map_iter_data: None,
      set_iter_data: None,
      iterator_wrap_data: None,
      iterator_concat_data: None,
      iterator_helper_data: None,
      weakmap_data: None,
      weakset_data: None,
      weakref_data: None,
      finreg_data: None,
      date_data: None,
      array_buffer_data: None,
      dataview_data: None,
      is_html_dda: false,
      is_error: false,
      is_module_namespace: false,
      realm_env: None,
      immutable_proto: false,
      extensible: true,
    })
    Object(obj)
  }
  let bool_proto_obj = match bool_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(bool_proto_ref, Some(bool_proto_obj))
  bool_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::BooleanToString)),
  )
  bool_proto_obj.props.set(
    "valueOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::BooleanValueOf)),
  )
  (bool_proto_obj, bool_proto_value)
}

///|
fn init_bigint_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let bigint_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let bigint_proto_obj = match bigint_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(bigint_proto_ref, Some(bigint_proto_obj))
  match symbol_to_string_tag_key() {
    Some(key) =>
      bigint_proto_obj.props.set(key, property_data_readonly(String("BigInt")))
    None => ()
  }
  bigint_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::BigIntToString)),
  )
  bigint_proto_obj.props.set(
    "valueOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::BigIntValueOf)),
  )
  (bigint_proto_obj, bigint_proto_value)
}
