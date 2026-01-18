///|
fn init_object_and_function_proto(env : Env) -> (ObjectValue, Value, Value) {
  let object_proto_value = new_object_value_with_proto(None)
  let object_proto_obj = match object_proto_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  object_proto_obj.immutable_proto = true
  set_proto_ref(object_proto_ref, Some(object_proto_obj))
  let function_proto_props = Map::new()
  function_proto_props.set("length", property_data_readonly(Number(0.0)))
  function_proto_props.set("name", property_data_readonly(String("")))
  let function_proto_value = {
    let func = register_function_value(FunctionValue::{
      name: None,
      source: None,
      source_root: None,
      source_path: None,
      line_number: None,
      column_number: None,
      local_name_binding: false,
      params: [],
      param_inits: [],
      body: [],
      env,
      id: alloc_id(),
      props: function_proto_props,
      proto: Some(Object(object_proto_obj)),
      home_object: None,
      super_constructor: None,
      instance_fields: [],
      private_instance_fields: [],
      private_instance_props: Map::new(),
      is_constructor: false,
      is_class_constructor: false,
      is_default_constructor: false,
      extensible: true,
      is_strict: false,
      is_generator: false,
      is_async: false,
      is_arrow: false,
      in_class_field_init: false,
    })
    Function(func)
  }
  set_value_ref(function_proto_ref, Some(function_proto_value))
  let throw_type_error = throw_type_error_value_for_env(env)
  match function_proto_value {
    Function(func) => {
      func.props.set(
        "caller",
        property_accessor(throw_type_error, throw_type_error),
      )
      func.props.set(
        "arguments",
        property_accessor(throw_type_error, throw_type_error),
      )
      func.props.set(
        "call",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FunctionCall)),
      )
      func.props.set(
        "apply",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::FunctionApply),
        ),
      )
      func.props.set(
        "bind",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FunctionBind)),
      )
      func.props.set(
        "toString",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::FunctionToString),
        ),
      )
      func.props.set(
        "lineNumber",
        property_accessor(
          new_builtin_value(BuiltinFunction::FunctionProtoLineNumber),
          Undefined,
        ),
      )
      func.props.set(
        "columnNumber",
        property_accessor(
          new_builtin_value(BuiltinFunction::FunctionProtoColumnNumber),
          Undefined,
        ),
      )
    }
    _ => ()
  }
  (object_proto_obj, object_proto_value, function_proto_value)
}
