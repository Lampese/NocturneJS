///|
fn init_object_and_function_proto(env : Env) -> (ObjectValue, Value, Value) {
  let object_proto_value = new_object_value_with_proto(None)
  let object_proto_obj = match object_proto_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  object_proto_obj.immutable_proto = true
  set_proto_ref(object_proto_ref, Some(object_proto_obj))
  let function_proto_props = props_new()
  props_set(function_proto_props, "length", property_data_readonly(Number(0.0)))
  props_set(function_proto_props, "name", property_data_readonly(String("")))
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
      private_instance_props: props_new(),
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
    rc_incref_env(env)
    rc_incref_optional_value(func.proto)
    Function(func)
  }
  set_value_ref(function_proto_ref, Some(function_proto_value))
  let throw_type_error = throw_type_error_value_for_env(env)
  match function_proto_value {
    Function(func) => {
      props_set(func.props, 
        "caller",
        property_accessor(throw_type_error, throw_type_error),
      )
      props_set(func.props, 
        "arguments",
        property_accessor(throw_type_error, throw_type_error),
      )
      props_set(func.props, 
        "call",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FunctionCall)),
      )
      props_set(func.props, 
        "apply",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::FunctionApply),
        ),
      )
      props_set(func.props, 
        "bind",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FunctionBind)),
      )
      props_set(func.props, 
        "toString",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::FunctionToString),
        ),
      )
      props_set(func.props, 
        "lineNumber",
        property_accessor(
          new_builtin_value(BuiltinFunction::FunctionProtoLineNumber),
          Undefined,
        ),
      )
      props_set(func.props, 
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
