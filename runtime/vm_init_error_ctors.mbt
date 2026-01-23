///|
fn init_error_ctors(
  error_proto_obj : ObjectValue,
  error_proto_value : Value,
  aggregate_error_proto_obj : ObjectValue,
  aggregate_error_proto_value : Value,
  eval_error_proto_obj : ObjectValue,
  eval_error_proto_value : Value,
  syntax_error_proto_obj : ObjectValue,
  syntax_error_proto_value : Value,
  range_error_proto_obj : ObjectValue,
  range_error_proto_value : Value,
  type_error_proto_obj : ObjectValue,
  type_error_proto_value : Value,
  reference_error_proto_obj : ObjectValue,
  reference_error_proto_value : Value,
  uri_error_proto_obj : ObjectValue,
  uri_error_proto_value : Value,
) -> (Value, Value, Value, Value, Value, Value, Value, Value) {
  let error_ctor = new_builtin_value(BuiltinFunction::ErrorConstructor)
  match error_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(error_proto_value))
      props_set(builtin.props, 
        "isError",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ErrorIsError)),
      )
    }
    _ => ()
  }
  props_set(error_proto_obj.props, "constructor", property_data_non_enum(error_ctor))
  props_set(error_proto_obj.props, 
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ErrorToString)),
  )
  let aggregate_error_ctor = new_builtin_value(
    BuiltinFunction::AggregateErrorConstructor,
  )
  match aggregate_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(aggregate_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(aggregate_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(aggregate_error_ctor),
  )
  let eval_error_ctor = new_builtin_value(BuiltinFunction::EvalErrorConstructor)
  match eval_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(eval_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(eval_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(eval_error_ctor),
  )
  let syntax_error_ctor = new_builtin_value(
    BuiltinFunction::SyntaxErrorConstructor,
  )
  match syntax_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(syntax_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(syntax_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(syntax_error_ctor),
  )
  let range_error_ctor = new_builtin_value(
    BuiltinFunction::RangeErrorConstructor,
  )
  match range_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(range_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(range_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(range_error_ctor),
  )
  let type_error_ctor = new_builtin_value(BuiltinFunction::TypeErrorConstructor)
  match type_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(type_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(type_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(type_error_ctor),
  )
  let reference_error_ctor = new_builtin_value(
    BuiltinFunction::ReferenceErrorConstructor,
  )
  match reference_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, 
        "prototype",
        property_data_const(reference_error_proto_value),
      )
    }
    _ => ()
  }
  props_set(reference_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(reference_error_ctor),
  )
  let uri_error_ctor = new_builtin_value(BuiltinFunction::UriErrorConstructor)
  match uri_error_ctor {
    Builtin(builtin) => {
      rc_replace_optional_value(builtin.proto, Some(error_ctor))
      builtin.proto = Some(error_ctor)
      props_set(builtin.props, "prototype", property_data_const(uri_error_proto_value))
    }
    _ => ()
  }
  props_set(uri_error_proto_obj.props, 
    "constructor",
    property_data_non_enum(uri_error_ctor),
  )
  (
    error_ctor, aggregate_error_ctor, eval_error_ctor, syntax_error_ctor, range_error_ctor,
    type_error_ctor, reference_error_ctor, uri_error_ctor,
  )
}
