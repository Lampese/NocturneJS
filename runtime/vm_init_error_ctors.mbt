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
      builtin.props.set("prototype", property_data_const(error_proto_value))
      builtin.props.set(
        "isError",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ErrorIsError)),
      )
    }
    _ => ()
  }
  error_proto_obj.props.set("constructor", property_data_non_enum(error_ctor))
  error_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ErrorToString)),
  )
  let aggregate_error_ctor = new_builtin_value(
    BuiltinFunction::AggregateErrorConstructor,
  )
  match aggregate_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(aggregate_error_proto_value),
      )
    }
    _ => ()
  }
  aggregate_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(aggregate_error_ctor),
  )
  let eval_error_ctor = new_builtin_value(BuiltinFunction::EvalErrorConstructor)
  match eval_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(eval_error_proto_value),
      )
    }
    _ => ()
  }
  eval_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(eval_error_ctor),
  )
  let syntax_error_ctor = new_builtin_value(
    BuiltinFunction::SyntaxErrorConstructor,
  )
  match syntax_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(syntax_error_proto_value),
      )
    }
    _ => ()
  }
  syntax_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(syntax_error_ctor),
  )
  let range_error_ctor = new_builtin_value(
    BuiltinFunction::RangeErrorConstructor,
  )
  match range_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(range_error_proto_value),
      )
    }
    _ => ()
  }
  range_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(range_error_ctor),
  )
  let type_error_ctor = new_builtin_value(BuiltinFunction::TypeErrorConstructor)
  match type_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(type_error_proto_value),
      )
    }
    _ => ()
  }
  type_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(type_error_ctor),
  )
  let reference_error_ctor = new_builtin_value(
    BuiltinFunction::ReferenceErrorConstructor,
  )
  match reference_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(reference_error_proto_value),
      )
    }
    _ => ()
  }
  reference_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(reference_error_ctor),
  )
  let uri_error_ctor = new_builtin_value(BuiltinFunction::UriErrorConstructor)
  match uri_error_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(error_ctor)
      builtin.props.set("prototype", property_data_const(uri_error_proto_value))
    }
    _ => ()
  }
  uri_error_proto_obj.props.set(
    "constructor",
    property_data_non_enum(uri_error_ctor),
  )
  (
    error_ctor, aggregate_error_ctor, eval_error_ctor, syntax_error_ctor, range_error_ctor,
    type_error_ctor, reference_error_ctor, uri_error_ctor,
  )
}
