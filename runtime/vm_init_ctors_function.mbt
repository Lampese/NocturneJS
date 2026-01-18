///|
fn init_function_ctor(function_proto_value : Value) -> Value {
  let function_ctor = new_builtin_value(BuiltinFunction::FunctionConstructor)
  match function_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(function_proto_value))
    _ => ()
  }
  match function_proto_value {
    Function(func) =>
      func.props.set("constructor", property_data_non_enum(function_ctor))
    Builtin(builtin) =>
      builtin.props.set("constructor", property_data_non_enum(function_ctor))
    _ => ()
  }
  function_ctor
}

///|
fn init_generator_family_ctors(
  function_ctor : Value,
  generator_function_proto_obj : ObjectValue,
  async_generator_function_proto_obj : ObjectValue,
  async_function_proto_obj : ObjectValue,
) -> Unit {
  let generator_function_ctor = new_builtin_value(
    BuiltinFunction::GeneratorFunctionConstructor,
  )
  match generator_function_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(function_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(Object(generator_function_proto_obj)),
      )
    }
    _ => ()
  }
  generator_function_proto_obj.props.set(
    "constructor",
    property_data_readonly(generator_function_ctor),
  )
  let async_generator_function_ctor = new_builtin_value(
    BuiltinFunction::AsyncGeneratorFunctionConstructor,
  )
  match async_generator_function_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(function_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(Object(async_generator_function_proto_obj)),
      )
    }
    _ => ()
  }
  async_generator_function_proto_obj.props.set(
    "constructor",
    property_data_readonly(async_generator_function_ctor),
  )
  let async_function_ctor = new_builtin_value(
    BuiltinFunction::AsyncFunctionConstructor,
  )
  match async_function_ctor {
    Builtin(builtin) => {
      builtin.proto = Some(function_ctor)
      builtin.props.set(
        "prototype",
        property_data_const(Object(async_function_proto_obj)),
      )
    }
    _ => ()
  }
  async_function_proto_obj.props.set(
    "constructor",
    property_data_readonly(async_function_ctor),
  )
}
