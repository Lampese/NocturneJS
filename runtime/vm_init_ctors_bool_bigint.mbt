///|
fn init_bool_ctor(
  bool_proto_obj : ObjectValue,
  bool_proto_value : Value,
) -> Value {
  let bool_ctor = new_builtin_value(BuiltinFunction::BooleanConstructor)
  match bool_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(bool_proto_value))
    _ => ()
  }
  bool_proto_obj.props.set("constructor", property_data_non_enum(bool_ctor))
  bool_ctor
}

///|
fn init_bigint_ctor(
  bigint_proto_obj : ObjectValue,
  bigint_proto_value : Value,
) -> Value {
  let bigint_ctor = new_builtin_value(BuiltinFunction::BigIntConstructor)
  match bigint_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(bigint_proto_value))
      builtin.props.set(
        "asIntN",
        property_data_non_enum(new_builtin_value(BuiltinFunction::BigIntAsIntN)),
      )
      builtin.props.set(
        "asUintN",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::BigIntAsUintN),
        ),
      )
    }
    _ => ()
  }
  bigint_proto_obj.props.set("constructor", property_data_non_enum(bigint_ctor))
  bigint_ctor
}
