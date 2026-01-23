///|
fn init_bool_ctor(
  bool_proto_obj : ObjectValue,
  bool_proto_value : Value,
) -> Value {
  let bool_ctor = new_builtin_value(BuiltinFunction::BooleanConstructor)
  match bool_ctor {
    Builtin(builtin) =>
      props_set(builtin.props, "prototype", property_data_const(bool_proto_value))
    _ => ()
  }
  props_set(bool_proto_obj.props, "constructor", property_data_non_enum(bool_ctor))
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
      props_set(builtin.props, "prototype", property_data_const(bigint_proto_value))
      props_set(builtin.props, 
        "asIntN",
        property_data_non_enum(new_builtin_value(BuiltinFunction::BigIntAsIntN)),
      )
      props_set(builtin.props, 
        "asUintN",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::BigIntAsUintN),
        ),
      )
    }
    _ => ()
  }
  props_set(bigint_proto_obj.props, "constructor", property_data_non_enum(bigint_ctor))
  bigint_ctor
}
