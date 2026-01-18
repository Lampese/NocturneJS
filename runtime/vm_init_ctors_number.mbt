///|
fn init_number_ctor(
  number_proto_obj : ObjectValue,
  number_proto_value : Value,
) -> (Value, Value, Value) {
  let parse_int_func = new_builtin_value(BuiltinFunction::ParseInt)
  let parse_float_func = new_builtin_value(BuiltinFunction::ParseFloat)
  let number_ctor = new_builtin_value(BuiltinFunction::NumberConstructor)
  match number_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(number_proto_value))
      builtin.props.set("parseInt", property_data_non_enum(parse_int_func))
      builtin.props.set("parseFloat", property_data_non_enum(parse_float_func))
      builtin.props.set(
        "isNaN",
        property_data_non_enum(new_builtin_value(BuiltinFunction::NumberIsNaN)),
      )
      builtin.props.set(
        "isFinite",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsFinite),
        ),
      )
      builtin.props.set(
        "isInteger",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsInteger),
        ),
      )
      builtin.props.set(
        "isSafeInteger",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsSafeInteger),
        ),
      )
      builtin.props.set(
        "MAX_VALUE",
        property_data_const(Number(1.7976931348623157e308)),
      )
      builtin.props.set(
        "MIN_VALUE",
        property_data_const(Number(number_min_value())),
      )
      builtin.props.set("NaN", property_data_const(Number(nan())))
      builtin.props.set(
        "NEGATIVE_INFINITY",
        property_data_const(Number(0.0 - inf())),
      )
      builtin.props.set("POSITIVE_INFINITY", property_data_const(Number(inf())))
      builtin.props.set(
        "EPSILON",
        property_data_const(Number(number_epsilon())),
      )
      builtin.props.set(
        "MAX_SAFE_INTEGER",
        property_data_const(Number(9007199254740991.0)),
      )
      builtin.props.set(
        "MIN_SAFE_INTEGER",
        property_data_const(Number(-9007199254740991.0)),
      )
    }
    _ => ()
  }
  number_proto_obj.props.set("constructor", property_data_non_enum(number_ctor))
  (number_ctor, parse_int_func, parse_float_func)
}
