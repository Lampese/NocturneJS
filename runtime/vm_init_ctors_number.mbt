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
      props_set(builtin.props, "prototype", property_data_const(number_proto_value))
      props_set(builtin.props, "parseInt", property_data_non_enum(parse_int_func))
      props_set(builtin.props, "parseFloat", property_data_non_enum(parse_float_func))
      props_set(builtin.props, 
        "isNaN",
        property_data_non_enum(new_builtin_value(BuiltinFunction::NumberIsNaN)),
      )
      props_set(builtin.props, 
        "isFinite",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsFinite),
        ),
      )
      props_set(builtin.props, 
        "isInteger",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsInteger),
        ),
      )
      props_set(builtin.props, 
        "isSafeInteger",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::NumberIsSafeInteger),
        ),
      )
      props_set(builtin.props, 
        "MAX_VALUE",
        property_data_const(Number(1.7976931348623157e308)),
      )
      props_set(builtin.props, 
        "MIN_VALUE",
        property_data_const(Number(number_min_value())),
      )
      props_set(builtin.props, "NaN", property_data_const(Number(nan())))
      props_set(builtin.props, 
        "NEGATIVE_INFINITY",
        property_data_const(Number(0.0 - inf())),
      )
      props_set(builtin.props, "POSITIVE_INFINITY", property_data_const(Number(inf())))
      props_set(builtin.props, 
        "EPSILON",
        property_data_const(Number(number_epsilon())),
      )
      props_set(builtin.props, 
        "MAX_SAFE_INTEGER",
        property_data_const(Number(9007199254740991.0)),
      )
      props_set(builtin.props, 
        "MIN_SAFE_INTEGER",
        property_data_const(Number(-9007199254740991.0)),
      )
    }
    _ => ()
  }
  props_set(number_proto_obj.props, "constructor", property_data_non_enum(number_ctor))
  (number_ctor, parse_int_func, parse_float_func)
}
