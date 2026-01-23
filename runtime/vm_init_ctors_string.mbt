///|
fn init_string_ctor(
  string_proto_obj : ObjectValue,
  string_proto_value : Value,
) -> Value {
  let string_ctor = new_builtin_value(BuiltinFunction::StringConstructor)
  match string_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(string_proto_value))
      props_set(builtin.props, 
        "fromCharCode",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringFromCharCode),
        ),
      )
      props_set(builtin.props, 
        "fromCodePoint",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringFromCodePoint),
        ),
      )
      props_set(builtin.props, 
        "raw",
        property_data_non_enum(new_builtin_value(BuiltinFunction::StringRaw)),
      )
    }
    _ => ()
  }
  props_set(string_proto_obj.props, "constructor", property_data_non_enum(string_ctor))
  string_ctor
}
