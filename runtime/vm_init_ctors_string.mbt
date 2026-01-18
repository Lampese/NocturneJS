///|
fn init_string_ctor(
  string_proto_obj : ObjectValue,
  string_proto_value : Value,
) -> Value {
  let string_ctor = new_builtin_value(BuiltinFunction::StringConstructor)
  match string_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(string_proto_value))
      builtin.props.set(
        "fromCharCode",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringFromCharCode),
        ),
      )
      builtin.props.set(
        "fromCodePoint",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::StringFromCodePoint),
        ),
      )
      builtin.props.set(
        "raw",
        property_data_non_enum(new_builtin_value(BuiltinFunction::StringRaw)),
      )
    }
    _ => ()
  }
  string_proto_obj.props.set("constructor", property_data_non_enum(string_ctor))
  string_ctor
}
