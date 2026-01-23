///|
fn init_regexp_date_ctors(
  regexp_proto_obj : ObjectValue,
  regexp_proto_value : Value,
  date_proto_obj : ObjectValue,
  date_proto_value : Value,
  species_key : String,
  to_primitive_key : String,
) -> (Value, Value) {
  let regexp_ctor = new_builtin_value(BuiltinFunction::RegExpConstructor)
  match regexp_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(regexp_proto_value))
      props_set(builtin.props, 
        "escape",
        property_data_non_enum(new_builtin_value(BuiltinFunction::RegExpEscape)),
      )
      set_accessor_property_with_enum(
        builtin.props,
        species_key,
        Some(new_builtin_value(BuiltinFunction::SpeciesGetter)),
        None,
        false,
      )
    }
    _ => ()
  }
  props_set(regexp_proto_obj.props, "constructor", property_data_non_enum(regexp_ctor))
  let date_ctor = new_builtin_value(BuiltinFunction::DateConstructor)
  match date_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(date_proto_value))
      props_set(builtin.props, 
        "parse",
        property_data_non_enum(new_builtin_value(BuiltinFunction::DateParse)),
      )
      props_set(builtin.props, 
        "UTC",
        property_data_non_enum(new_builtin_value(BuiltinFunction::DateUTC)),
      )
      props_set(builtin.props, 
        "now",
        property_data_non_enum(new_builtin_value(BuiltinFunction::DateNow)),
      )
    }
    _ => ()
  }
  props_set(date_proto_obj.props, "constructor", property_data_non_enum(date_ctor))
  props_set(date_proto_obj.props, 
    to_primitive_key,
    property_data_readonly(
      new_builtin_value(BuiltinFunction::DateSymbolToPrimitive),
    ),
  )
  (regexp_ctor, date_ctor)
}
