///|
fn init_symbol_ctor(
  symbol_proto_obj : ObjectValue,
  symbol_proto_value : Value,
) -> Value {
  let symbol_ctor = new_builtin_value(BuiltinFunction::SymbolConstructor)
  match symbol_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(symbol_proto_value))
      props_set(builtin.props, 
        "for",
        property_data_non_enum(new_builtin_value(BuiltinFunction::SymbolFor)),
      )
      props_set(builtin.props, 
        "keyFor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::SymbolKeyFor)),
      )
    }
    _ => ()
  }
  props_set(symbol_proto_obj.props, "constructor", property_data_non_enum(symbol_ctor))
  symbol_ctor
}

///|
fn init_iterator_ctor(iterator_proto_obj : ObjectValue) -> Value {
  let iterator_ctor = new_builtin_value(BuiltinFunction::IteratorConstructor)
  match iterator_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, 
        "prototype",
        property_data_const(Object(iterator_proto_obj)),
      )
      props_set(builtin.props, 
        "from",
        property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFrom)),
      )
      props_set(builtin.props, 
        "concat",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::IteratorConcat),
        ),
      )
    }
    _ => ()
  }
  iterator_ctor
}
