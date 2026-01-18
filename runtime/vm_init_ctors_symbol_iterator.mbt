///|
fn init_symbol_ctor(
  symbol_proto_obj : ObjectValue,
  symbol_proto_value : Value,
) -> Value {
  let symbol_ctor = new_builtin_value(BuiltinFunction::SymbolConstructor)
  match symbol_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(symbol_proto_value))
      builtin.props.set(
        "for",
        property_data_non_enum(new_builtin_value(BuiltinFunction::SymbolFor)),
      )
      builtin.props.set(
        "keyFor",
        property_data_non_enum(new_builtin_value(BuiltinFunction::SymbolKeyFor)),
      )
    }
    _ => ()
  }
  symbol_proto_obj.props.set("constructor", property_data_non_enum(symbol_ctor))
  symbol_ctor
}

///|
fn init_iterator_ctor(iterator_proto_obj : ObjectValue) -> Value {
  let iterator_ctor = new_builtin_value(BuiltinFunction::IteratorConstructor)
  match iterator_ctor {
    Builtin(builtin) => {
      builtin.props.set(
        "prototype",
        property_data_const(Object(iterator_proto_obj)),
      )
      builtin.props.set(
        "from",
        property_data_non_enum(new_builtin_value(BuiltinFunction::IteratorFrom)),
      )
      builtin.props.set(
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
