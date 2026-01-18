///|
fn init_collection_ctors(
  map_proto_obj : ObjectValue,
  map_proto_value : Value,
  set_proto_obj : ObjectValue,
  set_proto_value : Value,
  weakmap_proto_obj : ObjectValue,
  weakmap_proto_value : Value,
  weakset_proto_obj : ObjectValue,
  weakset_proto_value : Value,
  weakref_proto_obj : ObjectValue,
  weakref_proto_value : Value,
  finreg_proto_obj : ObjectValue,
  finreg_proto_value : Value,
  species_key : String,
) -> (Value, Value, Value, Value, Value, Value) {
  let map_ctor = new_builtin_value(BuiltinFunction::MapConstructor)
  match map_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(map_proto_value))
      builtin.props.set(
        "groupBy",
        property_data_non_enum(new_builtin_value(BuiltinFunction::MapGroupBy)),
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
  map_proto_obj.props.set("constructor", property_data_non_enum(map_ctor))
  let set_ctor = new_builtin_value(BuiltinFunction::SetConstructor)
  match set_ctor {
    Builtin(builtin) => {
      builtin.props.set("prototype", property_data_const(set_proto_value))
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
  set_proto_obj.props.set("constructor", property_data_non_enum(set_ctor))
  let weakmap_ctor = new_builtin_value(BuiltinFunction::WeakMapConstructor)
  match weakmap_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(weakmap_proto_value))
    _ => ()
  }
  weakmap_proto_obj.props.set(
    "constructor",
    property_data_non_enum(weakmap_ctor),
  )
  let weakset_ctor = new_builtin_value(BuiltinFunction::WeakSetConstructor)
  match weakset_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(weakset_proto_value))
    _ => ()
  }
  weakset_proto_obj.props.set(
    "constructor",
    property_data_non_enum(weakset_ctor),
  )
  let weakref_ctor = new_builtin_value(BuiltinFunction::WeakRefConstructor)
  match weakref_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(weakref_proto_value))
    _ => ()
  }
  weakref_proto_obj.props.set(
    "constructor",
    property_data_non_enum(weakref_ctor),
  )
  let finreg_ctor = new_builtin_value(
    BuiltinFunction::FinalizationRegistryConstructor,
  )
  match finreg_ctor {
    Builtin(builtin) =>
      builtin.props.set("prototype", property_data_const(finreg_proto_value))
    _ => ()
  }
  finreg_proto_obj.props.set("constructor", property_data_non_enum(finreg_ctor))
  (map_ctor, set_ctor, weakmap_ctor, weakset_ctor, weakref_ctor, finreg_ctor)
}
