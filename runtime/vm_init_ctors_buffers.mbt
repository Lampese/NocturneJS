///|
fn init_array_buffer_ctors(
  array_proto_obj : ObjectValue,
  array_proto_value : Value,
  array_buffer_proto_obj : ObjectValue,
  array_buffer_proto_value : Value,
  shared_array_buffer_proto_obj : ObjectValue,
  shared_array_buffer_proto_value : Value,
  dataview_proto_obj : ObjectValue,
  dataview_proto_value : Value,
  species_key : String,
) -> (Value, Value, Value, Value) {
  let array_ctor = new_builtin_value(BuiltinFunction::ArrayConstructor)
  match array_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, "prototype", property_data_const(array_proto_value))
      props_set(builtin.props, 
        "isArray",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayIsArray)),
      )
      props_set(builtin.props, 
        "from",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFrom)),
      )
      props_set(builtin.props, 
        "of",
        property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayOf)),
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
  props_set(array_proto_obj.props, "constructor", property_data_non_enum(array_ctor))
  let array_buffer_ctor = new_builtin_value(
    BuiltinFunction::ArrayBufferConstructor,
  )
  match array_buffer_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, 
        "prototype",
        property_data_const(array_buffer_proto_value),
      )
      props_set(builtin.props, 
        "isView",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::ArrayBufferIsView),
        ),
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
  props_set(array_buffer_proto_obj.props, 
    "constructor",
    property_data_non_enum(array_buffer_ctor),
  )
  let shared_array_buffer_ctor = new_builtin_value(
    BuiltinFunction::SharedArrayBufferConstructor,
  )
  match shared_array_buffer_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, 
        "prototype",
        property_data_const(shared_array_buffer_proto_value),
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
  props_set(shared_array_buffer_proto_obj.props, 
    "constructor",
    property_data_non_enum(shared_array_buffer_ctor),
  )
  let dataview_ctor = new_builtin_value(BuiltinFunction::DataViewConstructor)
  match dataview_ctor {
    Builtin(builtin) =>
      props_set(builtin.props, "prototype", property_data_const(dataview_proto_value))
    _ => ()
  }
  props_set(dataview_proto_obj.props, 
    "constructor",
    property_data_non_enum(dataview_ctor),
  )
  (array_ctor, array_buffer_ctor, shared_array_buffer_ctor, dataview_ctor)
}
