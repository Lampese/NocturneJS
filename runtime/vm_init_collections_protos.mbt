///|
fn init_collection_protos(
  object_proto_obj : ObjectValue,
) -> (
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  Value,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
  ObjectValue,
  Value,
) {
  let map_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let map_proto_obj = match map_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(map_proto_ref, Some(map_proto_obj))
  props_set(map_proto_obj.props, 
    "get",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapGet)),
  )
  props_set(map_proto_obj.props, 
    "getOrInsert",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapGetOrInsert)),
  )
  props_set(map_proto_obj.props, 
    "getOrInsertComputed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::MapGetOrInsertComputed),
    ),
  )
  props_set(map_proto_obj.props, 
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapSet)),
  )
  props_set(map_proto_obj.props, 
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapHas)),
  )
  props_set(map_proto_obj.props, 
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapDelete)),
  )
  props_set(map_proto_obj.props, 
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapForEach)),
  )
  props_set(map_proto_obj.props, 
    "clear",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapClear)),
  )
  let map_keys_func = new_builtin_value(BuiltinFunction::MapKeys)
  let map_values_func = new_builtin_value(BuiltinFunction::MapValues)
  let map_entries_func = new_builtin_value(BuiltinFunction::MapEntries)
  props_set(map_proto_obj.props, "keys", property_data_non_enum(map_keys_func))
  props_set(map_proto_obj.props, "values", property_data_non_enum(map_values_func))
  props_set(map_proto_obj.props, "entries", property_data_non_enum(map_entries_func))
  match symbol_iterator_key() {
    Some(key) =>
      props_set(map_proto_obj.props, key, property_data_non_enum(map_entries_func))
    None => ()
  }
  props_set(map_proto_obj.props, "size", Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: Some(new_builtin_value(BuiltinFunction::MapSize)),
    setter: None,
  })
  let set_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let set_proto_obj = match set_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(set_proto_ref_cell, Some(set_proto_obj))
  props_set(set_proto_obj.props, 
    "add",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetAdd)),
  )
  props_set(set_proto_obj.props, 
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetHas)),
  )
  props_set(set_proto_obj.props, 
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetDelete)),
  )
  props_set(set_proto_obj.props, 
    "clear",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetClear)),
  )
  props_set(set_proto_obj.props, 
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetForEach)),
  )
  props_set(set_proto_obj.props, 
    "isDisjointFrom",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SetIsDisjointFrom),
    ),
  )
  props_set(set_proto_obj.props, 
    "isSubsetOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIsSubsetOf)),
  )
  props_set(set_proto_obj.props, 
    "isSupersetOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIsSupersetOf)),
  )
  props_set(set_proto_obj.props, 
    "intersection",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIntersection)),
  )
  props_set(set_proto_obj.props, 
    "difference",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetDifference)),
  )
  props_set(set_proto_obj.props, 
    "symmetricDifference",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SetSymmetricDifference),
    ),
  )
  props_set(set_proto_obj.props, 
    "union",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetUnion)),
  )
  let set_values_func = new_builtin_value(BuiltinFunction::SetValues)
  let set_entries_func = new_builtin_value(BuiltinFunction::SetEntries)
  props_set(set_proto_obj.props, "values", property_data_non_enum(set_values_func))
  props_set(set_proto_obj.props, "keys", property_data_non_enum(set_values_func))
  props_set(set_proto_obj.props, "entries", property_data_non_enum(set_entries_func))
  match symbol_iterator_key() {
    Some(key) =>
      props_set(set_proto_obj.props, key, property_data_non_enum(set_values_func))
    None => ()
  }
  props_set(set_proto_obj.props, "size", Property::{
    value: Undefined,
    writable: false,
    configurable: true,
    enumerable: false,
    getter: Some(new_builtin_value(BuiltinFunction::SetSize)),
    setter: None,
  })
  let weakmap_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let weakmap_proto_obj = match weakmap_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(weakmap_proto_ref, Some(weakmap_proto_obj))
  props_set(weakmap_proto_obj.props, 
    "get",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapGet)),
  )
  props_set(weakmap_proto_obj.props, 
    "getOrInsert",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::WeakMapGetOrInsert),
    ),
  )
  props_set(weakmap_proto_obj.props, 
    "getOrInsertComputed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::WeakMapGetOrInsertComputed),
    ),
  )
  props_set(weakmap_proto_obj.props, 
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapSet)),
  )
  props_set(weakmap_proto_obj.props, 
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapHas)),
  )
  props_set(weakmap_proto_obj.props, 
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapDelete)),
  )
  let weakset_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let weakset_proto_obj = match weakset_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(weakset_proto_ref_cell, Some(weakset_proto_obj))
  props_set(weakset_proto_obj.props, 
    "add",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakSetAdd)),
  )
  props_set(weakset_proto_obj.props, 
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakSetHas)),
  )
  props_set(weakset_proto_obj.props, 
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakSetDelete)),
  )
  let weakref_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let weakref_proto_obj = match weakref_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(weakref_proto_ref, Some(weakref_proto_obj))
  props_set(weakref_proto_obj.props, 
    "deref",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakRefDeref)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      props_set(weakref_proto_obj.props, 
        key,
        property_data_readonly(String("WeakRef")),
      )
    None => ()
  }
  let finreg_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let finreg_proto_obj = match finreg_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(finreg_proto_ref, Some(finreg_proto_obj))
  props_set(finreg_proto_obj.props, 
    "register",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::FinalizationRegistryRegister),
    ),
  )
  props_set(finreg_proto_obj.props, 
    "unregister",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::FinalizationRegistryUnregister),
    ),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      props_set(finreg_proto_obj.props, 
        key,
        property_data_readonly(String("FinalizationRegistry")),
      )
    None => ()
  }
  (
    map_proto_obj, map_proto_value, map_entries_func, set_proto_obj, set_proto_value,
    set_values_func, weakmap_proto_obj, weakmap_proto_value, weakset_proto_obj, weakset_proto_value,
    weakref_proto_obj, weakref_proto_value, finreg_proto_obj, finreg_proto_value,
  )
}
