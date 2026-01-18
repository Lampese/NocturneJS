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
  map_proto_obj.props.set(
    "get",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapGet)),
  )
  map_proto_obj.props.set(
    "getOrInsert",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapGetOrInsert)),
  )
  map_proto_obj.props.set(
    "getOrInsertComputed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::MapGetOrInsertComputed),
    ),
  )
  map_proto_obj.props.set(
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapSet)),
  )
  map_proto_obj.props.set(
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapHas)),
  )
  map_proto_obj.props.set(
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapDelete)),
  )
  map_proto_obj.props.set(
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapForEach)),
  )
  map_proto_obj.props.set(
    "clear",
    property_data_non_enum(new_builtin_value(BuiltinFunction::MapClear)),
  )
  let map_keys_func = new_builtin_value(BuiltinFunction::MapKeys)
  let map_values_func = new_builtin_value(BuiltinFunction::MapValues)
  let map_entries_func = new_builtin_value(BuiltinFunction::MapEntries)
  map_proto_obj.props.set("keys", property_data_non_enum(map_keys_func))
  map_proto_obj.props.set("values", property_data_non_enum(map_values_func))
  map_proto_obj.props.set("entries", property_data_non_enum(map_entries_func))
  match symbol_iterator_key() {
    Some(key) =>
      map_proto_obj.props.set(key, property_data_non_enum(map_entries_func))
    None => ()
  }
  map_proto_obj.props.set("size", Property::{
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
  set_proto_obj.props.set(
    "add",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetAdd)),
  )
  set_proto_obj.props.set(
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetHas)),
  )
  set_proto_obj.props.set(
    "delete",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetDelete)),
  )
  set_proto_obj.props.set(
    "clear",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetClear)),
  )
  set_proto_obj.props.set(
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetForEach)),
  )
  set_proto_obj.props.set(
    "isDisjointFrom",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SetIsDisjointFrom),
    ),
  )
  set_proto_obj.props.set(
    "isSubsetOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIsSubsetOf)),
  )
  set_proto_obj.props.set(
    "isSupersetOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIsSupersetOf)),
  )
  set_proto_obj.props.set(
    "intersection",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetIntersection)),
  )
  set_proto_obj.props.set(
    "difference",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetDifference)),
  )
  set_proto_obj.props.set(
    "symmetricDifference",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SetSymmetricDifference),
    ),
  )
  set_proto_obj.props.set(
    "union",
    property_data_non_enum(new_builtin_value(BuiltinFunction::SetUnion)),
  )
  let set_values_func = new_builtin_value(BuiltinFunction::SetValues)
  let set_entries_func = new_builtin_value(BuiltinFunction::SetEntries)
  set_proto_obj.props.set("values", property_data_non_enum(set_values_func))
  set_proto_obj.props.set("keys", property_data_non_enum(set_values_func))
  set_proto_obj.props.set("entries", property_data_non_enum(set_entries_func))
  match symbol_iterator_key() {
    Some(key) =>
      set_proto_obj.props.set(key, property_data_non_enum(set_values_func))
    None => ()
  }
  set_proto_obj.props.set("size", Property::{
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
  weakmap_proto_obj.props.set(
    "get",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapGet)),
  )
  weakmap_proto_obj.props.set(
    "getOrInsert",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::WeakMapGetOrInsert),
    ),
  )
  weakmap_proto_obj.props.set(
    "getOrInsertComputed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::WeakMapGetOrInsertComputed),
    ),
  )
  weakmap_proto_obj.props.set(
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapSet)),
  )
  weakmap_proto_obj.props.set(
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakMapHas)),
  )
  weakmap_proto_obj.props.set(
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
  weakset_proto_obj.props.set(
    "add",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakSetAdd)),
  )
  weakset_proto_obj.props.set(
    "has",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakSetHas)),
  )
  weakset_proto_obj.props.set(
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
  weakref_proto_obj.props.set(
    "deref",
    property_data_non_enum(new_builtin_value(BuiltinFunction::WeakRefDeref)),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      weakref_proto_obj.props.set(
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
  finreg_proto_obj.props.set(
    "register",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::FinalizationRegistryRegister),
    ),
  )
  finreg_proto_obj.props.set(
    "unregister",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::FinalizationRegistryUnregister),
    ),
  )
  match symbol_to_string_tag_key() {
    Some(key) =>
      finreg_proto_obj.props.set(
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
