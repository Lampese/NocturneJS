///|
fn init_array_proto(
  object_proto_obj : ObjectValue,
) -> (ObjectValue, Value, Value) {
  let array_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let array_proto_obj = match array_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(array_proto_ref, Some(array_proto_obj))
  props_set(array_proto_obj.props, "length", Property::{
    value: Number(0.0),
    writable: true,
    configurable: false,
    enumerable: false,
    getter: None,
    setter: None,
  })
  props_set(array_proto_obj.props, 
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToString)),
  )
  props_set(array_proto_obj.props, 
    "toLocaleString",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayToLocaleString),
    ),
  )
  props_set(array_proto_obj.props, 
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayAt)),
  )
  props_set(array_proto_obj.props, 
    "push",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayPush)),
  )
  props_set(array_proto_obj.props, 
    "pop",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayPop)),
  )
  props_set(array_proto_obj.props, 
    "shift",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayShift)),
  )
  props_set(array_proto_obj.props, 
    "unshift",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayUnshift)),
  )
  props_set(array_proto_obj.props, 
    "splice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySplice)),
  )
  props_set(array_proto_obj.props, 
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySlice)),
  )
  props_set(array_proto_obj.props, 
    "reverse",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReverse)),
  )
  props_set(array_proto_obj.props, 
    "concat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayConcat)),
  )
  props_set(array_proto_obj.props, 
    "join",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayJoin)),
  )
  props_set(array_proto_obj.props, 
    "indexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayIndexOf)),
  )
  props_set(array_proto_obj.props, 
    "lastIndexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayLastIndexOf)),
  )
  props_set(array_proto_obj.props, 
    "includes",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayIncludes)),
  )
  props_set(array_proto_obj.props, 
    "copyWithin",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayCopyWithin)),
  )
  props_set(array_proto_obj.props, 
    "fill",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFill)),
  )
  props_set(array_proto_obj.props, 
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFind)),
  )
  props_set(array_proto_obj.props, 
    "findIndex",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFindIndex)),
  )
  props_set(array_proto_obj.props, 
    "findLast",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFindLast)),
  )
  props_set(array_proto_obj.props, 
    "findLastIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayFindLastIndex),
    ),
  )
  props_set(array_proto_obj.props, 
    "flat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFlat)),
  )
  props_set(array_proto_obj.props, 
    "flatMap",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFlatMap)),
  )
  props_set(array_proto_obj.props, 
    "toReversed",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToReversed)),
  )
  props_set(array_proto_obj.props, 
    "toSorted",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToSorted)),
  )
  props_set(array_proto_obj.props, 
    "toSpliced",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToSpliced)),
  )
  props_set(array_proto_obj.props, 
    "with",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayWith)),
  )
  props_set(array_proto_obj.props, 
    "entries",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEntries)),
  )
  props_set(array_proto_obj.props, 
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEvery)),
  )
  props_set(array_proto_obj.props, 
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySome)),
  )
  props_set(array_proto_obj.props, 
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayForEach)),
  )
  props_set(array_proto_obj.props, 
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayMap)),
  )
  props_set(array_proto_obj.props, 
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFilter)),
  )
  props_set(array_proto_obj.props, 
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReduce)),
  )
  props_set(array_proto_obj.props, 
    "reduceRight",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReduceRight)),
  )
  props_set(array_proto_obj.props, 
    "sort",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySort)),
  )
  let array_values_func = new_builtin_value(BuiltinFunction::ArrayValues)
  props_set(array_proto_obj.props, "values", property_data_non_enum(array_values_func))
  props_set(array_proto_obj.props, 
    "keys",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayKeys)),
  )
  props_set(array_proto_obj.props, 
    "entries",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEntries)),
  )
  match symbol_iterator_key() {
    Some(key) =>
      props_set(array_proto_obj.props, key, property_data_non_enum(array_values_func))
    None => ()
  }
  (array_proto_obj, array_proto_value, array_values_func)
}
