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
  array_proto_obj.props.set("length", Property::{
    value: Number(0.0),
    writable: true,
    configurable: false,
    enumerable: false,
    getter: None,
    setter: None,
  })
  array_proto_obj.props.set(
    "toString",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToString)),
  )
  array_proto_obj.props.set(
    "toLocaleString",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayToLocaleString),
    ),
  )
  array_proto_obj.props.set(
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayAt)),
  )
  array_proto_obj.props.set(
    "push",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayPush)),
  )
  array_proto_obj.props.set(
    "pop",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayPop)),
  )
  array_proto_obj.props.set(
    "shift",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayShift)),
  )
  array_proto_obj.props.set(
    "unshift",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayUnshift)),
  )
  array_proto_obj.props.set(
    "splice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySplice)),
  )
  array_proto_obj.props.set(
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySlice)),
  )
  array_proto_obj.props.set(
    "reverse",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReverse)),
  )
  array_proto_obj.props.set(
    "concat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayConcat)),
  )
  array_proto_obj.props.set(
    "join",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayJoin)),
  )
  array_proto_obj.props.set(
    "indexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayIndexOf)),
  )
  array_proto_obj.props.set(
    "lastIndexOf",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayLastIndexOf)),
  )
  array_proto_obj.props.set(
    "includes",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayIncludes)),
  )
  array_proto_obj.props.set(
    "copyWithin",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayCopyWithin)),
  )
  array_proto_obj.props.set(
    "fill",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFill)),
  )
  array_proto_obj.props.set(
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFind)),
  )
  array_proto_obj.props.set(
    "findIndex",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFindIndex)),
  )
  array_proto_obj.props.set(
    "findLast",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFindLast)),
  )
  array_proto_obj.props.set(
    "findLastIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayFindLastIndex),
    ),
  )
  array_proto_obj.props.set(
    "flat",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFlat)),
  )
  array_proto_obj.props.set(
    "flatMap",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFlatMap)),
  )
  array_proto_obj.props.set(
    "toReversed",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToReversed)),
  )
  array_proto_obj.props.set(
    "toSorted",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToSorted)),
  )
  array_proto_obj.props.set(
    "toSpliced",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayToSpliced)),
  )
  array_proto_obj.props.set(
    "with",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayWith)),
  )
  array_proto_obj.props.set(
    "entries",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEntries)),
  )
  array_proto_obj.props.set(
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEvery)),
  )
  array_proto_obj.props.set(
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySome)),
  )
  array_proto_obj.props.set(
    "forEach",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayForEach)),
  )
  array_proto_obj.props.set(
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayMap)),
  )
  array_proto_obj.props.set(
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayFilter)),
  )
  array_proto_obj.props.set(
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReduce)),
  )
  array_proto_obj.props.set(
    "reduceRight",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayReduceRight)),
  )
  array_proto_obj.props.set(
    "sort",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArraySort)),
  )
  let array_values_func = new_builtin_value(BuiltinFunction::ArrayValues)
  array_proto_obj.props.set("values", property_data_non_enum(array_values_func))
  array_proto_obj.props.set(
    "keys",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayKeys)),
  )
  array_proto_obj.props.set(
    "entries",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayEntries)),
  )
  match symbol_iterator_key() {
    Some(key) =>
      array_proto_obj.props.set(key, property_data_non_enum(array_values_func))
    None => ()
  }
  (array_proto_obj, array_proto_value, array_values_func)
}
