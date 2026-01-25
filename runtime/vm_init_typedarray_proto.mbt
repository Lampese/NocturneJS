///|
fn init_typed_array_proto(
  typed_array_proto_obj : ObjectValue,
  array_proto_obj : ObjectValue,
  species_key : String,
) -> Value {
  let typed_array_ctor = new_builtin_value(
    BuiltinFunction::TypedArrayConstructor,
  )
  match typed_array_ctor {
    Builtin(builtin) => {
      props_set(builtin.props, 
        "prototype",
        property_data_const(Object(typed_array_proto_obj)),
      )
      props_set(builtin.props, 
        "from",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::TypedArrayFrom),
        ),
      )
      props_set(builtin.props, 
        "of",
        property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayOf)),
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
  props_set(typed_array_proto_obj.props, 
    "constructor",
    property_data_non_enum(typed_array_ctor),
  )
  set_accessor_property_with_enum(
    typed_array_proto_obj.props,
    "length",
    Some(new_builtin_value(BuiltinFunction::TypedArrayGetLength)),
    None,
    false,
  )
  props_set(typed_array_proto_obj.props, 
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayAt)),
  )
  props_set(typed_array_proto_obj.props, 
    "with",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayWith)),
  )
  set_accessor_property_with_enum(
    typed_array_proto_obj.props,
    "buffer",
    Some(new_builtin_value(BuiltinFunction::TypedArrayGetBuffer)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    typed_array_proto_obj.props,
    "byteLength",
    Some(new_builtin_value(BuiltinFunction::TypedArrayGetByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    typed_array_proto_obj.props,
    "byteOffset",
    Some(new_builtin_value(BuiltinFunction::TypedArrayGetByteOffset)),
    None,
    false,
  )
  props_set(typed_array_proto_obj.props, 
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySet)),
  )
  let typed_array_values_func = new_builtin_value(
    BuiltinFunction::TypedArrayValues,
  )
  props_set(typed_array_proto_obj.props, 
    "values",
    property_data_non_enum(typed_array_values_func),
  )
  props_set(typed_array_proto_obj.props, 
    "keys",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayKeys)),
  )
  props_set(typed_array_proto_obj.props, 
    "entries",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayEntries),
    ),
  )
  match symbol_iterator_key() {
    Some(key) =>
      props_set(typed_array_proto_obj.props, 
        key,
        property_data_non_enum(typed_array_values_func),
      )
    None => ()
  }
  match symbol_to_string_tag_key() {
    Some(key) =>
      set_accessor_property_with_enum(
        typed_array_proto_obj.props,
        key,
        Some(new_builtin_value(BuiltinFunction::TypedArrayGetToStringTag)),
        None,
        false,
      )
    None => ()
  }
  props_set(typed_array_proto_obj.props, 
    "copyWithin",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayCopyWithin),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayEvery)),
  )
  props_set(typed_array_proto_obj.props, 
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySome)),
  )
  props_set(typed_array_proto_obj.props, 
    "forEach",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayForEach),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayMap)),
  )
  props_set(typed_array_proto_obj.props, 
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFilter)),
  )
  props_set(typed_array_proto_obj.props, 
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayReduce)),
  )
  props_set(typed_array_proto_obj.props, 
    "reduceRight",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayReduceRight),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "fill",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFill)),
  )
  props_set(typed_array_proto_obj.props, 
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFind)),
  )
  props_set(typed_array_proto_obj.props, 
    "findIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindIndex),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "findLast",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindLast),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "findLastIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindLastIndex),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "reverse",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayReverse),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "toReversed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToReversed),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySlice)),
  )
  props_set(typed_array_proto_obj.props, 
    "subarray",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArraySubarray),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "sort",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySort)),
  )
  props_set(typed_array_proto_obj.props, 
    "toSorted",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToSorted),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "join",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayJoin)),
  )
  props_set(typed_array_proto_obj.props, 
    "toLocaleString",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToLocaleString),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "indexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayIndexOf),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "lastIndexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayLastIndexOf),
    ),
  )
  props_set(typed_array_proto_obj.props, 
    "includes",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayIncludes),
    ),
  )
  match props_get(array_proto_obj.props, "toString") {
    Some(prop) =>
      props_set(typed_array_proto_obj.props, 
        "toString",
        property_data_non_enum(prop.value),
      )
    None => ()
  }
  typed_array_ctor
}

///|
fn init_typed_array_proto_root(object_proto_obj : ObjectValue) -> ObjectValue {
  let typed_array_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let typed_array_proto_obj = match typed_array_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(typed_array_proto_ref, Some(typed_array_proto_obj))
  typed_array_proto_obj
}
