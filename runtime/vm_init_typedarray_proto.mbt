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
      builtin.props.set(
        "prototype",
        property_data_const(Object(typed_array_proto_obj)),
      )
      builtin.props.set(
        "from",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::TypedArrayFrom),
        ),
      )
      builtin.props.set(
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
  typed_array_proto_obj.props.set(
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
  typed_array_proto_obj.props.set(
    "at",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayAt)),
  )
  typed_array_proto_obj.props.set(
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
  typed_array_proto_obj.props.set(
    "set",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySet)),
  )
  let typed_array_values_func = new_builtin_value(
    BuiltinFunction::TypedArrayValues,
  )
  typed_array_proto_obj.props.set(
    "values",
    property_data_non_enum(typed_array_values_func),
  )
  typed_array_proto_obj.props.set(
    "keys",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayKeys)),
  )
  typed_array_proto_obj.props.set(
    "entries",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayEntries),
    ),
  )
  match symbol_iterator_key() {
    Some(key) =>
      typed_array_proto_obj.props.set(
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
  typed_array_proto_obj.props.set(
    "copyWithin",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayCopyWithin),
    ),
  )
  typed_array_proto_obj.props.set(
    "every",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayEvery)),
  )
  typed_array_proto_obj.props.set(
    "some",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySome)),
  )
  typed_array_proto_obj.props.set(
    "forEach",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayForEach),
    ),
  )
  typed_array_proto_obj.props.set(
    "map",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayMap)),
  )
  typed_array_proto_obj.props.set(
    "filter",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFilter)),
  )
  typed_array_proto_obj.props.set(
    "reduce",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayReduce)),
  )
  typed_array_proto_obj.props.set(
    "reduceRight",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayReduceRight),
    ),
  )
  typed_array_proto_obj.props.set(
    "fill",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFill)),
  )
  typed_array_proto_obj.props.set(
    "find",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayFind)),
  )
  typed_array_proto_obj.props.set(
    "findIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindIndex),
    ),
  )
  typed_array_proto_obj.props.set(
    "findLast",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindLast),
    ),
  )
  typed_array_proto_obj.props.set(
    "findLastIndex",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayFindLastIndex),
    ),
  )
  typed_array_proto_obj.props.set(
    "reverse",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayReverse),
    ),
  )
  typed_array_proto_obj.props.set(
    "toReversed",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToReversed),
    ),
  )
  typed_array_proto_obj.props.set(
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySlice)),
  )
  typed_array_proto_obj.props.set(
    "subarray",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArraySubarray),
    ),
  )
  typed_array_proto_obj.props.set(
    "sort",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArraySort)),
  )
  typed_array_proto_obj.props.set(
    "toSorted",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToSorted),
    ),
  )
  typed_array_proto_obj.props.set(
    "join",
    property_data_non_enum(new_builtin_value(BuiltinFunction::TypedArrayJoin)),
  )
  typed_array_proto_obj.props.set(
    "toLocaleString",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayToLocaleString),
    ),
  )
  typed_array_proto_obj.props.set(
    "indexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayIndexOf),
    ),
  )
  typed_array_proto_obj.props.set(
    "lastIndexOf",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayLastIndexOf),
    ),
  )
  typed_array_proto_obj.props.set(
    "includes",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::TypedArrayIncludes),
    ),
  )
  match array_proto_obj.props.get("toString") {
    Some(prop) =>
      typed_array_proto_obj.props.set(
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
