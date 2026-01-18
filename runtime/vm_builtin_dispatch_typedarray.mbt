///|
fn call_builtin_typedarray(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::TypedArrayFrom => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_from_value(this_value, args)
    }
    BuiltinFunction::TypedArrayOf => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_of_value(this_value, args)
    }
    BuiltinFunction::TypedArrayGetLength =>
      typed_array_get_length_value(builtin.this_value)
    BuiltinFunction::TypedArrayGetBuffer =>
      typed_array_get_buffer_value(builtin.this_value)
    BuiltinFunction::TypedArrayGetByteLength =>
      typed_array_get_byte_length_value(builtin.this_value)
    BuiltinFunction::TypedArrayGetByteOffset =>
      typed_array_get_byte_offset_value(builtin.this_value)
    BuiltinFunction::TypedArrayGetToStringTag =>
      typed_array_get_to_string_tag_value(builtin.this_value)
    BuiltinFunction::TypedArrayAt => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_at_value(this_value, args)
    }
    BuiltinFunction::TypedArrayWith => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_with_value(this_value, args)
    }
    BuiltinFunction::TypedArrayValues => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_values_value(this_value)
    }
    BuiltinFunction::TypedArrayKeys => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_keys_value(this_value)
    }
    BuiltinFunction::TypedArrayEntries => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_entries_value(this_value)
    }
    BuiltinFunction::TypedArrayCopyWithin => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_copy_within_value(this_value, args)
    }
    BuiltinFunction::TypedArrayEvery => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      typed_array_iterate_value(
        this_value,
        callback,
        this_arg,
        ArrayIterKind::Every,
      )
    }
    BuiltinFunction::TypedArraySome => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      typed_array_iterate_value(
        this_value,
        callback,
        this_arg,
        ArrayIterKind::SomeMatch,
      )
    }
    BuiltinFunction::TypedArrayForEach => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      typed_array_iterate_value(
        this_value,
        callback,
        this_arg,
        ArrayIterKind::ForEach,
      )
    }
    BuiltinFunction::TypedArrayMap => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_map_value(this_value, args)
    }
    BuiltinFunction::TypedArrayFilter => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_filter_value(this_value, args)
    }
    BuiltinFunction::TypedArrayReduce => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_reduce_value(this_value, args, false)
    }
    BuiltinFunction::TypedArrayReduceRight => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_reduce_value(this_value, args, true)
    }
    BuiltinFunction::TypedArrayFind => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_find_value(this_value, args, ArrayFindKind::Find)
    }
    BuiltinFunction::TypedArrayFindIndex => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_find_value(this_value, args, ArrayFindKind::FindIndex)
    }
    BuiltinFunction::TypedArrayFindLast => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_find_value(this_value, args, ArrayFindKind::FindLast)
    }
    BuiltinFunction::TypedArrayFindLastIndex => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_find_value(this_value, args, ArrayFindKind::FindLastIndex)
    }
    BuiltinFunction::TypedArrayReverse => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_reverse_value(this_value)
    }
    BuiltinFunction::TypedArrayToReversed => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_to_reversed_value(this_value)
    }
    BuiltinFunction::TypedArraySlice => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_slice_value(this_value, args)
    }
    BuiltinFunction::TypedArraySubarray => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_subarray_value(this_value, args)
    }
    BuiltinFunction::TypedArraySort => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_sort_value(this_value, args)
    }
    BuiltinFunction::TypedArrayToSorted => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_to_sorted_value(this_value, args)
    }
    BuiltinFunction::TypedArrayJoin => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      String(typed_array_join_value(this_value, args))
    }
    BuiltinFunction::TypedArrayToLocaleString => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      String(typed_array_to_locale_string_value(this_value))
    }
    BuiltinFunction::TypedArrayIndexOf => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_index_of_value(this_value, args)
    }
    BuiltinFunction::TypedArrayLastIndexOf => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_last_index_of_value(this_value, args)
    }
    BuiltinFunction::TypedArrayIncludes => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_includes_value(this_value, args)
    }
    BuiltinFunction::TypedArraySet => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_set_value(this_value, args)
    }
    BuiltinFunction::TypedArrayFill => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      typed_array_fill_value(this_value, args)
    }
    BuiltinFunction::TypedArrayConstructor =>
      throw_type_error("cannot be called")
    BuiltinFunction::Uint8ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Int8ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Uint8ClampedArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Uint16ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Int16ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Uint32ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Int32ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::BigInt64ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::BigUint64ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Float16ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Float32ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::Float64ArrayConstructor =>
      throw_type_error("constructor requires 'new'")
    _ => throw_type_error("invalid builtin")
  }
}
