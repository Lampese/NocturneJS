///|
fn call_builtin_array(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::ArrayToString => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let join_method = property_get(obj, "join")
      if is_callable(join_method) {
        call_value_with_this(join_method, [], obj)
      } else {
        String(object_to_string(obj))
      }
    }
    BuiltinFunction::ArrayToLocaleString => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      String(array_to_locale_string_value(target))
    }
    BuiltinFunction::ArrayConstructor => {
      let array_proto_value = match current_env() {
        Some(env) =>
          match value_from_object(array_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(array_proto())
          }
        None => value_from_object(array_proto())
      }
      array_from_args_with_proto(args, array_proto_value)
    }
    BuiltinFunction::ArrayIsArray => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      Bool(is_array_value(value))
    }
    BuiltinFunction::ArrayFrom => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      array_from_value(target, args)
    }
    BuiltinFunction::ArrayOf => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      array_of_value(target, args)
    }
    BuiltinFunction::ArrayPush =>
      array_push_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayPop =>
      match builtin.this_value {
        Some(this_value) => {
          let obj = to_object(this_value)
          let len = array_like_length_int64(obj)
          let new_len = if len > 0L { len - 1L } else { 0L }
          let mut result = Undefined
          if len > 0L {
            let index_name = Int64::to_string(new_len)
            result = property_get(obj, index_name)
            delete_property_or_throw(obj, index_name)
          }
          let _ = property_set(
            obj,
            "length",
            Number(Int64::to_double(new_len)),
            true,
          )
          result
        }
        None => Undefined
      }
    BuiltinFunction::ArrayShift =>
      match builtin.this_value {
        Some(this_value) => {
          let obj = to_object(this_value)
          let len = array_like_length_int64(obj)
          if len <= 0L {
            let _ = property_set(
              obj,
              "length",
              Number(Double::from_int(0)),
              true,
            )
            Undefined
          } else {
            let first = property_get(obj, "0")
            let mut k = 1L
            while k < len {
              let from = Int64::to_string(k)
              let to = Int64::to_string(k - 1L)
              if has_property(obj, from) {
                let from_value = property_get(obj, from)
                let _ = property_set(obj, to, from_value, true)

              } else {
                delete_property_or_throw(obj, to)
              }
              k = k + 1L
            }
            delete_property_or_throw(obj, Int64::to_string(len - 1L))
            let _ = property_set(
              obj,
              "length",
              Number(Int64::to_double(len - 1L)),
              true,
            )
            first
          }
        }
        None => Undefined
      }
    BuiltinFunction::ArrayUnshift =>
      array_unshift_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArraySplice =>
      array_splice_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArraySlice =>
      array_slice_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayReverse =>
      array_reverse_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
      )
    BuiltinFunction::ArrayConcat =>
      match builtin.this_value {
        Some(this_value) => {
          let obj = to_object(this_value)
          let result = array_species_create(obj, 0)
          let max_safe = 9007199254740991L
          let mut n = 0L
          let mut i = -1
          while i < args.length() {
            let element = if i < 0 { obj } else { args[i] }
            if is_concat_spreadable(element) {
              let len = array_like_length_int64(element)
              if n + len > max_safe {
                return throw_type_error("Array too long")
              }
              let mut k = 0L
              while k < len {
                let key = Int64::to_string(k)
                if has_property_value(element, key) {
                  let value = property_get(element, key)
                  create_data_property_or_throw(
                    result,
                    Int64::to_string(n),
                    value,
                  )
                }
                n = n + 1L
                k = k + 1L
              }
            } else {
              if n >= max_safe {
                return throw_type_error("Array too long")
              }
              create_data_property_or_throw(
                result,
                Int64::to_string(n),
                element,
              )
              n = n + 1L
            }
            i = i + 1
          }
          let _ = property_set(
            result,
            "length",
            Number(Int64::to_double(n)),
            true,
          )
          result
        }
        None => new_array_value([])
      }
    BuiltinFunction::ArrayJoin => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let sep_value = if args.is_empty() { None } else { Some(args[0]) }
      String(array_join_value(target, sep_value))
    }
    BuiltinFunction::ArrayIndexOf =>
      array_index_of_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayLastIndexOf =>
      array_last_index_of_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayIncludes =>
      array_includes_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayCopyWithin =>
      array_copy_within_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayFill =>
      array_fill_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayFind =>
      array_find_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        ArrayFindKind::Find,
      )
    BuiltinFunction::ArrayFindIndex =>
      array_find_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        ArrayFindKind::FindIndex,
      )
    BuiltinFunction::ArrayFindLast =>
      array_find_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        ArrayFindKind::FindLast,
      )
    BuiltinFunction::ArrayFindLastIndex =>
      array_find_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        ArrayFindKind::FindLastIndex,
      )
    BuiltinFunction::ArrayFlat =>
      array_flatten_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        false,
      )
    BuiltinFunction::ArrayFlatMap =>
      array_flatten_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        true,
      )
    BuiltinFunction::ArrayToReversed =>
      array_to_reversed_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
      )
    BuiltinFunction::ArrayToSorted =>
      array_to_sorted_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayToSpliced =>
      array_to_spliced_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayWith =>
      array_with_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
      )
    BuiltinFunction::ArrayAt => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let len = array_like_length(obj)
      let index = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      let mut actual = index
      if actual < 0 {
        actual = len + actual
      }
      if actual < 0 || actual >= len {
        Undefined
      } else {
        property_get(obj, Int::to_string(actual))
      }
    }
    BuiltinFunction::ArrayMap => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      array_iterate(obj, callback, this_arg, ArrayIterKind::Map)
    }
    BuiltinFunction::ArrayForEach => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      let _ = array_iterate(obj, callback, this_arg, ArrayIterKind::ForEach)
      Undefined
    }
    BuiltinFunction::ArrayFilter => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      array_iterate(obj, callback, this_arg, ArrayIterKind::Filter)
    }
    BuiltinFunction::ArrayEvery => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      array_iterate(obj, callback, this_arg, ArrayIterKind::Every)
    }
    BuiltinFunction::ArraySome => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      array_iterate(obj, callback, this_arg, ArrayIterKind::SomeMatch)
    }
    BuiltinFunction::ArrayReduce => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let initial = if args.length() > 1 { Some(args[1]) } else { None }
      array_reduce_impl(obj, callback, initial, false)
    }
    BuiltinFunction::ArrayReduceRight => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let callback = if args.is_empty() { Undefined } else { args[0] }
      let initial = if args.length() > 1 { Some(args[1]) } else { None }
      array_reduce_impl(obj, callback, initial, true)
    }
    BuiltinFunction::ArraySort => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let compare_arg = if args.is_empty() { Undefined } else { args[0] }
      let compare_fn = if compare_arg is Undefined {
        None
      } else if !is_callable(compare_arg) {
        return throw_type_error("not a function")
      } else {
        Some(compare_arg)
      }
      array_sort_object(obj, compare_fn)
    }
    BuiltinFunction::ArrayValues => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      new_array_iterator(to_object(target), 1)
    }
    BuiltinFunction::ArrayKeys => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      new_array_iterator(to_object(target), 0)
    }
    BuiltinFunction::ArrayEntries => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      new_array_iterator(to_object(target), 2)
    }
    BuiltinFunction::ArrayIteratorNext =>
      array_iterator_next(builtin.this_value)
    _ => throw_type_error("invalid builtin")
  }
}
