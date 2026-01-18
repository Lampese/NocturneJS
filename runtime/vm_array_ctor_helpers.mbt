///|
fn array_from_value(this_value : Value, args : Array[Value]) -> Value raise {
  let items = if args.is_empty() { Undefined } else { args[0] }
  let mapfn = if args.length() > 1 { args[1] } else { Undefined }
  let this_arg = if args.length() > 2 { args[2] } else { Undefined }
  if !(mapfn is Undefined) && !is_callable(mapfn) {
    return throw_type_error("not a function")
  }
  let obj = to_object(items)
  let iter_method = match symbol_iterator_key() {
    Some(key) => property_get_with_receiver(obj, key, items)
    None => Undefined
  }
  if !(iter_method is Undefined) && !(iter_method is Null) {
    if !is_callable(iter_method) {
      return throw_type_error("value is not iterable")
    }
    let result = if is_constructor_value(this_value) {
      call_constructor(this_value, [])
    } else {
      let array_proto_value = match current_env() {
        Some(env) =>
          match value_from_object(array_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(array_proto())
          }
        None => value_from_object(array_proto())
      }
      new_array_value_with_proto([], proto=array_proto_value)
    }
    let iterator = call_value_with_this(iter_method, [], obj)
    let next_method = property_get(iterator, "next")
    if !is_callable(next_method) {
      return throw_type_error("not a function")
    }
    let mut index = 0L
    while true {
      let (done, value) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      try {
        let mapped = if mapfn is Undefined {
          value
        } else {
          call_value_with_this(
            mapfn,
            [value, Number(Int64::to_double(index))],
            this_arg,
          )
        }
        create_data_property_or_throw(result, Int64::to_string(index), mapped)
      } catch {
        err => {
          iterator_close_on_error(iterator)
          raise err
        }
      }
      index = index + 1L
    }
    let _ = property_set(
      result,
      "length",
      Number(Int64::to_double(index)),
      true,
    )
    result
  } else {
    let len = array_like_length_int64(obj)
    let result = if is_constructor_value(this_value) {
      call_constructor(this_value, [Number(Int64::to_double(len))])
    } else {
      let array_proto_value = match current_env() {
        Some(env) =>
          match value_from_object(array_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(array_proto())
          }
        None => value_from_object(array_proto())
      }
      let created = new_array_value_with_proto([], proto=array_proto_value)
      let _ = property_set(
        created,
        "length",
        Number(Int64::to_double(len)),
        true,
      )
      created
    }
    let mut k = 0L
    while k < len {
      let value = property_get(obj, Int64::to_string(k))
      let mapped = if mapfn is Undefined {
        value
      } else {
        call_value_with_this(
          mapfn,
          [value, Number(Int64::to_double(k))],
          this_arg,
        )
      }
      create_data_property_or_throw(result, Int64::to_string(k), mapped)
      k = k + 1L
    }
    let _ = property_set(result, "length", Number(Int64::to_double(len)), true)
    result
  }
}

///|
fn array_of_value(this_value : Value, args : Array[Value]) -> Value raise {
  let len = args.length()
  let result = if is_constructor_value(this_value) {
    call_constructor(this_value, [Number(Double::from_int(len))])
  } else {
    new_array_value([])
  }
  let mut i = 0
  while i < len {
    create_data_property_or_throw(result, Int::to_string(i), args[i])
    i = i + 1
  }
  let _ = property_set(result, "length", Number(Double::from_int(len)), true)
  result
}
