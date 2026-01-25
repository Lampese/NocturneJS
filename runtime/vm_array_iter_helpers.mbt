///|
fn array_iterate(
  obj : Value,
  callback : Value,
  this_arg : Value,
  kind : ArrayIterKind,
) -> Value raise {
  let len = array_like_length_int64(obj)
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let result = match kind {
    ArrayIterKind::Map => array_species_create64(obj, len)
    ArrayIterKind::Filter => array_species_create64(obj, 0L)
    _ => Undefined
  }
  let mut out_index = 0L
  let mut k = 0L
  let mut fast_array : ArrayValue? = None
  match obj {
    Array(arr) =>
      if arr.typed_array_data is None {
        fast_array = Some(arr)
      }
    _ => ()
  }
  while k < len {
    let key = Int64::to_string(k)
    let mut present = false
    let mut value = Undefined
    match fast_array {
      Some(arr) => {
        if props_contains(arr.props, key) {
          present = true
          value = property_get(obj, key)
        } else {
          let elements_len = Int64::from_int(arr.elements.length())
          if k < elements_len {
            let index = Int64::to_int(k)
            match arr.elements[index] {
              Some(found) => {
                present = true
                value = found
              }
              None => ()
            }
          }
          if !present && has_property_value(obj, key) {
            present = true
            value = property_get(obj, key)
          }
        }
      }
      None =>
        if has_property_value(obj, key) {
          present = true
          value = property_get(obj, key)
        }
    }
    if present {
      let res = call_value_with_this(
        callback,
        [value, Number(Int64::to_double(k)), obj],
        this_arg,
      )
      match kind {
        ArrayIterKind::ForEach => ()
        ArrayIterKind::Map => create_data_property_or_throw(result, key, res)
        ArrayIterKind::Filter =>
          if is_truthy(res) {
            create_data_property_or_throw(
              result,
              Int64::to_string(out_index),
              value,
            )
            out_index = out_index + 1
          }
        ArrayIterKind::Every => if !is_truthy(res) { return Bool(false) }
        ArrayIterKind::SomeMatch => if is_truthy(res) { return Bool(true) }
      }
    }
    k = k + 1
  }
  match kind {
    ArrayIterKind::Every => Bool(true)
    ArrayIterKind::SomeMatch => Bool(false)
    _ => result
  }
}

///|
fn array_reduce_impl(
  obj : Value,
  callback : Value,
  initial : Value?,
  from_right : Bool,
) -> Value raise {
  let len = array_like_length_int64(obj)
  if !is_callable(callback) {
    return throw_type_error("not a function")
  }
  let mut k = 0L
  let mut acc : Value? = initial
  if acc is None {
    for {
      if k >= len {
        return throw_type_error("Reduce of empty array with no initial value")
      }
      let k1 = if from_right { len - k - 1L } else { k }
      k = k + 1L
      let key = Int64::to_string(k1)
      if has_property_value(obj, key) {
        acc = Some(property_get(obj, key))
        break
      }
    }
  }
  let mut current = match acc {
    Some(value) => value
    None => Undefined
  }
  while k < len {
    let k1 = if from_right { len - k - 1L } else { k }
    let key = Int64::to_string(k1)
    if has_property_value(obj, key) {
      let value = property_get(obj, key)
      current = call_value_with_this(
        callback,
        [current, value, Number(Int64::to_double(k1)), obj],
        Undefined,
      )
    }
    k = k + 1L
  }
  current
}
