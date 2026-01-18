///|
fn new_array_iterator(obj : Value, kind : Int) -> Value {
  let iterator = new_object_value_with_proto(
    value_from_object(array_iterator_proto()),
  )
  match iterator {
    Object(iter_obj) => {
      iter_obj.props.set(array_iter_target_key, property_data_non_enum(obj))
      iter_obj.props.set(
        array_iter_index_key,
        property_data_non_enum(Number(0.0)),
      )
      iter_obj.props.set(
        array_iter_kind_key,
        property_data_non_enum(Number(Double::from_int(kind))),
      )
    }
    _ => ()
  }
  iterator
}

///|
fn iterator_internal_value(
  iter_obj : ObjectValue,
  key : String,
  error_msg : String,
) -> (Value, Bool) raise {
  let desc = get_own_property_descriptor(Value::Object(iter_obj), key)
  match desc {
    Undefined => {
      let _ = throw_type_error(error_msg)
      (Undefined, false)
    }
    _ =>
      if descriptor_is_accessor(desc) {
        let _ = throw_type_error(error_msg)
        (Undefined, false)
      } else {
        (property_get(desc, "value"), true)
      }
  }
}

///|
fn array_iterator_next(this_value : Value?) -> Value raise {
  let iter_obj = match this_value {
    Some(Object(obj)) => obj
    _ => {
      let _ = throw_type_error("not an array iterator")
      return iterator_result(Undefined, true)
    }
  }
  let (target, ok) = iterator_internal_value(
    iter_obj, array_iter_target_key, "not an array iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let (index_value, ok) = iterator_internal_value(
    iter_obj, array_iter_index_key, "not an array iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let (kind_value, ok) = iterator_internal_value(
    iter_obj, array_iter_kind_key, "not an array iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  if is_nullish(target) {
    return iterator_result(Undefined, true)
  }
  let index = to_int32(to_number(index_value))
  let kind = to_int32(to_number(kind_value))
  let obj = to_object(target)
  let mut typed_len : Int? = None
  match obj {
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) => {
          if typed_array_is_oob(data) {
            return throw_type_error("ArrayBuffer is detached or resized")
          }
          typed_len = Some(typed_array_effective_length(data))
        }
        None => ()
      }
    _ => ()
  }
  let len = match typed_len {
    Some(value) => value
    None => array_like_length(obj)
  }
  if index < 0 || index >= len {
    iter_obj.props.set(array_iter_target_key, property_data_non_enum(Undefined))
    return iterator_result(Undefined, true)
  }
  let key = Number(Double::from_int(index))
  let value = property_get(obj, Int::to_string(index))
  let result = if kind == 0 {
    key
  } else if kind == 1 {
    value
  } else {
    new_array_value([Some(key), Some(value)])
  }
  iter_obj.props.set(
    array_iter_index_key,
    property_data_non_enum(Number(Double::from_int(index + 1))),
  )
  iterator_result(result, false)
}
