///|
fn new_string_iterator(value : Value) -> Value {
  let iterator = new_object_value_with_proto(
    value_from_object(string_iterator_proto()),
  )
  match iterator {
    Object(iter_obj) => {
      props_set(iter_obj.props, string_iter_target_key, property_data_non_enum(value))
      props_set(iter_obj.props, 
        string_iter_index_key,
        property_data_non_enum(Number(0.0)),
      )
    }
    _ => ()
  }
  iterator
}

///|
fn string_iterator_next(this_value : Value?) -> Value raise {
  let iter_obj = match this_value {
    Some(Object(obj)) => obj
    _ => {
      let _ = throw_type_error("not a string iterator")
      return iterator_result(Undefined, true)
    }
  }
  let (target, ok) = iterator_internal_value(
    iter_obj, string_iter_target_key, "not a string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  if is_nullish(target) {
    return iterator_result(Undefined, true)
  }
  let (index_value, ok) = iterator_internal_value(
    iter_obj, string_iter_index_key, "not a string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let index = to_int32(to_number(index_value))
  let text = match target {
    String(value) => value
    _ => to_string_value(target)
  }
  let len = text.length()
  if index < 0 || index >= len {
    props_set(iter_obj.props, 
      string_iter_target_key,
      property_data_non_enum(Undefined),
    )
    return iterator_result(Undefined, true)
  }
  match text.get_char(index) {
    Some(ch) => {
      let next_index = index + ch.utf16_len()
      props_set(iter_obj.props, 
        string_iter_index_key,
        property_data_non_enum(Number(Double::from_int(next_index))),
      )
      iterator_result(String(ch.to_string()), false)
    }
    None => {
      let next_index = if index + 1 <= len { index + 1 } else { len }
      let part = text.unsafe_substring(start=index, end=next_index)
      props_set(iter_obj.props, 
        string_iter_index_key,
        property_data_non_enum(Number(Double::from_int(next_index))),
      )
      iterator_result(String(part), false)
    }
  }
}

///|
fn new_regexp_string_iterator(
  regexp : Value,
  input : String,
  global : Bool,
  unicode : Bool,
) -> Value {
  let iterator = new_object_value_with_proto(
    value_from_object(regexp_string_iterator_proto()),
  )
  match iterator {
    Object(iter_obj) => {
      props_set(iter_obj.props, regexp_iter_regexp_key, property_data_non_enum(regexp))
      props_set(iter_obj.props, 
        regexp_iter_string_key,
        property_data_non_enum(String(input)),
      )
      props_set(iter_obj.props, 
        regexp_iter_global_key,
        property_data_non_enum(Bool(global)),
      )
      props_set(iter_obj.props, 
        regexp_iter_unicode_key,
        property_data_non_enum(Bool(unicode)),
      )
      props_set(iter_obj.props, 
        regexp_iter_done_key,
        property_data_non_enum(Bool(false)),
      )
    }
    _ => ()
  }
  iterator
}

///|
fn regexp_string_iterator_next(this_value : Value?) -> Value raise {
  let iter_obj = match this_value {
    Some(Object(obj)) => obj
    _ => {
      let _ = throw_type_error("not a regexp string iterator")
      return iterator_result(Undefined, true)
    }
  }
  let (done_value, ok) = iterator_internal_value(
    iter_obj, regexp_iter_done_key, "not a regexp string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  if is_truthy(done_value) {
    return iterator_result(Undefined, true)
  }
  let (regexp_value, ok) = iterator_internal_value(
    iter_obj, regexp_iter_regexp_key, "not a regexp string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let (input_value, ok) = iterator_internal_value(
    iter_obj, regexp_iter_string_key, "not a regexp string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let (global_value, ok) = iterator_internal_value(
    iter_obj, regexp_iter_global_key, "not a regexp string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let (unicode_value, ok) = iterator_internal_value(
    iter_obj, regexp_iter_unicode_key, "not a regexp string iterator",
  )
  if !ok {
    return iterator_result(Undefined, true)
  }
  let global = is_truthy(global_value)
  let unicode = is_truthy(unicode_value)
  let input = to_string_strict(input_value)
  let match_value = regexp_exec_method(regexp_value, String(input))
  match match_value {
    Null => {
      props_set(iter_obj.props, 
        regexp_iter_done_key,
        property_data_non_enum(Bool(true)),
      )
      iterator_result(Undefined, true)
    }
    _ => {
      if global {
        let matched = to_string_strict(property_get(match_value, "0"))
        if matched.is_empty() {
          let last_index_value = property_get(regexp_value, "lastIndex")
          let last_index = to_length_int64(to_number(last_index_value))
          let next_index = advance_string_index_int64(
            input, last_index, unicode,
          )
          let _ = property_set(
            regexp_value,
            "lastIndex",
            Number(Int64::to_double(next_index)),
            true,
          )

        }
      } else {
        props_set(iter_obj.props, 
          regexp_iter_done_key,
          property_data_non_enum(Bool(true)),
        )
      }
      iterator_result(match_value, false)
    }
  }
}
