///|
fn regexp_exec_method(rx : Value, input : Value) -> Value raise {
  let exec_method = property_get(rx, "exec")
  if is_callable(exec_method) {
    let result = call_value_with_this(exec_method, [input], rx)
    if is_object_like(result) || result is Null {
      return result
    }
    return throw_type_error("RegExp exec method must return an object or null")
  }
  match regexp_object_from_value(rx) {
    Some((obj, data)) => {
      let text = to_string_strict(input)
      regexp_exec_value(obj, data, text)
    }
    None => throw_type_error("not a regexp")
  }
}

///|
fn regexp_exec_value(
  obj : ObjectValue,
  data : RegexpData,
  input : String,
) -> Value raise {
  let len = input.length()
  let last_index_value = regexp_last_index(obj)
  let mut exec_data = data
  match obj.regexp_data {
    Some(current) => exec_data = current
    None => ()
  }
  let use_last_index = exec_data.global || exec_data.sticky
  let mut last_index = if use_last_index { last_index_value } else { 0L }
  if use_last_index {
    let len64 = Int64::from_int(len)
    if last_index > len64 {
      set_regexp_last_index(obj, 0)
      return Null
    }
    if (exec_data.unicode || exec_data.unicode_sets) &&
      !is_valid_unicode_index(input, Int64::to_int(last_index)) {
      set_regexp_last_index(obj, 0)
      last_index = 0L
    }
  }
  let start_index = if use_last_index { Int64::to_int(last_index) } else { 0 }
  match regexp_match_from(exec_data, input, start_index, exec_data.sticky) {
    None => {
      if use_last_index {
        set_regexp_last_index(obj, 0)
      }
      Null
    }
    Some(match_value) => {
      let start = match_value.start
      let end = match_value.end
      let groups = match_value.groups
      let groups_value = build_regexp_groups(
        exec_data.regex.capture_names,
        groups,
      )
      if use_last_index {
        set_regexp_last_index(obj, end)
      }
      let elements : Array[Value?] = []
      for entry in groups {
        match entry {
          Some(text) => elements.push(Some(String(text)))
          None => elements.push(None)
        }
      }
      let result = new_array_value(elements)
      match result {
        Array(arr) => {
          props_set(arr.props, "index", property_data(Number(Double::from_int(start))))
          props_set(arr.props, "input", property_data(String(input)))
          props_set(arr.props, "groups", property_data(groups_value))
          if exec_data.has_indices {
            let indices = build_regexp_indices(
              start,
              end,
              match_value.indices,
              exec_data.regex.capture_names,
            )
            props_set(arr.props, "indices", property_data(indices))
          }
        }
        _ => ()
      }
      result
    }
  }
}

///|
fn regexp_match_from(
  data : RegexpData,
  input : String,
  start : Int,
  sticky : Bool,
) -> RegexpMatch? {
  regexp_match_from_program(data.regex, input, start, sticky)
}

///|
fn has_named_captures(capture_names : Array[String?]) -> Bool {
  for name in capture_names {
    if name is Some(_) {
      return true
    }
  }
  false
}

///|
fn build_regexp_groups(
  capture_names : Array[String?],
  groups : Array[String?],
) -> Value {
  if !has_named_captures(capture_names) {
    return Undefined
  }
  let groups_value = new_object_value_with_proto(None)
  let groups_obj = match groups_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  let mut i = 0
  while i < capture_names.length() {
    match capture_names[i] {
      Some(name) => {
        let value = match groups.get(i + 1) {
          Some(Some(text)) => String(text)
          _ => Undefined
        }
        if !(value is Undefined) || !groups_obj.props.contains(name) {
          props_set(groups_obj.props, name, property_data(value))
        }
      }
      None => ()
    }
    i = i + 1
  }
  Object(groups_obj)
}

///|
fn build_regexp_indices(
  start : Int,
  end : Int,
  group_indices : Array[CaptureSpan?],
  capture_names : Array[String?],
) -> Value {
  let values : Array[Value?] = []
  let use_groups = has_named_captures(capture_names)
  let mut groups_value = Undefined
  let mut groups_obj : ObjectValue? = None
  if use_groups {
    groups_value = new_object_value_with_proto(None)
    groups_obj = match groups_value {
      Object(obj) => Some(obj)
      _ => None
    }
  }
  let pair : Array[Value?] = [
    Some(Number(Double::from_int(start))),
    Some(Number(Double::from_int(end))),
  ]
  values.push(Some(new_array_value(pair)))
  let mut i = 0
  for entry in group_indices {
    let val = match entry {
      Some(span) =>
        new_array_value([
          Some(Number(Double::from_int(span.start))),
          Some(Number(Double::from_int(span.end))),
        ])
      None => Undefined
    }
    if val is Undefined {
      values.push(None)
    } else {
      values.push(Some(val))
    }
    match (groups_obj, capture_names.get(i)) {
      (Some(obj), Some(Some(name))) =>
        if !(val is Undefined) || !obj.props.contains(name) {
          props_set(obj.props, name, property_data(val))
        }
      _ => ()
    }
    i = i + 1
  }
  let indices_value = new_array_value(values)
  match indices_value {
    Array(arr) => props_set(arr.props, "groups", property_data(groups_value))
    _ => ()
  }
  indices_value
}

///|
fn regexp_last_index(obj : ObjectValue) -> Int64 raise {
  let value = property_get(Value::Object(obj), "lastIndex")
  to_length_int64(to_number(value))
}

///|
fn set_regexp_last_index(obj : ObjectValue, index : Int) -> Unit raise {
  let _ = property_set(
    Value::Object(obj),
    "lastIndex",
    Number(Double::from_int(index)),
    true,
  )

}
