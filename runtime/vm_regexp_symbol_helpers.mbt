///|
fn regexp_symbol_match_value(rx : Value, input : String) -> Value raise {
  if !is_object_like(rx) {
    return throw_type_error("not an object")
  }
  let flags = to_string_strict(property_get(rx, "flags"))
  let global = string_index_of(flags, "g", 0) >= 0
  if !global {
    return regexp_exec_method(rx, String(input))
  }
  let full_unicode = string_index_of(flags, "u", 0) >= 0 ||
    string_index_of(flags, "v", 0) >= 0
  let _ = property_set(rx, "lastIndex", Number(0.0), true)
  let matches : Array[Value?] = []
  while true {
    let result = regexp_exec_method(rx, String(input))
    match result {
      Null => break
      _ => {
        let matched = to_string_strict(property_get(result, "0"))
        matches.push(Some(String(matched)))
        if matched.is_empty() {
          let last_index_value = property_get(rx, "lastIndex")
          let last_index = to_length_int64(to_number(last_index_value))
          let next_index = advance_string_index_int64(
            input, last_index, full_unicode,
          )
          let _ = property_set(
            rx,
            "lastIndex",
            Number(Int64::to_double(next_index)),
            true,
          )

        }
      }
    }
  }
  if matches.is_empty() {
    Null
  } else {
    new_array_value(matches)
  }
}

///|
fn regexp_symbol_match_all_value(rx : Value, input : String) -> Value raise {
  if !is_object_like(rx) {
    return throw_type_error("not an object")
  }
  let ctor = regexp_species_constructor(rx)
  let flags = to_string_strict(property_get(rx, "flags"))
  let matcher = call_constructor(ctor, [rx, String(flags)])
  let last_index_value = property_get(rx, "lastIndex")
  let last_index = to_length_int64(to_number(last_index_value))
  let _ = property_set(
    matcher,
    "lastIndex",
    Number(Int64::to_double(last_index)),
    true,
  )
  let global = string_index_of(flags, "g", 0) >= 0
  let unicode = string_index_of(flags, "u", 0) >= 0 ||
    string_index_of(flags, "v", 0) >= 0
  new_regexp_string_iterator(matcher, input, global, unicode)
}

///|
fn regexp_symbol_search_value(rx : Value, input : String) -> Value raise {
  if !is_object_like(rx) {
    return throw_type_error("not an object")
  }
  let previous_last_index = property_get(rx, "lastIndex")
  if !same_value(previous_last_index, Number(0.0)) {
    let _ = property_set(rx, "lastIndex", Number(0.0), true)

  }
  let result = regexp_exec_method(rx, String(input))
  let current_last_index = property_get(rx, "lastIndex")
  if !same_value(current_last_index, previous_last_index) {
    let _ = property_set(rx, "lastIndex", previous_last_index, true)

  }
  match result {
    Null => Number(-1.0)
    _ => property_get(result, "index")
  }
}

///|
fn regexp_symbol_replace_value(
  rx : Value,
  input : String,
  replace_value : Value,
) -> Value raise {
  if !is_object_like(rx) {
    return throw_type_error("not an object")
  }
  let functional_replace = is_callable(replace_value)
  let replace_str = if functional_replace {
    ""
  } else {
    to_string_strict(replace_value)
  }
  let flags = to_string_strict(property_get(rx, "flags"))
  let is_global = string_index_of(flags, "g", 0) >= 0
  let full_unicode = string_index_of(flags, "u", 0) >= 0 ||
    string_index_of(flags, "v", 0) >= 0
  if is_global {
    let _ = property_set(rx, "lastIndex", Number(0.0), true)

  }
  let results : Array[Value] = []
  while true {
    let result = regexp_exec_method(rx, String(input))
    match result {
      Null => break
      _ => results.push(result)
    }
    if !is_global {
      break
    }
    let matched = to_string_strict(property_get(result, "0"))
    if matched.is_empty() {
      let last_index_value = property_get(rx, "lastIndex")
      let last_index = to_length_int64(to_number(last_index_value))
      let next_index = advance_string_index_int64(
        input, last_index, full_unicode,
      )
      let _ = property_set(
        rx,
        "lastIndex",
        Number(Int64::to_double(next_index)),
        true,
      )

    }
  }
  if results.is_empty() {
    return String(input)
  }
  let mut next_source_position = 0
  let output = StringBuilder::new()
  for result in results {
    let captures_len = array_like_length(result)
    let matched = to_string_strict(property_get(result, "0"))
    let position_value = property_get(result, "index")
    let mut position = to_length_index(
      to_number(position_value),
      input.length(),
    )
    if position < 0 {
      position = 0
    } else if position > input.length() {
      position = input.length()
    }
    let captures : Array[String?] = []
    captures.push(Some(matched))
    let mut i = 1
    while i < captures_len {
      let cap = property_get(result, Int::to_string(i))
      match cap {
        Undefined => captures.push(None)
        _ => captures.push(Some(to_string_strict(cap)))
      }
      i = i + 1
    }
    let named = property_get(result, "groups")
    let replacement = if functional_replace {
      let args : Array[Value] = []
      for capture in captures {
        match capture {
          Some(text) => args.push(String(text))
          None => args.push(Undefined)
        }
      }
      args.push(Number(Double::from_int(position)))
      args.push(String(input))
      if !(named is Undefined) {
        args.push(named)
      }
      let res = call_value_with_this(replace_value, args, Undefined)
      to_string_strict(res)
    } else {
      let named_captures = if named is Undefined {
        Undefined
      } else {
        to_object(named)
      }
      string_get_substitution(
        matched, input, position, captures, named_captures, replace_str,
      )
    }
    if position >= next_source_position {
      output.write_string(
        input.unsafe_substring(start=next_source_position, end=position),
      )
      output.write_string(replacement)
      next_source_position = position + matched.length()
    }
  }
  output.write_string(
    if next_source_position < input.length() {
      input.unsafe_substring(start=next_source_position, end=input.length())
    } else {
      ""
    },
  )
  String(output.to_string())
}

///|
fn regexp_symbol_split_value(
  rx : Value,
  input : String,
  limit_value : Value,
) -> Value raise {
  if !is_object_like(rx) {
    return throw_type_error("not an object")
  }
  let ctor = regexp_species_constructor(rx)
  let mut flags = to_string_strict(property_get(rx, "flags"))
  let unicode = string_index_of(flags, "u", 0) >= 0 ||
    string_index_of(flags, "v", 0) >= 0
  if string_index_of(flags, "y", 0) < 0 {
    flags = flags + "y"
  }
  let splitter = call_constructor(ctor, [rx, String(flags)])
  let results : Array[Value?] = []
  let lim = if limit_value is Undefined {
    0xffffffffU
  } else {
    to_uint32(to_number(limit_value))
  }
  if lim == 0U {
    return new_array_value(results)
  }
  let size = input.length()
  let mut lengthA : UInt = 0U
  if size == 0 {
    let z = regexp_exec_method(splitter, String(input))
    match z {
      Null => {
        results.push(Some(String(input)))
        lengthA = lengthA + 1U
      }
      _ => ()
    }
    return new_array_value(results)
  }
  let mut p = 0
  let mut q = 0
  while q < size {
    let _ = property_set(
      splitter,
      "lastIndex",
      Number(Double::from_int(q)),
      true,
    )
    let z = regexp_exec_method(splitter, String(input))
    match z {
      Null => q = advance_string_index(input, q, unicode)
      _ => {
        let end_value = property_get(splitter, "lastIndex")
        let mut e = to_length_index(to_number(end_value), input.length())
        if e > size {
          e = size
        }
        if e == p {
          q = advance_string_index(input, q, unicode)
        } else {
          let part = input.unsafe_substring(start=p, end=q)
          results.push(Some(String(part)))
          lengthA = lengthA + 1U
          if lengthA == lim {
            return new_array_value(results)
          }
          p = e
          let captures_len = array_like_length(z)
          let mut i = 1
          while i < captures_len {
            let cap = property_get(z, Int::to_string(i))
            results.push(Some(cap))
            lengthA = lengthA + 1U
            if lengthA == lim {
              return new_array_value(results)
            }
            i = i + 1
          }
          q = p
        }
      }
    }
  }
  if p > size {
    p = size
  }
  if lengthA < lim {
    let tail = input.unsafe_substring(start=p, end=size)
    results.push(Some(String(tail)))
  }
  new_array_value(results)
}
