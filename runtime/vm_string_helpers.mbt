///|
fn to_uint16_from_double(num : Double) -> Int {
  let masked = UInt::reinterpret_as_int(to_uint32(num)) & 0xffff
  masked
}

///|
fn string_index_of(value : String, search : String, from : Int) -> Int {
  let len = value.length()
  let search_len = search.length()
  if search_len == 0 {
    return if from > len { len } else { from }
  }
  if search_len > len {
    return -1
  }
  let mut i = from
  let last = len - search_len
  while i <= last {
    let slice = value.unsafe_substring(start=i, end=i + search_len)
    if slice == search {
      return i
    }
    i = i + 1
  }
  -1
}

///|
fn string_last_index_of(value : String, search : String, from : Int) -> Int {
  let len = value.length()
  let search_len = search.length()
  if search_len == 0 {
    return if from > len { len } else { from }
  }
  if search_len > len {
    return -1
  }
  let mut i = from
  if i > len - search_len {
    i = len - search_len
  }
  while i >= 0 {
    let slice = value.unsafe_substring(start=i, end=i + search_len)
    if slice == search {
      return i
    }
    if i == 0 {
      break
    }
    i = i - 1
  }
  -1
}

///|
fn string_split_value(
  value : String,
  args : Array[Value],
  limit : UInt,
) -> Value raise {
  let result : Array[Value?] = []
  let mut count : UInt = 0U
  if args.is_empty() || args[0] is Undefined {
    if limit == 0U {
      return new_array_value([])
    }
    if count < limit {
      result.push(Some(String(value)))
      count = count + 1U
    }
    return new_array_value(result)
  }
  let sep = to_string_strict(args[0])
  if limit == 0U {
    return new_array_value([])
  }
  if sep.is_empty() {
    let len = value.length()
    let mut i = 0
    while i < len {
      if count >= limit {
        break
      }
      result.push(Some(String(value.unsafe_substring(start=i, end=i + 1))))
      count = count + 1U
      i = i + 1
    }
    return new_array_value(result)
  }
  let sep_len = sep.length()
  let mut start = 0
  let len = value.length()
  while start <= len {
    if count >= limit {
      break
    }
    let index = string_index_of(value, sep, start)
    if index < 0 {
      if count < limit {
        result.push(Some(String(value.unsafe_substring(start~, end=len))))
        count = count + 1U
      }
      break
    }
    if count < limit {
      result.push(Some(String(value.unsafe_substring(start~, end=index))))
      count = count + 1U
    } else {
      break
    }
    start = index + sep_len
    if start == len {
      if count < limit {
        result.push(Some(String("")))
        count = count + 1U
      }
      break
    }
  }
  new_array_value(result)
}

///|
fn add_string_length(base : Int, extra : Int) -> Int raise {
  if extra < 0 || base > js_string_len_max - extra {
    let _ = throw_range_error("invalid string length")

  }
  base + extra
}

///|
fn string_pad_start(value : String, target_len : Int, fill : String) -> String {
  let len = value.length()
  if target_len <= len || fill.is_empty() {
    return value
  }
  let fill_len = fill.length()
  if fill_len == 0 {
    return value
  }
  let needed = target_len - len
  let repeat_count = needed / fill_len
  let extra = needed % fill_len
  let mut padding = fill.repeat(repeat_count)
  if extra > 0 {
    padding = padding + fill.unsafe_substring(start=0, end=extra)
  }
  padding + value
}

///|
fn string_pad_end(value : String, target_len : Int, fill : String) -> String {
  let len = value.length()
  if target_len <= len || fill.is_empty() {
    return value
  }
  let fill_len = fill.length()
  if fill_len == 0 {
    return value
  }
  let needed = target_len - len
  let repeat_count = needed / fill_len
  let extra = needed % fill_len
  let mut padding = fill.repeat(repeat_count)
  if extra > 0 {
    padding = padding + fill.unsafe_substring(start=0, end=extra)
  }
  value + padding
}

///|
fn string_create_html(
  this_value : Value?,
  args : Array[Value],
  tag : String,
  attr : String?,
) -> Value raise {
  let text = this_to_string_coerce(this_value)
  let result = StringBuilder::new()
  result.write_string("<")
  result.write_string(tag)
  match attr {
    Some(name) => {
      result.write_string(" ")
      result.write_string(name)
      result.write_string("=\"")
      let attr_value = if args.is_empty() { Undefined } else { args[0] }
      let attr_text = this_to_string_coerce(Some(attr_value))
      for ch in attr_text {
        if ch == '"' {
          result.write_string("&quot;")
        } else {
          result.write_char(ch)
        }
      }
      result.write_string("\"")
    }
    None => ()
  }
  result.write_string(">")
  result.write_string(text)
  result.write_string("</")
  result.write_string(tag)
  result.write_string(">")
  String(result.to_string())
}
