///|
priv struct JsonStringifyState {
  gap : String
  mut indent : String
  mut replacer : Value
  mut property_list : Array[String]?
  stack : Array[Int]
}

///|
fn json_gap_from_space(value : Value) -> String raise {
  match value {
    Number(num) =>
      if Double::is_nan(num) || Double::is_inf(num) {
        ""
      } else {
        let count = Double::to_int(Double::trunc(num))
        let clamped = if count < 0 {
          0
        } else if count > 10 {
          10
        } else {
          count
        }
        " ".repeat(clamped)
      }
    String(s) => {
      let len = s.length()
      let max_len = if len > 10 { 10 } else { len }
      s.unsafe_substring(start=0, end=max_len)
    }
    Object(obj) =>
      match obj.number_data {
        Some(_) => {
          let num = to_number(value)
          if Double::is_nan(num) || Double::is_inf(num) {
            ""
          } else {
            let count = Double::to_int(Double::trunc(num))
            let clamped = if count < 0 {
              0
            } else if count > 10 {
              10
            } else {
              count
            }
            " ".repeat(clamped)
          }
        }
        None =>
          match obj.string_data {
            Some(_) => {
              let text = to_string_strict(value)
              let len = text.length()
              let max_len = if len > 10 { 10 } else { len }
              text.unsafe_substring(start=0, end=max_len)
            }
            None => ""
          }
      }
    _ => ""
  }
}

///|
fn json_replacer_item_name(value : Value) -> String? raise {
  match value {
    String(s) => Some(s)
    Number(num) => Some(to_string_value(Number(num)))
    Object(obj) =>
      if obj.string_data is Some(_) || obj.number_data is Some(_) {
        Some(to_string_strict(value))
      } else {
        None
      }
    _ => None
  }
}

///|
fn json_replacer_property_list(value : Value) -> Array[String] raise {
  let list : Array[String] = []
  let seen : Map[String, Bool] = Map::new()
  let len_value = property_get(value, "length")
  let len = to_length_int64(to_number(len_value))
  let mut i : Int64 = 0
  while i < len {
    let key = Int64::to_string(i)
    let item = property_get(value, key)
    match json_replacer_item_name(item) {
      Some(name) =>
        if !seen.contains(name) {
          seen.set(name, true)
          list.push(name)
        }
      None => ()
    }
    i = i + 1
  }
  list
}

///|
fn json_unwrap_object_value(value : Value) -> Value raise {
  match value {
    Object(obj) =>
      match obj.number_data {
        Some(_) => Number(to_number(value))
        None =>
          match obj.string_data {
            Some(_) => String(to_string_strict(value))
            None =>
              match obj.bool_data {
                Some(flag) => Bool(flag)
                None =>
                  match obj.bigint_data {
                    Some(bigint) => BigInt(bigint)
                    None => value
                  }
              }
          }
      }
    _ => value
  }
}

///|
fn json_number_string(value : Double) -> String {
  if Double::is_nan(value) || Double::is_inf(value) {
    "null"
  } else {
    Double::to_string(value)
  }
}

///|
fn json_stack_contains(stack : Array[Int], id : Int) -> Bool {
  for entry in stack {
    if entry == id {
      return true
    }
  }
  false
}

///|
fn json_stack_push(state : JsonStringifyState, value : Value) -> Bool raise {
  match value_id(value) {
    Some(id) =>
      if json_stack_contains(state.stack, id) {
        let _ = throw_type_error("cyclic object value")
        false
      } else {
        state.stack.push(id)
        true
      }
    None => false
  }
}

///|
fn json_serialize_array(
  value : Value,
  state : JsonStringifyState,
) -> String raise {
  let pushed = json_stack_push(state, value)
  let stepback = state.indent
  state.indent = stepback + state.gap
  let len_value = property_get(value, "length")
  let len = to_length_int64(to_number(len_value))
  let parts : Array[String] = []
  let mut i : Int64 = 0
  while i < len {
    let key = Int64::to_string(i)
    let part = json_serialize_property(key, value, state)
    match part {
      Some(text) => parts.push(text)
      None => parts.push("null")
    }
    i = i + 1
  }
  let result = if parts.is_empty() {
    "[]"
  } else if state.gap.is_empty() {
    "[" + parts.join(",") + "]"
  } else {
    let inner_indent = state.indent
    let outer_indent = stepback
    let formatted : Array[String] = parts.map(fn(part) { inner_indent + part })
    "[\n" + formatted.join(",\n") + "\n" + outer_indent + "]"
  }
  state.indent = stepback
  if pushed {
    let _ = state.stack.pop()

  }
  result
}

///|
fn json_serialize_object(
  value : Value,
  state : JsonStringifyState,
) -> String raise {
  let pushed = json_stack_push(state, value)
  let stepback = state.indent
  state.indent = stepback + state.gap
  let keys = match state.property_list {
    Some(list) => list
    None => own_enumerable_string_keys(value)
  }
  let parts : Array[String] = []
  for key in keys {
    let rendered = json_serialize_property(key, value, state)
    match rendered {
      Some(text) => {
        let name = json_escape(key)
        if state.gap.is_empty() {
          parts.push("\"" + name + "\":" + text)
        } else {
          parts.push("\"" + name + "\": " + text)
        }
      }
      None => ()
    }
  }
  let result = if parts.is_empty() {
    "{}"
  } else if state.gap.is_empty() {
    "{" + parts.join(",") + "}"
  } else {
    let inner_indent = state.indent
    let outer_indent = stepback
    let formatted : Array[String] = parts.map(fn(part) { inner_indent + part })
    "{\n" + formatted.join(",\n") + "\n" + outer_indent + "}"
  }
  state.indent = stepback
  if pushed {
    let _ = state.stack.pop()

  }
  result
}

///|
fn json_serialize_property(
  key : String,
  holder : Value,
  state : JsonStringifyState,
) -> String? raise {
  let mut value = property_get(holder, key)
  if is_object_like(value) || value is BigInt(_) {
    let to_json = property_get(value, "toJSON")
    if is_callable(to_json) {
      value = call_value_with_this(to_json, [String(key)], value)
    }
  }
  if is_callable(state.replacer) {
    value = call_value_with_this(state.replacer, [String(key), value], holder)
  }
  value = json_unwrap_object_value(value)
  match value {
    Undefined => return None
    Symbol(_) => return None
    BigInt(_) => {
      let _ = throw_type_error("cannot convert BigInt to JSON")
      return None
    }
    _ => ()
  }
  if is_callable(value) {
    return None
  }
  match value {
    Null => Some("null")
    Bool(v) => Some(if v { "true" } else { "false" })
    Number(num) => Some(json_number_string(num))
    String(text) => Some("\"" + json_escape(text) + "\"")
    _ =>
      if is_array_value(value) {
        Some(json_serialize_array(value, state))
      } else {
        Some(json_serialize_object(value, state))
      }
  }
}

///|
fn json_stringify_with_replacer(
  value : Value,
  replacer : Value,
  space : Value,
) -> Value raise {
  let gap = json_gap_from_space(space)
  let state = JsonStringifyState::{
    gap,
    indent: "",
    replacer: Undefined,
    property_list: None,
    stack: [],
  }
  if is_callable(replacer) {
    state.replacer = replacer
  } else if is_object_like(replacer) {
    if is_array_value(replacer) {
      state.property_list = Some(json_replacer_property_list(replacer))
    }
  }
  let wrapper = new_object_value()
  create_data_property_or_throw(wrapper, "", value)
  match json_serialize_property("", wrapper, state) {
    Some(text) => String(text)
    None => Undefined
  }
}

///|
fn json_stringify_value(value : Value) -> String raise {
  match value {
    Null => "null"
    Bool(v) => if v { "true" } else { "false" }
    Number(v) =>
      if Double::is_nan(v) || Double::is_inf(v) {
        "null"
      } else {
        Double::to_string(v)
      }
    String(s) => "\"\{json_escape(s)}\""
    Object(obj) => json_stringify_object(obj)
    Array(arr) => json_stringify_array(arr)
    Arguments(args) => json_stringify_arguments(args)
    Function(_) => "null"
    BoundFunction(_) => "null"
    Builtin(_) => "null"
    _ => "null"
  }
}

///|
fn json_indent_prefix(indent : String, level : Int) -> String {
  if indent.is_empty() || level <= 0 {
    ""
  } else {
    indent.repeat(level)
  }
}

///|
fn json_stringify_value_with_indent(
  value : Value,
  indent : String,
  level : Int,
) -> String raise {
  match value {
    Null => "null"
    Bool(v) => if v { "true" } else { "false" }
    Number(v) =>
      if Double::is_nan(v) || Double::is_inf(v) {
        "null"
      } else {
        Double::to_string(v)
      }
    String(s) => "\"\{json_escape(s)}\""
    Object(obj) => json_stringify_object_with_indent(obj, indent, level)
    Array(arr) => json_stringify_array_with_indent(arr, indent, level)
    Arguments(args) => json_stringify_arguments_with_indent(args, indent, level)
    Function(_) => "null"
    BoundFunction(_) => "null"
    Builtin(_) => "null"
    _ => "null"
  }
}

///|
fn json_stringify_array(arr : ArrayValue) -> String raise {
  let parts : Array[String] = []
  let len = arr.elements.length()
  for i = 0; i < len; i = i + 1 {
    let name = Int::to_string(i)
    let mut value_opt : Value? = None
    match props_get(arr.props, name) {
      Some(prop) => value_opt = Some(prop.value)
      None => ()
    }
    match value_opt {
      Some(value) =>
        match value {
          Undefined => parts.push("null")
          Function(_) => parts.push("null")
          Builtin(_) => parts.push("null")
          _ => parts.push(json_stringify_value(value))
        }
      None =>
        match arr.elements[i] {
          None => parts.push("null")
          Some(value) =>
            match value {
              Undefined => parts.push("null")
              Function(_) => parts.push("null")
              Builtin(_) => parts.push("null")
              _ => parts.push(json_stringify_value(value))
            }
        }
    }
  }
  "[" + parts.join(",") + "]"
}

///|
fn json_stringify_array_with_indent(
  arr : ArrayValue,
  indent : String,
  level : Int,
) -> String raise {
  if indent.is_empty() {
    return json_stringify_array(arr)
  }
  let parts : Array[String] = []
  let len = arr.elements.length()
  for i = 0; i < len; i = i + 1 {
    let name = Int::to_string(i)
    let mut value_opt : Value? = None
    match props_get(arr.props, name) {
      Some(prop) => value_opt = Some(prop.value)
      None => ()
    }
    match value_opt {
      Some(value) =>
        match value {
          Undefined => parts.push("null")
          Function(_) => parts.push("null")
          Builtin(_) => parts.push("null")
          _ =>
            parts.push(
              json_stringify_value_with_indent(value, indent, level + 1),
            )
        }
      None =>
        match arr.elements[i] {
          None => parts.push("null")
          Some(value) =>
            match value {
              Undefined => parts.push("null")
              Function(_) => parts.push("null")
              Builtin(_) => parts.push("null")
              _ =>
                parts.push(
                  json_stringify_value_with_indent(value, indent, level + 1),
                )
            }
        }
    }
  }
  if parts.is_empty() {
    return "[]"
  }
  let inner_indent = json_indent_prefix(indent, level + 1)
  let outer_indent = json_indent_prefix(indent, level)
  let formatted : Array[String] = parts.map(fn(part) { inner_indent + part })
  "[\n" + formatted.join(",\n") + "\n" + outer_indent + "]"
}

///|
fn json_stringify_object(obj : ObjectValue) -> String raise {
  let parts : Array[String] = []
  let target = Object(obj)
  let keys = own_enumerable_string_keys(target)
  for key in keys {
    match json_stringify_member(property_get(target, key)) {
      Some(rendered) => parts.push("\"\{json_escape(key)}\":\{rendered}")
      None => ()
    }
  }
  "{" + parts.join(",") + "}"
}

///|
fn json_stringify_object_with_indent(
  obj : ObjectValue,
  indent : String,
  level : Int,
) -> String raise {
  if indent.is_empty() {
    return json_stringify_object(obj)
  }
  let parts : Array[String] = []
  let inner_indent = json_indent_prefix(indent, level + 1)
  let target = Object(obj)
  let keys = own_enumerable_string_keys(target)
  for key in keys {
    match
      json_stringify_member_with_indent(
        property_get(target, key),
        indent,
        level + 1,
      ) {
      Some(rendered) =>
        parts.push(inner_indent + "\"" + json_escape(key) + "\": " + rendered)
      None => ()
    }
  }
  if parts.is_empty() {
    return "{}"
  }
  let outer_indent = json_indent_prefix(indent, level)
  "{\n" + parts.join(",\n") + "\n" + outer_indent + "}"
}

///|
fn json_stringify_arguments(args : ArgumentsValue) -> String raise {
  let parts : Array[String] = []
  let len = args.elements.length()
  for i = 0; i < len; i = i + 1 {
    if arguments_has_index(args, i) {
      let value = arguments_index_value(args, i)
      match json_stringify_member(value) {
        Some(rendered) => parts.push("\"\{Int::to_string(i)}\":\{rendered}")
        None => ()
      }
    }
  }
  props_iter_raise(args.props, fn(key, prop) raise {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if !prop.enumerable {
      return ()
    }
    match parse_array_index(key) {
      Some(index) =>
        if index >= 0 && index < len {
          ()
        } else {
          match json_stringify_member(prop.value) {
            Some(rendered) => parts.push("\"\{json_escape(key)}\":\{rendered}")
            None => ()
          }
        }
      None =>
        match json_stringify_member(prop.value) {
          Some(rendered) => parts.push("\"\{json_escape(key)}\":\{rendered}")
          None => ()
        }
    }
  })
  "{" + parts.join(",") + "}"
}

///|
fn json_stringify_arguments_with_indent(
  args : ArgumentsValue,
  indent : String,
  level : Int,
) -> String raise {
  if indent.is_empty() {
    return json_stringify_arguments(args)
  }
  let parts : Array[String] = []
  let len = args.elements.length()
  let inner_indent = json_indent_prefix(indent, level + 1)
  for i = 0; i < len; i = i + 1 {
    if arguments_has_index(args, i) {
      let value = arguments_index_value(args, i)
      match json_stringify_member_with_indent(value, indent, level + 1) {
        Some(rendered) =>
          parts.push(
            inner_indent + "\"" + Int::to_string(i) + "\": " + rendered,
          )
        None => ()
      }
    }
  }
  props_iter_raise(args.props, fn(key, prop) raise {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if !prop.enumerable {
      return ()
    }
    match parse_array_index(key) {
      Some(index) =>
        if index >= 0 && index < len {
          ()
        } else {
          match
            json_stringify_member_with_indent(prop.value, indent, level + 1) {
            Some(rendered) =>
              parts.push(
                inner_indent + "\"" + json_escape(key) + "\": " + rendered,
              )
            None => ()
          }
        }
      None =>
        match json_stringify_member_with_indent(prop.value, indent, level + 1) {
          Some(rendered) =>
            parts.push(
              inner_indent + "\"" + json_escape(key) + "\": " + rendered,
            )
          None => ()
        }
    }
  })
  if parts.is_empty() {
    return "{}"
  }
  let outer_indent = json_indent_prefix(indent, level)
  "{\n" + parts.join(",\n") + "\n" + outer_indent + "}"
}

///|
fn json_stringify_member(value : Value) -> String? raise {
  match value {
    Undefined => None
    Function(_) => None
    BoundFunction(_) => None
    Builtin(_) => None
    Symbol(_) => None
    _ => Some(json_stringify_value(value))
  }
}

///|
fn json_stringify_member_with_indent(
  value : Value,
  indent : String,
  level : Int,
) -> String? raise {
  match value {
    Undefined => None
    Function(_) => None
    BoundFunction(_) => None
    Builtin(_) => None
    Symbol(_) => None
    _ => Some(json_stringify_value_with_indent(value, indent, level))
  }
}

///|
fn json_escape(value : String) -> String {
  let sb = StringBuilder::new()
  let len = value.length()
  let mut i = 0
  while i < len {
    let unit = UInt16::to_int(value.code_unit_at(i))
    if unit == 0x22 {
      sb.write_string("\\\"")
      i = i + 1
      continue
    }
    if unit == 0x5c {
      sb.write_string("\\\\")
      i = i + 1
      continue
    }
    if unit == 0x08 {
      sb.write_string("\\b")
      i = i + 1
      continue
    }
    if unit == 0x0c {
      sb.write_string("\\f")
      i = i + 1
      continue
    }
    if unit == 0x0a {
      sb.write_string("\\n")
      i = i + 1
      continue
    }
    if unit == 0x0d {
      sb.write_string("\\r")
      i = i + 1
      continue
    }
    if unit == 0x09 {
      sb.write_string("\\t")
      i = i + 1
      continue
    }
    if is_lead_surrogate(unit) {
      if i + 1 < len {
        let next = UInt16::to_int(value.code_unit_at(i + 1))
        if is_trail_surrogate(next) {
          sb.write_string(value.unsafe_substring(start=i, end=i + 2))
          i = i + 2
          continue
        }
      }
      sb.write_string(unicode_escape(unit))
      i = i + 1
      continue
    }
    if is_trail_surrogate(unit) {
      sb.write_string(unicode_escape(unit))
      i = i + 1
      continue
    }
    if unit < 0x20 {
      sb.write_string(unicode_escape(unit))
      i = i + 1
      continue
    }
    sb.write_string(value.unsafe_substring(start=i, end=i + 1))
    i = i + 1
  }
  sb.to_string()
}

///|
fn unicode_escape(code : Int) -> String {
  let sb = StringBuilder::new()
  sb.write_string("\\u")
  sb.write_char(hex_digit((code >> 12) & 0x0f))
  sb.write_char(hex_digit((code >> 8) & 0x0f))
  sb.write_char(hex_digit((code >> 4) & 0x0f))
  sb.write_char(hex_digit(code & 0x0f))
  sb.to_string()
}
