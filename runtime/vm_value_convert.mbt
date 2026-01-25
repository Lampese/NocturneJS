///|
fn to_object(value : Value) -> Value raise {
  let env = current_env()
  match value {
    Object(_) => value
    Function(_) => value
    BoundFunction(_) => value
    Builtin(_) => value
    Array(_) => value
    Arguments(_) => value
    String(s) => {
      let proto = match env {
        Some(env) =>
          match value_from_object(string_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(string_proto())
          }
        None => value_from_object(string_proto())
      }
      new_string_object(s, proto~)
    }
    Number(n) => {
      let proto = match env {
        Some(env) =>
          match value_from_object(number_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(number_proto())
          }
        None => value_from_object(number_proto())
      }
      new_number_object(n, proto~)
    }
    Bool(b) => {
      let proto = match env {
        Some(env) =>
          match value_from_object(bool_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(bool_proto())
          }
        None => value_from_object(bool_proto())
      }
      new_bool_object(b, proto~)
    }
    BigInt(b) => {
      let proto = match env {
        Some(env) =>
          match value_from_object(bigint_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(bigint_proto())
          }
        None => value_from_object(bigint_proto())
      }
      new_bigint_object(b, proto~)
    }
    Symbol(symbol) => {
      let proto = match env {
        Some(env) =>
          match value_from_object(symbol_proto_for_env(env)) {
            Some(value) => Some(value)
            None => value_from_object(symbol_proto())
          }
        None => value_from_object(symbol_proto())
      }
      new_symbol_object(symbol, proto~)
    }
    Null => throw_type_error("cannot convert to object")
    Undefined => throw_type_error("cannot convert to object")
  }
}

///|
fn string_array_value(items : Array[String]) -> Value {
  let elements : Array[Value?] = []
  for item in items {
    elements.push(Some(String(item)))
  }
  new_array_value(elements)
}

///|
fn value_array_to_strings(value : Value) -> Array[String] raise {
  let arr = require_array_buffer_array(value)
  let items : Array[String] = []
  let mut i = 0
  while i < arr.elements.length() {
    let text = match arr.elements[i] {
      Some(v) => to_string_value(v)
      None => ""
    }
    items.push(text)
    i = i + 1
  }
  items
}

///|
fn call_primitive_method(value : Value, name : String) -> Value? raise {
  let method_value = property_get(value, name)
  if is_callable(method_value) {
    let result = call_value_with_this(method_value, [], value)
    if !is_object_like(result) {
      return Some(result)
    }
  }
  None
}

///|
fn to_primitive(value : Value, prefer_string : Bool) -> Value raise {
  if !is_object_like(value) {
    return value
  }
  match symbol_to_primitive_key() {
    Some(key) =>
      match property_get(value, key) {
        Undefined | Null => ()
        method_value => {
          let hint = if prefer_string { "string" } else { "number" }
          let result = call_value_with_this(method_value, [String(hint)], value)
          if !is_object_like(result) {
            return result
          }
          let _ = throw_type_error("toPrimitive")
          return Undefined
        }
      }
    None => ()
  }
  let first = if prefer_string { "toString" } else { "valueOf" }
  let second = if prefer_string { "valueOf" } else { "toString" }
  match call_primitive_method(value, first) {
    Some(result) => return result
    None => ()
  }
  match call_primitive_method(value, second) {
    Some(result) => return result
    None => ()
  }
  let _ = throw_type_error("cannot convert object to primitive")
  Undefined
}

///|
fn to_primitive_default(value : Value) -> Value raise {
  if !is_object_like(value) {
    return value
  }
  match symbol_to_primitive_key() {
    Some(key) =>
      match property_get(value, key) {
        Undefined | Null => ()
        method_value => {
          let result = call_value_with_this(
            method_value,
            [String("default")],
            value,
          )
          if !is_object_like(result) {
            return result
          }
          let _ = throw_type_error("toPrimitive")
          return Undefined
        }
      }
    None => ()
  }
  match call_primitive_method(value, "valueOf") {
    Some(result) => return result
    None => ()
  }
  match call_primitive_method(value, "toString") {
    Some(result) => return result
    None => ()
  }
  let _ = throw_type_error("cannot convert object to primitive")
  Undefined
}

///|
fn to_number(value : Value) -> Double raise {
  match value {
    Number(v) => v
    Bool(v) => if v { 1.0 } else { 0.0 }
    Null => 0.0
    Undefined => nan()
    String(s) => string_to_number(s)
    BigInt(_) => {
      let _ = throw_type_error("cannot convert bigint to number")
      nan()
    }
    Symbol(_) => {
      let _ = throw_type_error("cannot convert symbol to number")
      nan()
    }
    Object(_)
    | Array(_)
    | Builtin(_)
    | Function(_)
    | BoundFunction(_)
    | Arguments(_) => {
      let prim = to_primitive(value, false)
      to_number(prim)
    }
  }
}

///|
fn to_numeric(value : Value) -> Value raise {
  let prim = if is_object_like(value) {
    to_primitive(value, false)
  } else {
    value
  }
  match prim {
    BigInt(_) => prim
    _ => Number(to_number(prim))
  }
}

///|
fn to_string_value(value : Value) -> String {
  match value {
    String(s) => s
    Bool(v) => if v { "true" } else { "false" }
    Null => "null"
    Undefined => "undefined"
    Number(v) => Double::to_string(v)
    BigInt(v) => v.to_string()
    Symbol(symbol) => symbol_to_string(symbol)
    Builtin(builtin) => "function \{builtin_name(builtin.kind)}"
    Function(func) =>
      match func.name {
        Some(name) => "function \{name}"
        None => "function"
      }
    BoundFunction(bound) =>
      match props_get(bound.props, "name") {
        Some(prop) =>
          match prop.value {
            String(name) =>
              if name.is_empty() {
                "function"
              } else {
                "function \{name}"
              }
            _ => "function"
          }
        None => "function"
      }
    Arguments(_) | Object(_) =>
      try {
        let prim = to_primitive(value, true)
        to_string_value(prim)
      } catch {
        _ => "[object Object]"
      }
    Array(arr) => array_to_string(arr)
  }
}

///|
fn to_string_strict(value : Value) -> String raise {
  if is_object_like(value) {
    let prim = to_primitive(value, true)
    return to_string_strict(prim)
  }
  match value {
    Symbol(_) => {
      let _ = throw_type_error("cannot convert symbol to string")
      ""
    }
    _ => to_string_value(value)
  }
}

///|
pub fn value_to_string(value : Value) -> String {
  to_string_value(value)
}

///|
pub fn format_error(value : Value) -> String {
  let stack_value = property_get(value, "stack") catch { _ => Undefined }
  match stack_value {
    String(stack) =>
      if stack.is_empty() {
        to_string_value(value)
      } else {
        stack
      }
    _ => {
      let mut message = to_string_value(value)
      let line_value = property_get(value, "lineNumber") catch {
        _ => Undefined
      }
      let column_value = property_get(value, "columnNumber") catch {
        _ => Undefined
      }
      match line_value {
        Number(line) => {
          message = message + " (line " + Double::to_string(line)
          match column_value {
            Number(column) =>
              message = message + ", column " + Double::to_string(column)
            _ => ()
          }
          message = message + ")"
          message
        }
        _ => message
      }
    }
  }
}

///|
pub fn error_name(value : Value) -> String? {
  match value {
    Object(obj) => {
      fn name_from_props(value : Value) -> String? {
        let name_value = property_get(value, "name") catch { _ => Undefined }
        match name_value {
          String(name) => return Some(name)
          _ => ()
        }
        let ctor_value = property_get(value, "constructor") catch {
          _ => Undefined
        }
        if is_object_like(ctor_value) {
          let ctor_name = property_get(ctor_value, "name") catch {
            _ => Undefined
          }
          match ctor_name {
            String(name) => return Some(name)
            _ => ()
          }
        }
        None
      }

      if !obj.is_error {
        name_from_props(value)
      } else {
        match obj.proto {
          Some(proto) =>
            match value_id(proto) {
              Some(proto_id) => {
                fn proto_matches(proto_opt : ObjectValue?) -> Bool {
                  match proto_opt {
                    Some(proto_obj) => proto_id == proto_obj.id
                    None => false
                  }
                }

                if proto_matches(syntax_error_proto()) {
                  Some("SyntaxError")
                } else if proto_matches(type_error_proto()) {
                  Some("TypeError")
                } else if proto_matches(reference_error_proto()) {
                  Some("ReferenceError")
                } else if proto_matches(range_error_proto()) {
                  Some("RangeError")
                } else if proto_matches(eval_error_proto()) {
                  Some("EvalError")
                } else if proto_matches(uri_error_proto()) {
                  Some("URIError")
                } else if proto_matches(aggregate_error_proto()) {
                  Some("AggregateError")
                } else if proto_matches(error_proto()) {
                  Some("Error")
                } else {
                  name_from_props(value)
                }
              }
              None => name_from_props(value)
            }
          None => name_from_props(value)
        }
      }
    }
    _ => None
  }
}

///|
