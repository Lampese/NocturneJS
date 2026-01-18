///|
fn object_builtin_tag(value : Value) -> String raise {
  match value {
    Undefined => "Undefined"
    Null => "Null"
    Bool(_) => "Boolean"
    Number(_) => "Number"
    String(_) => "String"
    BigInt(_) => "BigInt"
    Symbol(_) => "Symbol"
    Function(_) => "Function"
    Builtin(_) => "Function"
    BoundFunction(_) => "Function"
    Array(_) => "Array"
    Arguments(_) => "Arguments"
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          proxy_check_revoked(data)
          match data.target {
            Function(_) => "Function"
            _ =>
              if is_array_value(data.target) {
                "Array"
              } else if is_callable(data.target) {
                "Function"
              } else {
                "Object"
              }
          }
        }
        None =>
          if is_array_proto_object(obj) {
            "Array"
          } else if obj.string_data is Some(_) {
            "String"
          } else if obj.number_data is Some(_) {
            "Number"
          } else if obj.bool_data is Some(_) {
            "Boolean"
          } else if obj.regexp_data is Some(_) {
            "RegExp"
          } else if obj.date_data is Some(_) {
            "Date"
          } else if obj.is_error {
            "Error"
          } else {
            "Object"
          }
      }
  }
}

///|
fn function_to_string_value(value : Value) -> String raise {
  if !is_callable(value) {
    let _ = throw_type_error("not a function")
    return ""
  }
  match value {
    Function(func) =>
      match func.source {
        Some(source) => return source
        None => ()
      }
    _ => ()
  }
  let mut prefix = "function "
  match value {
    Function(func) =>
      if func.is_async && func.is_generator {
        prefix = "async function *"
      } else if func.is_async {
        prefix = "async function "
      } else if func.is_generator {
        prefix = "function *"
      }
    _ => ()
  }
  let name_value = property_get(value, "name")
  let name = match name_value {
    String(value) => value
    _ => ""
  }
  prefix + name + "() {\n    [native code]\n}"
}

///|
fn object_to_string(value : Value) -> String raise {
  match value {
    Undefined => "[object Undefined]"
    Null => "[object Null]"
    _ => {
      let obj = to_object(value)
      let mut tag = object_builtin_tag(obj)
      match symbol_to_string_tag_key() {
        Some(key) =>
          match property_get(obj, key) {
            String(value) => tag = value
            _ => ()
          }
        None => ()
      }
      "[object " + tag + "]"
    }
  }
}
