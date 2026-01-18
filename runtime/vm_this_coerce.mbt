///|
fn this_to_string(this_value : Value?) -> String raise {
  match this_value {
    Some(String(value)) => value
    Some(Object(obj)) =>
      match obj.string_data {
        Some(value) => value
        None => {
          let _ = throw_type_error("not a string")
          ""
        }
      }
    Some(Null) => {
      let _ = throw_type_error("not a string")
      ""
    }
    Some(Undefined) => {
      let _ = throw_type_error("not a string")
      ""
    }
    Some(_) => {
      let _ = throw_type_error("not a string")
      ""
    }
    None => {
      let _ = throw_type_error("not a string")
      ""
    }
  }
}

///|
fn this_to_string_coerce(this_value : Value?) -> String raise {
  let value = match this_value {
    Some(inner) => inner
    None => Undefined
  }
  match value {
    Null => {
      let _ = throw_type_error("cannot convert to object")
      ""
    }
    Undefined => {
      let _ = throw_type_error("cannot convert to object")
      ""
    }
    _ => to_string_strict(value)
  }
}

///|
fn this_to_number(this_value : Value?) -> Double raise {
  match this_value {
    Some(Number(value)) => value
    Some(Object(obj)) =>
      match obj.number_data {
        Some(value) => value
        None => {
          let _ = throw_type_error("not a number")
          0.0
        }
      }
    Some(_) => {
      let _ = throw_type_error("not a number")
      0.0
    }
    None => {
      let _ = throw_type_error("not a number")
      0.0
    }
  }
}

///|
fn this_to_bool(this_value : Value?) -> Bool raise {
  match this_value {
    Some(Bool(value)) => value
    Some(Object(obj)) =>
      match obj.bool_data {
        Some(value) => value
        None => {
          let _ = throw_type_error("not a boolean")
          false
        }
      }
    Some(_) => {
      let _ = throw_type_error("not a boolean")
      false
    }
    None => {
      let _ = throw_type_error("not a boolean")
      false
    }
  }
}

///|
fn this_to_bigint(this_value : Value?) -> @bigint.BigInt raise {
  match this_value {
    Some(BigInt(value)) => value
    Some(Object(obj)) =>
      match obj.bigint_data {
        Some(value) => value
        None => {
          let _ = throw_type_error("not a BigInt")
          @bigint.BigInt::from_int(0)
        }
      }
    Some(_) => {
      let _ = throw_type_error("not a BigInt")
      @bigint.BigInt::from_int(0)
    }
    None => {
      let _ = throw_type_error("not a BigInt")
      @bigint.BigInt::from_int(0)
    }
  }
}

///|
fn coerce_this(env : Env, value : Value) -> Value {
  match value {
    Undefined => global_object_value(env)
    Null => global_object_value(env)
    String(s) => {
      let proto = match value_from_object(string_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(string_proto())
      }
      new_string_object(s, proto~)
    }
    Number(n) => {
      let proto = match value_from_object(number_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(number_proto())
      }
      new_number_object(n, proto~)
    }
    Bool(b) => {
      let proto = match value_from_object(bool_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(bool_proto())
      }
      new_bool_object(b, proto~)
    }
    BigInt(b) => {
      let proto = match value_from_object(bigint_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(bigint_proto())
      }
      new_bigint_object(b, proto~)
    }
    Symbol(symbol) => {
      let proto = match value_from_object(symbol_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(symbol_proto())
      }
      new_symbol_object(symbol, proto~)
    }
    _ => value
  }
}

///|
fn this_to_symbol(this_value : Value?) -> SymbolValue raise {
  match this_value {
    Some(Symbol(symbol)) => symbol
    Some(Object(obj)) =>
      match obj.symbol_data {
        Some(symbol) => symbol
        None => {
          let _ = throw_type_error("not a symbol")
          SymbolValue::{ id: 0, description: None, key: None }
        }
      }
    Some(_) => {
      let _ = throw_type_error("not a symbol")
      SymbolValue::{ id: 0, description: None, key: None }
    }
    None => {
      let _ = throw_type_error("not a symbol")
      SymbolValue::{ id: 0, description: None, key: None }
    }
  }
}
