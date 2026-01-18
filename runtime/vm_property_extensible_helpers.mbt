///|
fn is_extensible_value(value : Value) -> Bool {
  match value {
    Object(obj) => obj.extensible
    Function(func) => func.extensible
    BoundFunction(bound) => bound.extensible
    Builtin(builtin) => builtin.extensible
    Array(arr) => arr.extensible
    Arguments(args) => args.extensible
    _ => false
  }
}

///|
fn is_extensible_value_checked(value : Value) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_is_extensible(data)
        None => obj.extensible
      }
    Function(func) => func.extensible
    BoundFunction(bound) => bound.extensible
    Builtin(builtin) => builtin.extensible
    Array(arr) => arr.extensible
    Arguments(args) => args.extensible
    _ => false
  }
}

///|
fn prevent_extensions_value(value : Value) -> Bool raise {
  match value {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => proxy_prevent_extensions(data)
        None => {
          obj.extensible = false
          true
        }
      }
    Function(func) => {
      func.extensible = false
      true
    }
    BoundFunction(bound) => {
      bound.extensible = false
      true
    }
    Builtin(builtin) => {
      builtin.extensible = false
      true
    }
    Array(arr) => {
      match arr.typed_array_data {
        Some(data) =>
          match data.buffer.array_buffer_data {
            Some(buf) =>
              if data.length_tracking || (buf.resizable && !buf.shared) {
                return false
              }
            None => ()
          }
        None => ()
      }
      arr.extensible = false
      true
    }
    Arguments(args) => {
      args.extensible = false
      true
    }
    _ => false
  }
}
