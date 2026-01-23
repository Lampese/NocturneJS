///|
fn require_file_fd(this_value : Value?) -> Int raise {
  match this_value {
    Some(Object(obj)) => {
      let fd_value = property_get(Object(obj), "__fd")
      match fd_value {
        Number(num) => to_int32(num)
        _ => {
          let _ = throw_type_error("not a file")
          -1
        }
      }
    }
    _ => {
      let _ = throw_type_error("not a file")
      -1
    }
  }
}

///|
fn new_file_object(fd : Int) -> Value {
  let obj = new_object_value()
  match obj {
    Object(object) => {
      props_set(object.props, 
        "__fd",
        property_data_non_enum(Number(Double::from_int(fd))),
      )
      props_set(object.props, 
        "puts",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FilePuts)),
      )
      props_set(object.props, 
        "putByte",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FilePutByte)),
      )
      props_set(object.props, 
        "seek",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileSeek)),
      )
      props_set(object.props, 
        "tell",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileTell)),
      )
      props_set(object.props, 
        "read",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileRead)),
      )
      props_set(object.props, 
        "readAsString",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::FileReadAsString),
        ),
      )
      props_set(object.props, 
        "getByte",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileGetByte)),
      )
      props_set(object.props, 
        "getline",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileGetline)),
      )
      props_set(object.props, 
        "eof",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileEof)),
      )
      props_set(object.props, 
        "close",
        property_data_non_enum(new_builtin_value(BuiltinFunction::FileClose)),
      )
    }
    _ => ()
  }
  obj
}

///|
fn resolve_path(path : String) -> String {
  if path.is_empty() {
    return path
  }
  match @env.current_dir() {
    Some(cwd) => {
      let target = @path.Path(path)
      if target.is_absolute() {
        target.resolve().to_string()
      } else {
        let base = @path.Path(cwd)
        base.join(target).resolve().to_string()
      }
    }
    None => path
  }
}

///|
fn resolve_path_from_source(path : String) -> String {
  if path.is_empty() {
    return path
  }
  let target = @path.Path(path)
  if target.is_absolute() {
    return target.resolve().to_string()
  }
  match current_source_path() {
    Some(source_path) => {
      let base = @path.Path(source_path).dirname()
      base.join(target).resolve().to_string()
    }
    None =>
      match current_function() {
        Some(func) =>
          match func.source_path {
            Some(source_path) => {
              let base = @path.Path(source_path).dirname()
              base.join(target).resolve().to_string()
            }
            None => resolve_path(path)
          }
        None => resolve_path(path)
      }
  }
}

///|
fn std_load_file(path : String) -> Value {
  let full_path = resolve_path(path)
  try {
    let text = @fs.read_file_to_string(full_path)
    String(text)
  } catch {
    _ => Null
  }
}

///|
fn std_load_script(path : String) -> Value raise {
  let full_path = resolve_path(path)
  let source = @fs.read_file_to_string(full_path) catch {
    _ => {
      let _ = throw_reference_error("could not load '\{path}'")
      ""
    }
  }
  let script = parse_script_with_pos(source)
  match root_env() {
    Some(env) =>
      with_source_path(full_path, fn() raise { eval_script(env, script) })
    None => Undefined
  }
}

///|
fn std_open_file(path : String, mode : String) -> Value {
  let readable = mode.contains("r") || mode.contains("+")
  let writable = mode.contains("w") || mode.contains("a") || mode.contains("+")
  let truncate = mode.contains("w")
  let create = writable
  let full_path = resolve_path(path)
  match open_file_handle(full_path, readable, writable, truncate, create) {
    Some(fd) => new_file_object(fd)
    None => Null
  }
}

///|
fn std_tmpfile() -> Value {
  new_file_object(tmp_file_handle())
}

///|
fn std_popen(command : String, mode : String) -> Value {
  if !mode.contains("r") {
    return Null
  }
  let mut output = ""
  if command.has_prefix("cat ") && command.length() > 4 {
    let name = command.unsafe_substring(start=4, end=command.length())
    match std_load_file(name) {
      String(text) => output = text
      _ => output = ""
    }
  }
  let fd = tmp_file_handle()
  let bytes = bytes_from_string(output)
  let _ = fd_write_bytes(fd, bytes, 0, bytes.length())
  let _ = fd_seek(fd, 0, seek_set)
  new_file_object(fd)
}

///|
fn std_fdopen(fd : Int, _mode : String) -> Value {
  match get_fd_handle(fd) {
    Some(_) => new_file_object(fd)
    None => Null
  }
}
