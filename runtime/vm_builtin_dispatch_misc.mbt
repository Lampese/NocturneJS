///|
fn call_builtin_misc(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::FunctionCall => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_callable(target) {
        return throw_type_error("not a function")
      }
      if args.is_empty() {
        call_value_with_this(target, [], Undefined)
      } else {
        let rest : Array[Value] = []
        if args.length() > 1 {
          for i = 1; i < args.length(); i = i + 1 {
            rest.push(args[i])
          }
        }
        call_value_with_this(target, rest, args[0])
      }
    }
    BuiltinFunction::FunctionApply => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_callable(target) {
        return throw_type_error("not a function")
      }
      let this_arg = if args.is_empty() { Undefined } else { args[0] }
      let array_arg = if args.length() < 2 { Undefined } else { args[1] }
      if array_arg is Undefined || array_arg is Null {
        return call_value_with_this(target, [], this_arg)
      }
      if !is_object_like(array_arg) {
        return throw_type_error("argArray is not an object")
      }
      let argv = build_arg_list(array_arg)
      call_value_with_this(target, argv, this_arg)
    }
    BuiltinFunction::FunctionBind => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_callable(target) {
        return throw_type_error("not a function")
      }
      let this_arg = if args.is_empty() { Undefined } else { args[0] }
      let bound_args : Array[Value] = []
      if args.length() > 1 {
        for i = 1; i < args.length(); i = i + 1 {
          bound_args.push(args[i])
        }
      }
      new_bound_function(target, this_arg, bound_args)
    }
    BuiltinFunction::FunctionToString => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      String(function_to_string_value(target))
    }
    BuiltinFunction::FunctionProtoLineNumber =>
      function_debug_location(builtin.this_value, false)
    BuiltinFunction::FunctionProtoColumnNumber =>
      function_debug_location(builtin.this_value, true)
    BuiltinFunction::FunctionSymbolHasInstance => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !is_callable(target) {
        return Bool(false)
      }
      let value = if args.is_empty() { Undefined } else { args[0] }
      Bool(ordinary_instanceof(value, target))
    }
    BuiltinFunction::Escape => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_escape(text))
    }
    BuiltinFunction::Unescape => {
      let text = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      String(uri_unescape(text))
    }
    BuiltinFunction::SpeciesGetter =>
      match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
    BuiltinFunction::WorkerConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::WorkerPostMessage => {
      let (worker_id, to_main) = worker_port_info(builtin.this_value)
      let payload = if args.is_empty() { Undefined } else { args[0] }
      let cloned = clone_worker_value(payload)
      worker_enqueue(worker_id, to_main, cloned)
      Undefined
    }
    BuiltinFunction::StdSprintf => std_sprintf(args)
    BuiltinFunction::StdLoadScript => {
      if args.is_empty() {
        return throw_type_error("missing filename")
      }
      std_load_script(to_string_value(args[0]))
    }
    BuiltinFunction::StdTmpfile => std_tmpfile()
    BuiltinFunction::StdOpen => {
      if args.is_empty() {
        return Null
      }
      let path = to_string_value(args[0])
      let mode = if args.length() > 1 { to_string_value(args[1]) } else { "r" }
      std_open_file(path, mode)
    }
    BuiltinFunction::StdLoadFile => {
      if args.is_empty() {
        return Null
      }
      std_load_file(to_string_value(args[0]))
    }
    BuiltinFunction::StdPopen => {
      if args.is_empty() {
        return Null
      }
      let command = to_string_value(args[0])
      let mode = if args.length() > 1 { to_string_value(args[1]) } else { "r" }
      std_popen(command, mode)
    }
    BuiltinFunction::StdParseExtJSON => {
      if args.is_empty() {
        return throw_type_error("invalid JSON")
      }
      std_parse_ext_json(to_string_value(args[0]))
    }
    BuiltinFunction::StdFdopen => {
      if args.is_empty() {
        return Null
      }
      let fd = to_int32(to_number(args[0]))
      let mode = if args.length() > 1 { to_string_value(args[1]) } else { "r" }
      std_fdopen(fd, mode)
    }
    BuiltinFunction::StdGc => {
      run_gc()
      Undefined
    }
    BuiltinFunction::Print => {
      let sb = StringBuilder::new()
      let mut i = 0
      while i < args.length() {
        if i > 0 {
          sb.write_char(' ')
        }
        sb.write_string(to_string_value(args[i]))
        i = i + 1
      }
      let line = sb.to_string()
      record_print_line(line)
      println(line)
      Undefined
    }
    BuiltinFunction::FunctionConstructor =>
      match function_realm_env(Value::Builtin(builtin)) {
        Some(env) => function_from_args_with_env(args, env)
        None => function_from_args(args)
      }
    _ => throw_type_error("invalid builtin")
  }
}
