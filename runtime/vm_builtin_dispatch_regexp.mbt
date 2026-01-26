///|
fn call_builtin_regexp(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::RegExpConstructor =>
      regexp_from_args(args, false, new_target=Value::Builtin(builtin))
    BuiltinFunction::RegExpExec => {
      let (obj, data) = require_regexp(builtin.this_value)
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      regexp_exec_value(obj, data, input)
    }
    BuiltinFunction::RegExpTest => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() { Undefined } else { args[0] }
      Bool(regexp_test_method(this_value, input))
    }
    BuiltinFunction::RegExpToString =>
      regexp_to_string_value(builtin.this_value)
    BuiltinFunction::RegExpCompile =>
      regexp_compile_value(builtin.this_value, args)
    BuiltinFunction::RegExpEscape =>
      if args.is_empty() || !(args[0] is String(_)) {
        throw_type_error("not a string")
      } else {
        let text = to_string_value(args[0])
        String(regexp_escape_string(text))
      }
    BuiltinFunction::RegExpGetFlags =>
      regexp_get_flags_value(builtin.this_value)
    BuiltinFunction::RegExpGetSource =>
      regexp_get_source_value(builtin.this_value)
    BuiltinFunction::RegExpGetGlobal =>
      regexp_get_flag_value(builtin.this_value, "global")
    BuiltinFunction::RegExpGetIgnoreCase =>
      regexp_get_flag_value(builtin.this_value, "ignoreCase")
    BuiltinFunction::RegExpGetMultiline =>
      regexp_get_flag_value(builtin.this_value, "multiline")
    BuiltinFunction::RegExpGetDotAll =>
      regexp_get_flag_value(builtin.this_value, "dotAll")
    BuiltinFunction::RegExpGetUnicode =>
      regexp_get_flag_value(builtin.this_value, "unicode")
    BuiltinFunction::RegExpGetUnicodeSets =>
      regexp_get_flag_value(builtin.this_value, "unicodeSets")
    BuiltinFunction::RegExpGetSticky =>
      regexp_get_flag_value(builtin.this_value, "sticky")
    BuiltinFunction::RegExpGetHasIndices =>
      regexp_get_flag_value(builtin.this_value, "hasIndices")
    BuiltinFunction::RegExpSymbolMatch => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      regexp_symbol_match_value(this_value, input)
    }
    BuiltinFunction::RegExpSymbolMatchAll => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      regexp_symbol_match_all_value(this_value, input)
    }
    BuiltinFunction::RegExpSymbolReplace => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      let replace_value = if args.length() > 1 { args[1] } else { Undefined }
      regexp_symbol_replace_value(this_value, input, replace_value)
    }
    BuiltinFunction::RegExpSymbolSearch => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      regexp_symbol_search_value(this_value, input)
    }
    BuiltinFunction::RegExpSymbolSplit => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let input = if args.is_empty() {
        "undefined"
      } else {
        to_string_strict(args[0])
      }
      let limit_value = if args.length() > 1 { args[1] } else { Undefined }
      regexp_symbol_split_value(this_value, input, limit_value)
    }
    BuiltinFunction::RegExpStringIteratorNext =>
      regexp_string_iterator_next(builtin.this_value)
    _ => throw_type_error("invalid builtin")
  }
}
