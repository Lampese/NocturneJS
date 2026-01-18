///|
fn parse_regexp_flags(flags : String) -> RegexpFlags raise {
  let mut global = false
  let mut sticky = false
  let mut unicode = false
  let mut dot_all = false
  let mut ignore_case = false
  let mut multiline = false
  let mut has_indices = false
  let mut unicode_sets = false
  for ch in flags {
    match ch {
      'g' =>
        if global {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          global = true
        }
      'y' =>
        if sticky {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          sticky = true
        }
      'u' =>
        if unicode || unicode_sets {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          unicode = true
        }
      'v' =>
        if unicode || unicode_sets {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          unicode_sets = true
          unicode = true
        }
      's' =>
        if dot_all {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          dot_all = true
        }
      'i' =>
        if ignore_case {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          ignore_case = true
        }
      'm' =>
        if multiline {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          multiline = true
        }
      'd' =>
        if has_indices {
          let _ = throw_syntax_error("invalid regexp flags")

        } else {
          has_indices = true
        }
      _ => {
        let _ = throw_syntax_error("invalid regexp flags")

      }
    }
  }
  let mut compile = ""
  if ignore_case {
    compile = compile + "i"
  }
  if multiline {
    compile = compile + "m"
  }
  if dot_all {
    compile = compile + "s"
  }
  if unicode_sets {
    compile = compile + "v"
  } else if unicode {
    compile = compile + "u"
  }
  RegexpFlags::{
    compile,
    global,
    sticky,
    unicode,
    dot_all,
    ignore_case,
    multiline,
    has_indices,
    unicode_sets,
  }
}

///|
fn regexp_data_from_pattern(
  pattern : String,
  flags : String,
) -> RegexpData raise {
  let parsed = parse_regexp_flags(flags)
  let regex = try regexp_compile(pattern, parsed) catch {
    _ => {
      let _ = throw_syntax_error("invalid regular expression")
      fail("unreachable")
    }
  } noraise {
    value => value
  }
  RegexpData::{
    source: pattern,
    flags,
    regex,
    global: parsed.global,
    sticky: parsed.sticky,
    unicode: parsed.unicode,
    dot_all: parsed.dot_all,
    ignore_case: parsed.ignore_case,
    multiline: parsed.multiline,
    has_indices: parsed.has_indices,
    unicode_sets: parsed.unicode_sets,
  }
}

///|
fn is_regexp_value(value : Value) -> Bool raise {
  if !is_object_like(value) {
    return false
  }
  match symbol_match_key() {
    Some(key) =>
      match property_get(value, key) {
        Undefined =>
          match regexp_object_from_value(value) {
            Some(_) => true
            None => false
          }
        matcher => is_truthy(matcher)
      }
    None =>
      match regexp_object_from_value(value) {
        Some(_) => true
        None => false
      }
  }
}

///|
fn check_regexp_g_flag(value : Value) -> Unit raise {
  if !is_regexp_value(value) {
    return
  }
  let flags = property_get(value, "flags")
  match flags {
    Undefined | Null => {
      let _ = throw_type_error("cannot convert to object")
      return
    }
    _ => ()
  }
  let flag_text = to_string_strict(flags)
  if string_index_of(flag_text, "g", 0) < 0 {
    let _ = throw_type_error("regexp must have the 'g' flag")

  }
}

///|
fn regexp_object_from_value(value : Value) -> (ObjectValue, RegexpData)? {
  match value {
    Object(obj) =>
      match obj.regexp_data {
        Some(data) => Some((obj, data))
        None => None
      }
    _ => None
  }
}

///|
fn require_regexp(this_value : Value?) -> (ObjectValue, RegexpData) raise {
  match this_value {
    Some(Object(obj)) =>
      match obj.regexp_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a regexp")
          fail("unreachable")
        }
      }
    Some(_) => {
      let _ = throw_type_error("not a regexp")
      fail("unreachable")
    }
    None => {
      let _ = throw_type_error("not a regexp")
      fail("unreachable")
    }
  }
}

///|
fn regexp_compile_value(
  this_value : Value?,
  args : Array[Value],
) -> Value raise {
  let (obj, _) = require_regexp(this_value)
  let pattern_value = if args.is_empty() { Undefined } else { args[0] }
  let flags_value = if args.length() > 1 { args[1] } else { Undefined }
  let data = match regexp_object_from_value(pattern_value) {
    Some((_, pattern_data)) => {
      if !(flags_value is Undefined) {
        let _ = throw_type_error("flags must be undefined")

      }
      pattern_data
    }
    None => {
      let pattern = if pattern_value is Undefined {
        ""
      } else {
        to_string_strict(pattern_value)
      }
      let flags = if flags_value is Undefined {
        ""
      } else {
        to_string_strict(flags_value)
      }
      regexp_data_from_pattern(pattern, flags)
    }
  }
  obj.regexp_data = Some(data)
  let value = Object(obj)
  let _ = property_set(value, "lastIndex", Number(0.0), true)
  value
}

///|
fn regexp_from_args(
  args : Array[Value],
  force_new : Bool,
  new_target? : Value = Undefined,
) -> Value raise {
  let pattern_value = if args.is_empty() { Undefined } else { args[0] }
  let flags_value = if args.length() > 1 { args[1] } else { Undefined }
  let pattern_is_regexp = is_regexp_value(pattern_value)
  if pattern_is_regexp {
    if flags_value is Undefined && !force_new {
      let ctor = property_get(pattern_value, "constructor")
      if same_value(ctor, new_target) {
        return pattern_value
      }
    }
  }
  let mut pattern = ""
  let mut flags_str : String? = None
  let mut flags_raw : Value? = None
  if pattern_is_regexp {
    match regexp_object_from_value(pattern_value) {
      Some((_, data)) => {
        pattern = data.source
        if flags_value is Undefined {
          flags_str = Some(data.flags)
        } else {
          flags_raw = Some(flags_value)
        }
      }
      None => {
        let source_value = property_get(pattern_value, "source")
        pattern = to_string_strict(source_value)
        if flags_value is Undefined {
          flags_raw = Some(property_get(pattern_value, "flags"))
        } else {
          flags_raw = Some(flags_value)
        }
      }
    }
  } else {
    if pattern_value is Undefined {
      pattern = ""
    } else {
      pattern = to_string_strict(pattern_value)
    }
    if flags_value is Undefined {
      flags_str = Some("")
    } else {
      flags_raw = Some(flags_value)
    }
  }
  let proto = if new_target is Undefined {
    value_from_object(regexp_proto())
  } else {
    let default_proto = match function_realm_env(new_target) {
      Some(env) =>
        match value_from_object(regexp_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(regexp_proto())
        }
      None => value_from_object(regexp_proto())
    }
    constructor_proto_from_target(new_target, default_proto)
  }
  let flags = match flags_str {
    Some(value) => value
    None =>
      match flags_raw {
        Some(value) =>
          if value is Undefined {
            ""
          } else {
            to_string_strict(value)
          }
        None => ""
      }
  }
  new_regexp_value(pattern, flags, proto)
}
