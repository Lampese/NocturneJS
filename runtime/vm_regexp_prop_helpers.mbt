///|
fn is_regexp_proto_obj(obj : ObjectValue, env : Env?) -> Bool {
  match env {
    Some(value) =>
      match regexp_proto_for_env(value) {
        Some(proto) => proto.id == obj.id
        None => false
      }
    None =>
      match regexp_proto() {
        Some(proto) => proto.id == obj.id
        None => false
      }
  }
}

///|
fn regexp_get_source_value(this_value : Value?) -> Value raise {
  match this_value {
    Some(Object(obj)) =>
      if is_regexp_proto_obj(obj, current_env()) {
        String("(?:)")
      } else {
        match obj.regexp_data {
          Some(data) => String(regexp_source_escape(data.source))
          None => throw_type_error("not a regexp")
        }
      }
    Some(_) => throw_type_error("not an object")
    None => throw_type_error("not an object")
  }
}

///|
fn regexp_get_flag_value(this_value : Value?, flag : String) -> Value raise {
  match this_value {
    Some(Object(obj)) =>
      if is_regexp_proto_obj(obj, current_env()) {
        Undefined
      } else {
        match obj.regexp_data {
          Some(data) => {
            let enabled = match flag {
              "global" => data.global
              "ignoreCase" => data.ignore_case
              "multiline" => data.multiline
              "dotAll" => data.dot_all
              "unicode" => data.unicode && !data.unicode_sets
              "unicodeSets" => data.unicode_sets
              "sticky" => data.sticky
              "hasIndices" => data.has_indices
              _ => false
            }
            Bool(enabled)
          }
          None => throw_type_error("not a regexp")
        }
      }
    Some(_) => throw_type_error("not an object")
    None => throw_type_error("not an object")
  }
}

///|
fn regexp_get_flags_value(this_value : Value?) -> Value raise {
  let target = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(target) {
    return throw_type_error("not an object")
  }
  let flags = StringBuilder::new()
  let entries : Array[(String, String)] = [
    ("hasIndices", "d"),
    ("global", "g"),
    ("ignoreCase", "i"),
    ("multiline", "m"),
    ("dotAll", "s"),
    ("unicode", "u"),
    ("unicodeSets", "v"),
    ("sticky", "y"),
  ]
  for entry in entries {
    let (name, flag) = entry
    if is_truthy(property_get(target, name)) {
      flags.write_string(flag)
    }
  }
  String(flags.to_string())
}

///|
fn regexp_to_string_value(this_value : Value?) -> Value raise {
  let target = match this_value {
    Some(value) => value
    None => Undefined
  }
  if !is_object_like(target) {
    return throw_type_error("not an object")
  }
  let source = to_string_strict(property_get(target, "source"))
  let flags = to_string_strict(property_get(target, "flags"))
  let sb = StringBuilder::new()
  sb.write_char('/')
  sb.write_string(source)
  sb.write_char('/')
  sb.write_string(flags)
  String(sb.to_string())
}

///|
fn regexp_species_constructor(rx : Value) -> Value raise {
  let default_ctor = match global_property_value("RegExp") {
    Some(value) => value
    None => new_builtin_value(BuiltinFunction::RegExpConstructor)
  }
  let ctor = property_get(rx, "constructor")
  match ctor {
    Undefined => default_ctor
    _ =>
      if !is_object_like(ctor) {
        throw_type_error("constructor is not an object")
      } else {
        match symbol_species_key() {
          Some(key) =>
            match property_get(ctor, key) {
              Undefined | Null => default_ctor
              species =>
                if !is_constructor_value(species) {
                  throw_type_error("not a constructor")
                } else {
                  species
                }
            }
          None => default_ctor
        }
      }
  }
}
