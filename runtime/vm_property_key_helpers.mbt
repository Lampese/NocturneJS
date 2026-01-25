///|
fn property_key_name(value : Value) -> String raise {
  let prim = if is_object_like(value) {
    to_primitive(value, true)
  } else {
    value
  }
  match symbol_from_value(prim) {
    Some(symbol) => symbol_prop_key(symbol)
    None => to_string_strict(prim)
  }
}

///|
fn to_property_key_value(value : Value) -> Value raise {
  let prim = if is_object_like(value) {
    to_primitive(value, true)
  } else {
    value
  }
  match symbol_from_value(prim) {
    Some(symbol) => Symbol(symbol)
    None => String(to_string_strict(prim))
  }
}

///|
fn member_key_name(env : Env, key : @engine.MemberKey) -> String raise {
  match key {
    @engine.MemberKey::Ident(name) => name
    @engine.MemberKey::Private(name) => env_private_key_checked(env, name)
    @engine.MemberKey::Computed(expr) => property_key_name(eval_expr(env, expr))
  }
}

///|
fn string_property_get(value : String, name : String) -> Value raise {
  if name == "length" {
    return Number(Double::from_int(value.length()))
  }
  match parse_array_index(name) {
    Some(index) =>
      if index >= 0 && index < value.length() {
        return String(value.unsafe_substring(start=index, end=index + 1))
      }
    None => ()
  }
  match string_proto() {
    Some(proto) =>
      property_get_from_chain(proto.props, proto.proto, name, String(value))
    None => Undefined
  }
}

///|
fn ordered_prop_keys(
  props : Props,
  include_non_enum : Bool,
) -> Array[String] {
  let indices : Array[Int64] = []
  let non_indices : Array[String] = []
  props_iter(props, fn(key, prop) {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if include_non_enum || prop.enumerable {
      match array_index_from_name(key) {
        Some(index) => indices.push(index)
        None => non_indices.push(key)
      }
    }
  })
  sort_int64s(indices)
  let names : Array[String] = []
  for index in indices {
    names.push(Int64::to_string(index))
  }
  for name in non_indices {
    names.push(name)
  }
  names
}

///|
fn module_namespace_prop_keys(
  props : Props,
  include_non_enum : Bool,
) -> Array[String] {
  let names : Array[String] = []
  props_iter(props, fn(key, prop) {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if !include_non_enum && !prop.enumerable {
      return ()
    }
    names.push(key)
  })
  sort_strings_lex(names)
  names
}

///|
fn names_to_array_value(names : Array[String]) -> Value {
  let values : Array[Value?] = []
  for name in names {
    values.push(Some(String(name)))
  }
  new_array_value(values)
}
