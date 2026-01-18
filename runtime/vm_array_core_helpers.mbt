///|
fn typed_array_join(data : TypedArrayData, sep : String) -> String {
  let parts : Array[String] = []
  let len = typed_array_effective_length(data)
  let mut i = 0
  while i < len {
    let value = typed_array_get_index(data, i)
    match value {
      Undefined => parts.push("")
      Null => parts.push("")
      _ => parts.push(to_string_value(value))
    }
    i = i + 1
  }
  parts.join(sep)
}

///|
fn array_like_length(value : Value) -> Int raise {
  let len64 = array_like_length_int64(value)
  let max_int = 2147483647
  if len64 > Int64::from_int(max_int) {
    max_int
  } else {
    Int64::to_int(len64)
  }
}

///|
fn array_like_length_int64(value : Value) -> Int64 raise {
  let len_value = property_get(value, "length")
  let len_num = to_number(len_value)
  if Double::is_nan(len_num) || len_num <= 0.0 {
    return 0L
  }
  if Double::is_inf(len_num) {
    return 9007199254740991L
  }
  let trunc = Double::trunc(len_num)
  if trunc <= 0.0 {
    return 0L
  }
  if trunc >= 9007199254740991.0 {
    return 9007199254740991L
  }
  Double::to_int64(trunc)
}

///|
fn array_species_create(obj : Value, len : Int) -> Value raise {
  if !is_array_value(obj) {
    let array_proto_value = match current_env() {
      Some(env) =>
        match value_from_object(array_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(array_proto())
        }
      None => value_from_object(array_proto())
    }
    return new_array_value_with_proto(
      Array::make(len, None),
      proto=array_proto_value,
    )
  }
  let ctor = property_get(obj, "constructor")
  let mut species = ctor
  if is_constructor_value(ctor) {
    match (current_env(), function_realm_env(ctor)) {
      (Some(current), Some(realm)) =>
        if current.id != realm.id {
          let realm_array = property_get(global_object_value(realm), "Array")
          if same_value(ctor, realm_array) {
            species = Undefined
          }
        }
      _ => ()
    }
  }
  if is_object_like(species) {
    match symbol_species_key() {
      Some(key) => {
        let next = property_get(species, key)
        species = match next {
          Null => Undefined
          _ => next
        }
      }
      None => ()
    }
  }
  if species is Undefined {
    let array_proto_value = match current_env() {
      Some(env) =>
        match value_from_object(array_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(array_proto())
        }
      None => value_from_object(array_proto())
    }
    return new_array_value_with_proto(
      Array::make(len, None),
      proto=array_proto_value,
    )
  }
  call_constructor(species, [Number(Double::from_int(len))])
}

///|
fn array_species_create64(obj : Value, len : Int64) -> Value raise {
  let max_int = 2147483647
  if !is_array_value(obj) {
    if len > Int64::from_int(max_int) {
      return throw_range_error("invalid array length")
    }
    let array_proto_value = match current_env() {
      Some(env) =>
        match value_from_object(array_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(array_proto())
        }
      None => value_from_object(array_proto())
    }
    return new_array_value_with_proto(
      Array::make(Int64::to_int(len), None),
      proto=array_proto_value,
    )
  }
  let ctor = property_get(obj, "constructor")
  let mut species = ctor
  if is_constructor_value(ctor) {
    match (current_env(), function_realm_env(ctor)) {
      (Some(current), Some(realm)) =>
        if current.id != realm.id {
          let realm_array = property_get(global_object_value(realm), "Array")
          if same_value(ctor, realm_array) {
            species = Undefined
          }
        }
      _ => ()
    }
  }
  if is_object_like(species) {
    match symbol_species_key() {
      Some(key) => {
        let next = property_get(species, key)
        species = match next {
          Null => Undefined
          _ => next
        }
      }
      None => ()
    }
  }
  if species is Undefined {
    if len > Int64::from_int(max_int) {
      return throw_range_error("invalid array length")
    }
    let array_proto_value = match current_env() {
      Some(env) =>
        match value_from_object(array_proto_for_env(env)) {
          Some(value) => Some(value)
          None => value_from_object(array_proto())
        }
      None => value_from_object(array_proto())
    }
    return new_array_value_with_proto(
      Array::make(Int64::to_int(len), None),
      proto=array_proto_value,
    )
  }
  call_constructor(species, [Number(Int64::to_double(len))])
}

///|
fn typed_array_species_constructor(
  obj : Value,
  kind : TypedArrayKind,
) -> Value raise {
  let ctor = property_get(obj, "constructor")
  let mut species = ctor
  if is_object_like(species) {
    match symbol_species_key() {
      Some(key) => {
        let next = property_get(species, key)
        species = match next {
          Null => Undefined
          _ => next
        }
      }
      None => ()
    }
  }
  if species is Undefined {
    match
      typed_array_ctor_for_kind_env(function_realm_env_checked(ctor), kind) {
      Some(value) => return value
      None => return ctor
    }
  }
  if !is_constructor_value(species) {
    let _ = throw_type_error("not a constructor")

  }
  species
}

///|
fn typed_array_species_create64(
  obj : Value,
  kind : TypedArrayKind,
  len : Int64,
) -> Value raise {
  let max_int = 2147483647
  if len > Int64::from_int(max_int) {
    return throw_range_error("invalid typed array length")
  }
  let ctor = typed_array_species_constructor(obj, kind)
  let result = call_constructor(ctor, [Number(Int64::to_double(len))])
  let (_, data) = require_typed_array(Some(result))
  let new_len = typed_array_effective_length(data)
  if new_len < Int64::to_int(len) {
    return throw_type_error("TypedArray length is too small")
  }
  result
}

///|
fn typed_array_create_from_constructor(
  ctor : Value,
  len : Int64,
) -> Value raise {
  let max_int = 2147483647
  if len < 0L || len > Int64::from_int(max_int) {
    return throw_range_error("invalid typed array length")
  }
  let result = call_constructor(ctor, [Number(Int64::to_double(len))])
  let (_, data) = require_typed_array_checked(Some(result))
  let new_len = typed_array_effective_length(data)
  if new_len < Int64::to_int(len) {
    return throw_type_error("TypedArray length is too small")
  }
  result
}

///|
fn is_concat_spreadable(value : Value) -> Bool raise {
  if is_object_like(value) {
    match symbol_is_concat_spreadable_key() {
      Some(key) =>
        match property_get(value, key) {
          Undefined => ()
          spread => return is_truthy(spread)
        }
      None => ()
    }
  }
  is_array_value(value)
}

///|
fn array_to_string(arr : ArrayValue) -> String {
  array_join(arr, ",")
}

///|
fn array_join(arr : ArrayValue, sep : String) -> String {
  match arr.typed_array_data {
    Some(data) => return typed_array_join(data, sep)
    None => ()
  }
  let parts : Array[String] = []
  let len = arr.elements.length()
  for i = 0; i < len; i = i + 1 {
    let name = Int::to_string(i)
    let mut value_opt : Value? = None
    match arr.props.get(name) {
      Some(prop) => value_opt = Some(prop.value)
      None => ()
    }
    match value_opt {
      Some(value) =>
        match value {
          Undefined => parts.push("")
          Null => parts.push("")
          _ => parts.push(to_string_value(value))
        }
      None =>
        match arr.elements[i] {
          None => parts.push("")
          Some(value) =>
            match value {
              Undefined => parts.push("")
              Null => parts.push("")
              _ => parts.push(to_string_value(value))
            }
        }
    }
  }
  parts.join(sep)
}

///|
fn array_join_value(target : Value, sep_value : Value?) -> String raise {
  let obj = to_object(target)
  let len = array_like_length_int64(obj)
  let sep = match sep_value {
    None => ","
    Some(Undefined) => ","
    Some(value) => to_string_strict(value)
  }
  if len <= 0L {
    return ""
  }
  let sb = StringBuilder::new()
  let mut i = 0L
  while i < len {
    if i > 0L {
      sb.write_string(sep)
    }
    let name = Int64::to_string(i)
    let value = property_get(obj, name)
    match value {
      Undefined => ()
      Null => ()
      _ => sb.write_string(to_string_strict(value))
    }
    i = i + 1L
  }
  sb.to_string()
}

///|
fn value_to_locale_string(value : Value) -> String raise {
  match value {
    Undefined => to_string_strict(value)
    Null => to_string_strict(value)
    _ => {
      let obj = to_object(value)
      let locale_method = property_get_with_receiver(
        obj, "toLocaleString", value,
      )
      if !is_callable(locale_method) {
        let _ = throw_type_error("not a function")

      }
      let result = call_value_with_this(locale_method, [], value)
      to_string_strict(result)
    }
  }
}

///|
fn array_to_locale_string_value(target : Value) -> String raise {
  let obj = to_object(target)
  let len = array_like_length_int64(obj)
  if len <= 0L {
    return ""
  }
  let sb = StringBuilder::new()
  let mut i = 0L
  while i < len {
    if i > 0L {
      sb.write_string(",")
    }
    let name = Int64::to_string(i)
    let value = property_get(obj, name)
    match value {
      Undefined => ()
      Null => ()
      _ => sb.write_string(value_to_locale_string(value))
    }
    i = i + 1L
  }
  sb.to_string()
}

///|
priv enum ArrayIterKind {
  ForEach
  Map
  Filter
  Every
  SomeMatch
}

///|
priv enum ArrayFindKind {
  Find
  FindIndex
  FindLast
  FindLastIndex
}
