///|
fn object_get_own_property_names(target : Value) -> Value raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => names_to_array_value(proxy_own_keys(data))
        None =>
          match obj.string_data {
            Some(_) => names_to_array_value(string_own_keys(obj, true))
            None =>
              if obj.is_module_namespace {
                names_to_array_value(
                  module_namespace_prop_keys(obj.props, true),
                )
              } else {
                names_to_array_value(ordered_prop_keys(obj.props, true))
              }
          }
      }
    Function(func) => names_to_array_value(ordered_prop_keys(func.props, true))
    BoundFunction(bound) =>
      names_to_array_value(ordered_prop_keys(bound.props, true))
    Builtin(builtin) =>
      names_to_array_value(ordered_prop_keys(builtin.props, true))
    Array(arr) => names_to_array_value(array_own_keys(arr, true))
    Arguments(args) => names_to_array_value(arguments_own_keys(args, true))
    _ => new_array_value([])
  }
}

///|
fn own_enumerable_property_names(target : Value) -> Array[String] raise {
  let names : Array[String] = []
  let keys = own_property_keys_values(target)
  for key in keys {
    match key {
      String(name) => {
        let desc = get_own_property_descriptor(target, name)
        match desc {
          Undefined => ()
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")

            } else if is_truthy(property_get(desc, "enumerable")) {
              names.push(name)
            }
        }
      }
      _ => ()
    }
  }
  names
}

///|
fn object_keys(target : Value) -> Value raise {
  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let keys = proxy_own_keys_values(data)
          let names : Array[String] = []
          for key in keys {
            match key {
              String(name) => {
                let desc = proxy_get_own_property_descriptor(data, name)
                match desc {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc, "enumerable")) {
                      names.push(name)
                    }
                }
              }
              _ => ()
            }
          }
          names_to_array_value(names)
        }
        None => names_to_array_value(own_enumerable_property_names(target))
      }
    _ => {
      names_to_array_value(own_enumerable_property_names(target))
    }
  }
}

///|
const OBJECT_KEYS_KIND_KEYS : Int = 0

///|
const OBJECT_KEYS_KIND_VALUES : Int = 1

///|
const OBJECT_KEYS_KIND_ENTRIES : Int = 2

///|
fn object_key_values(target : Value, kind : Int) -> Value raise {
  let entries : Array[Value?] = []
  fn push_entry(
    entries : Array[Value?],
    name : String,
    value : Value,
    kind : Int,
  ) -> Unit {
    match kind {
      OBJECT_KEYS_KIND_KEYS => entries.push(Some(String(name)))
      OBJECT_KEYS_KIND_VALUES => entries.push(Some(value))
      _ => {
        let pair = new_array_value([Some(String(name)), Some(value)])
        entries.push(Some(pair))
      }
    }
  }

  match target {
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => {
          let keys = proxy_own_keys_values(data)
          for key in keys {
            match key {
              String(name) => {
                let desc = proxy_get_own_property_descriptor(data, name)
                match desc {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc, "enumerable")) {
                      if kind == OBJECT_KEYS_KIND_KEYS {
                        entries.push(Some(String(name)))
                      } else {
                        let value = property_get(target, name)
                        push_entry(entries, name, value, kind)
                      }
                    }
                }
              }
              _ => ()
            }
          }
        }
        None => {
          let keys = own_property_keys_values(target)
          for key in keys {
            match key {
              String(name) => {
                let desc = get_own_property_descriptor(target, name)
                match desc {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc, "enumerable")) {
                      if kind == OBJECT_KEYS_KIND_KEYS {
                        entries.push(Some(String(name)))
                      } else {
                        let value = property_get(target, name)
                        push_entry(entries, name, value, kind)
                      }
                    }
                }
              }
              _ => ()
            }
          }
        }
      }
    _ => {
      let keys = own_property_keys_values(target)
      for key in keys {
        match key {
          String(name) => {
            let desc = get_own_property_descriptor(target, name)
            match desc {
              Undefined => ()
              _ =>
                if !is_object_like(desc) {
                  let _ = throw_type_error("invalid property descriptor")

                } else if is_truthy(property_get(desc, "enumerable")) {
                  if kind == OBJECT_KEYS_KIND_KEYS {
                    entries.push(Some(String(name)))
                  } else {
                    let value = property_get(target, name)
                    push_entry(entries, name, value, kind)
                  }
                }
            }
          }
          _ => ()
        }
      }
    }
  }
  new_array_value(entries)
}

///|
fn object_get_own_property_descriptors(target : Value) -> Value raise {
  let result = new_object_value()
  match result {
    Object(obj) => {
      let mut keys : Array[Value] = []
      match target {
        Object(raw) =>
          match raw.proxy_data {
            Some(data) => keys = proxy_own_keys_values(data)
            None => {
              let names = object_get_own_property_names(target)
              match names {
                Array(arr) =>
                  for entry in arr.elements {
                    match entry {
                      Some(String(name)) => keys.push(String(name))
                      _ => ()
                    }
                  }
                _ => ()
              }
              let symbols = own_symbol_keys(target, true)
              for symbol in symbols {
                keys.push(Symbol(symbol))
              }
            }
          }
        _ => {
          let names = object_get_own_property_names(target)
          match names {
            Array(arr) =>
              for entry in arr.elements {
                match entry {
                  Some(String(name)) => keys.push(String(name))
                  _ => ()
                }
              }
            _ => ()
          }
          let symbols = own_symbol_keys(target, true)
          for symbol in symbols {
            keys.push(Symbol(symbol))
          }
        }
      }
      for key in keys {
        match key {
          String(name) => {
            let desc = get_own_property_descriptor(target, name)
            if !(desc is Undefined) {
              let desc_prop = property_descriptor_object(property_data(desc))
              let _ = define_property(Object(obj), String(name), desc_prop)

            }
          }
          Symbol(symbol) => {
            let name = symbol_prop_key(symbol)
            let desc = get_own_property_descriptor(target, name)
            if !(desc is Undefined) {
              let desc_prop = property_descriptor_object(property_data(desc))
              let _ = define_property(Object(obj), Symbol(symbol), desc_prop)

            }
          }
          _ => ()
        }
      }
    }
    _ => ()
  }
  result
}

///|
fn own_property_keys_values(target : Value) -> Array[Value] raise {
  let mut keys : Array[Value] = []
  match target {
    Object(raw) =>
      match raw.proxy_data {
        Some(data) => keys = proxy_own_keys_values(data)
        None => {
          let names = object_get_own_property_names(target)
          match names {
            Array(arr) =>
              for entry in arr.elements {
                match entry {
                  Some(String(name)) => keys.push(String(name))
                  _ => ()
                }
              }
            _ => ()
          }
          let symbols = own_symbol_keys(target, true)
          for symbol in symbols {
            keys.push(Symbol(symbol))
          }
        }
      }
    _ => {
      let names = object_get_own_property_names(target)
      match names {
        Array(arr) =>
          for entry in arr.elements {
            match entry {
              Some(String(name)) => keys.push(String(name))
              _ => ()
            }
          }
        _ => ()
      }
      let symbols = own_symbol_keys(target, true)
      for symbol in symbols {
        keys.push(Symbol(symbol))
      }
    }
  }
  keys
}

///|
fn object_from_entries(iterable : Value) -> Value raise {
  let obj = new_object_value()
  let (iterator, next_method) = get_iterator_from_value(iterable)
  while true {
    let (done, item) = iterator_step_value(iterator, next_method) catch {
      err => raise err
    }
    if done {
      break
    }
    try {
      if !is_object_like(item) {
        let _ = throw_type_error("not an object")

      }
      let key = property_get(item, "0")
      let value = property_get(item, "1")
      let name = property_key_name(key)
      create_data_property_or_throw(obj, name, value)
    } catch {
      err => {
        iterator_close_on_error(iterator)
        raise err
      }
    }
  }
  obj
}

///|
fn object_group_by(iterable : Value, callback : Value) -> Value raise {
  if !is_callable(callback) {
    let _ = throw_type_error("not a function")
    return Undefined
  }
  let (iterator, next_method) = get_iterator_from_value(iterable)
  let groups = new_object_value_with_proto(None)
  let mut index : Int64 = 0
  let max_safe : Int64 = 9007199254740991L
  let this_arg = match current_env() {
    Some(env) =>
      match global_object(env) {
        Some(obj) => Object(obj)
        None => Undefined
      }
    None => Undefined
  }
  try {
    while true {
      if index >= max_safe {
        let _ = throw_type_error("too many elements")

      }
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      let key = call_value_with_this(
        callback,
        [item, Number(Int64::to_double(index))],
        this_arg,
      )
      let name = property_key_name(key)
      let mut bucket = property_get(groups, name)
      if bucket is Undefined {
        bucket = new_array_value([])
        let _ = create_data_property(groups, name, bucket)

      }
      let _ = array_push_value(bucket, [item])
      index = index + 1L
    }
  } catch {
    err => {
      iterator_close_on_error(iterator)
      raise err
    }
  }
  groups
}

///|
fn map_group_by(iterable : Value, callback : Value) -> Value raise {
  if !is_callable(callback) {
    let _ = throw_type_error("not a function")
    return Undefined
  }
  let (iterator, next_method) = get_iterator_from_value(iterable)
  let mut data = MapData::{ entries: [] }
  let proto = match current_env() {
    Some(env) =>
      match value_from_object(map_proto_for_env(env)) {
        Some(value) => Some(value)
        None => value_from_object(map_proto())
      }
    None => value_from_object(map_proto())
  }
  let groups = new_map_value_with_proto(proto)
  match groups {
    Object(obj) =>
      match obj.map_data {
        Some(map_data) => data = map_data
        None => ()
      }
    _ => ()
  }
  let mut index : Int64 = 0
  let max_safe : Int64 = 9007199254740991L
  let this_arg = match current_env() {
    Some(env) =>
      match global_object(env) {
        Some(obj) => Object(obj)
        None => Undefined
      }
    None => Undefined
  }
  try {
    while true {
      if index >= max_safe {
        let _ = throw_type_error("too many elements")

      }
      let (done, item) = iterator_step_value(iterator, next_method)
      if done {
        break
      }
      let key = call_value_with_this(
        callback,
        [item, Number(Int64::to_double(index))],
        this_arg,
      )
      let normalized = map_normalize_key(key)
      let mut bucket = Undefined
      match map_find_index(data.entries, normalized) {
        Some(entry_index) =>
          match data.entries[entry_index] {
            Some((_, value)) => bucket = value
            None => ()
          }
        None => {
          bucket = new_array_value([])
          map_set_value(data, normalized, bucket)
        }
      }
      let _ = array_push_value(bucket, [item])
      index = index + 1L
    }
  } catch {
    err => {
      iterator_close_on_error(iterator)
      raise err
    }
  }
  groups
}

///|
fn object_define_accessor(
  this_value : Value,
  name_value : Value,
  func_value : Value,
  is_setter : Bool,
) -> Value raise {
  let obj = to_object(this_value)
  if !is_callable(func_value) {
    let _ = throw_type_error("not a function")
    return Undefined
  }
  let name = property_key_name(name_value)
  let desc = new_object_value()
  match desc {
    Object(desc_obj) => {
      if is_setter {
        props_set(desc_obj.props, "set", property_data(func_value))
      } else {
        props_set(desc_obj.props, "get", property_data(func_value))
      }
      props_set(desc_obj.props, "enumerable", property_data(Bool(true)))
      props_set(desc_obj.props, "configurable", property_data(Bool(true)))
    }
    _ => ()
  }
  let _ = define_property(obj, String(name), desc)
  Undefined
}

///|
fn object_lookup_accessor(
  this_value : Value,
  name_value : Value,
  is_setter : Bool,
) -> Value raise {
  let obj = to_object(this_value)
  let name = property_key_name(name_value)
  let mut current : Value? = Some(obj)
  while true {
    match current {
      Some(value) => {
        let desc = get_own_property_descriptor(value, name)
        match desc {
          Undefined => current = get_proto_of_value_checked(value)
          _ =>
            if !is_object_like(desc) {
              let _ = throw_type_error("invalid property descriptor")
              return Undefined
            } else if descriptor_is_accessor(desc) {
              let key = if is_setter { "set" } else { "get" }
              return property_get(desc, key)
            } else {
              return Undefined
            }
        }
      }
      None => return Undefined
    }
  }
  Undefined
}

///|
fn object_proto_get(this_value : Value) -> Value raise {
  let obj = to_object(this_value)
  match get_proto_of_value_checked(obj) {
    Some(proto) => proto
    None => Null
  }
}

///|
fn object_proto_set(this_value : Value, proto_value : Value) -> Value raise {
  match this_value {
    Undefined | Null => {
      let _ = throw_type_error("not an object")
      return Undefined
    }
    _ => ()
  }
  if !is_object_like(proto_value) && !(proto_value is Null) {
    return Undefined
  }
  if is_object_like(this_value) {
    let proto = if proto_value is Null { None } else { Some(proto_value) }
    let _ = set_proto_of_value(this_value, proto, true)

  }
  Undefined
}

///|
fn string_own_keys(obj : ObjectValue, include_non_enum : Bool) -> Array[String] {
  match obj.string_data {
    None => ordered_prop_keys(obj.props, include_non_enum)
    Some(value) => {
      let indices : Array[Int64] = []
      let non_indices : Array[String] = []
      let seen : Map[Int64, Bool] = Map::new()
      let len = value.length()
      for i = 0; i < len; i = i + 1 {
        let index = Int64::from_int(i)
        seen.set(index, true)
        indices.push(index)
      }
      props_iter(obj.props, fn(key, prop) {
        if is_symbol_prop_key(key) || is_private_prop_key(key) {
          return ()
        }
        if include_non_enum || prop.enumerable {
          match array_index_from_name(key) {
            Some(index) =>
              if !seen.contains(index) {
                seen.set(index, true)
                indices.push(index)
              }
            None => non_indices.push(key)
          }
        }
      })
      sort_int64s(indices)
      let names : Array[String] = []
      for index in indices {
        names.push(Int64::to_string(index))
      }
      if include_non_enum && !props_contains(obj.props, "length") {
        names.push("length")
      }
      for name in non_indices {
        names.push(name)
      }
      names
    }
  }
}

///|
fn array_own_keys(arr : ArrayValue, include_non_enum : Bool) -> Array[String] {
  let indices : Array[Int64] = []
  let non_indices : Array[String] = []
  let seen : Map[Int64, Bool] = Map::new()
  let mut use_typed_indices = false
  let mut len = arr.elements.length()
  match arr.typed_array_data {
    Some(data) => {
      use_typed_indices = true
      len = typed_array_effective_length(data)
    }
    None => ()
  }
  for i = 0; i < len; i = i + 1 {
    if use_typed_indices {
      let index = Int64::from_int(i)
      seen.set(index, true)
      indices.push(index)
    } else {
      let name = Int::to_string(i)
      match props_get(arr.props, name) {
        Some(prop) =>
          if include_non_enum || prop.enumerable {
            let index = Int64::from_int(i)
            seen.set(index, true)
            indices.push(index)
          }
        None =>
          match arr.elements[i] {
            Some(_) => {
              let index = Int64::from_int(i)
              seen.set(index, true)
              indices.push(index)
            }
            None => ()
          }
      }
    }
  }
  props_iter(arr.props, fn(key, prop) {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if include_non_enum || prop.enumerable {
      match array_index_from_name(key) {
        Some(index) =>
          if !seen.contains(index) {
            seen.set(index, true)
            indices.push(index)
          }
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
fn arguments_own_keys(
  args : ArgumentsValue,
  include_non_enum : Bool,
) -> Array[String] {
  let indices : Array[Int64] = []
  let non_indices : Array[String] = []
  let seen : Map[Int64, Bool] = Map::new()
  let len = args.elements.length()
  for i = 0; i < len; i = i + 1 {
    let name = Int::to_string(i)
    match props_get(args.props, name) {
      Some(prop) =>
        if include_non_enum || prop.enumerable {
          let index = Int64::from_int(i)
          seen.set(index, true)
          indices.push(index)
        }
      None =>
        if arguments_has_index(args, i) {
          let index = Int64::from_int(i)
          seen.set(index, true)
          indices.push(index)
        }
    }
  }
  props_iter(args.props, fn(key, prop) {
    if is_symbol_prop_key(key) || is_private_prop_key(key) {
      return ()
    }
    if include_non_enum || prop.enumerable {
      match array_index_from_name(key) {
        Some(index) =>
          if !seen.contains(index) {
            seen.set(index, true)
            indices.push(index)
          }
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
