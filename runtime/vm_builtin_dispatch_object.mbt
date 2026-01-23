///|
fn call_builtin_object(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::ObjectConstructor =>
      if args.is_empty() {
        new_object_value()
      } else {
        match args[0] {
          Undefined => new_object_value()
          Null => new_object_value()
          _ =>
            if is_object_like(args[0]) {
              args[0]
            } else {
              let env = function_realm_env_checked(Value::Builtin(builtin))
              coerce_this(env, args[0])
            }
        }
      }
    BuiltinFunction::ProxyConstructor =>
      throw_type_error("Proxy constructor requires 'new'")
    BuiltinFunction::ProxyRevocable => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      let handler = if args.length() > 1 { args[1] } else { Undefined }
      let proxy = new_proxy_value(target, handler)
      let revoke = new_proxy_revoke_function(proxy)
      let result = new_object_value()
      match result {
        Object(obj) => {
          props_set(obj.props, "proxy", property_data(proxy))
          props_set(obj.props, "revoke", property_data(revoke))
        }
        _ => ()
      }
      result
    }
    BuiltinFunction::ProxyRevoke => {
      match builtin.this_value {
        Some(Object(obj)) =>
          match obj.proxy_data {
            Some(data) => {
              data.handler = Null
              data.target = Null
            }
            None => ()
          }
        _ => ()
      }
      Undefined
    }
    BuiltinFunction::ObjectDefineProperty => {
      if args.length() < 3 {
        return Undefined
      }
      define_property(args[0], args[1], args[2])
    }
    BuiltinFunction::ObjectDefineProperties => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      let props = if args.length() > 1 { args[1] } else { Undefined }
      define_properties_from_object(target, props)
      target
    }
    BuiltinFunction::ObjectGetOwnPropertyDescriptor => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          let key = if args.length() > 1 { args[1] } else { Undefined }
          let name = property_key_name(key)
          get_own_property_descriptor(obj, name)
        }
      }
    }
    BuiltinFunction::ObjectGetOwnPropertyNames => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          object_get_own_property_names(obj)
        }
      }
    }
    BuiltinFunction::ObjectGetOwnPropertySymbols => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          let values : Array[Value?] = match obj {
            Object(raw) =>
              match raw.proxy_data {
                Some(data) => {
                  let keys = proxy_own_keys_values(data)
                  let items : Array[Value?] = []
                  for key in keys {
                    match key {
                      Symbol(_) => items.push(Some(key))
                      _ => ()
                    }
                  }
                  items
                }
                None => {
                  let symbols = own_symbol_keys(obj, true)
                  let items : Array[Value?] = []
                  for symbol in symbols {
                    items.push(Some(Symbol(symbol)))
                  }
                  items
                }
              }
            _ => {
              let symbols = own_symbol_keys(obj, true)
              let items : Array[Value?] = []
              for symbol in symbols {
                items.push(Some(Symbol(symbol)))
              }
              items
            }
          }
          new_array_value(values)
        }
      }
    }
    BuiltinFunction::ObjectGetOwnPropertyDescriptors => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          object_get_own_property_descriptors(obj)
        }
      }
    }
    BuiltinFunction::ObjectGetPrototypeOf => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          match get_proto_of_value_checked(obj) {
            Some(proto) => proto
            None => Null
          }
        }
      }
    }
    BuiltinFunction::ObjectSetPrototypeOf => {
      if args.is_empty() {
        return Undefined
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ =>
          if !is_object_like(target) {
            target
          } else {
            let proto_value = if args.length() > 1 {
              args[1]
            } else {
              Undefined
            }
            let proto = match proto_value {
              _ if is_object_like(proto_value) => Some(proto_value)
              Null => None
              _ => {
                let _ = throw_type_error("not a prototype")
                None
              }
            }
            let _ = set_proto_of_value(target, proto, true)
            target
          }
      }
    }
    BuiltinFunction::ObjectCreate => {
      if args.is_empty() {
        return throw_type_error("not a prototype")
      }
      let proto_value = args[0]
      let proto = match proto_value {
        _ if is_object_like(proto_value) => Some(proto_value)
        Null => None
        _ => {
          let _ = throw_type_error("not a prototype")
          None
        }
      }
      let obj = new_object_value_with_proto(proto)
      if args.length() > 1 && !(args[1] is Undefined) {
        define_properties_from_object(obj, args[1])
      }
      obj
    }
    BuiltinFunction::ObjectKeys => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          object_keys(obj)
        }
      }
    }
    BuiltinFunction::ObjectValues => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          object_key_values(obj, OBJECT_KEYS_KIND_VALUES)
        }
      }
    }
    BuiltinFunction::ObjectEntries => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      match target {
        Null => throw_type_error("not an object")
        Undefined => throw_type_error("not an object")
        _ => {
          let obj = if is_object_like(target) {
            target
          } else {
            to_object(target)
          }
          object_key_values(obj, OBJECT_KEYS_KIND_ENTRIES)
        }
      }
    }
    BuiltinFunction::ObjectGroupBy => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let callback = if args.length() > 1 { args[1] } else { Undefined }
      object_group_by(iterable, callback)
    }
    BuiltinFunction::ObjectIs => {
      let left = if args.is_empty() { Undefined } else { args[0] }
      let right = if args.length() > 1 { args[1] } else { Undefined }
      Bool(same_value(left, right))
    }
    BuiltinFunction::ObjectIsExtensible => {
      if args.is_empty() {
        return Bool(false)
      }
      let target = args[0]
      if is_object_like(target) {
        Bool(is_extensible_value_checked(target))
      } else {
        Bool(false)
      }
    }
    BuiltinFunction::ObjectPreventExtensions => {
      if args.is_empty() {
        return Undefined
      }
      let target = args[0]
      if is_object_like(target) {
        if !prevent_extensions_value(target) {
          return throw_type_error(
            "proxy preventExtensions handler returned false",
          )
        }
        target
      } else {
        target
      }
    }
    BuiltinFunction::ObjectSeal => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      object_seal(target, false)
    }
    BuiltinFunction::ObjectIsSealed => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      Bool(object_is_sealed(target, false))
    }
    BuiltinFunction::ObjectIsFrozen => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      Bool(object_is_sealed(target, true))
    }
    BuiltinFunction::ObjectFreeze => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      object_seal(target, true)
    }
    BuiltinFunction::ObjectFromEntries => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      object_from_entries(iterable)
    }
    BuiltinFunction::ObjectAssign => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = to_object(args[0])
      if args.length() > 1 {
        for i = 1; i < args.length(); i = i + 1 {
          let source = args[i]
          if source is Null || source is Undefined {
            continue
          }
          let from = to_object(source)
          let keys = own_property_keys_values(from)
          for key in keys {
            match key {
              String(name) => {
                let desc = get_own_property_descriptor(from, name)
                match desc {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc, "enumerable")) {
                      let value = property_get(from, name)
                      let _ = property_set(target, name, value, true)

                    }
                }
              }
              Symbol(symbol) => {
                let name = symbol_prop_key(symbol)
                let desc = get_own_property_descriptor(from, name)
                match desc {
                  Undefined => ()
                  _ =>
                    if !is_object_like(desc) {
                      let _ = throw_type_error("invalid property descriptor")

                    } else if is_truthy(property_get(desc, "enumerable")) {
                      let value = property_get(from, name)
                      let _ = property_set(target, name, value, true)

                    }
                }
              }
              _ => ()
            }
          }
        }
      }
      target
    }
    BuiltinFunction::ObjectHasOwn => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      let obj = to_object(target)
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let name = property_key_name(key)
      Bool(has_own_property(obj, name))
    }
    BuiltinFunction::ObjectToString =>
      match builtin.this_value {
        Some(value) => String(object_to_string(value))
        None => String(object_to_string(Undefined))
      }
    BuiltinFunction::ObjectToLocaleString => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let obj = to_object(target)
      let func = property_get_with_receiver(obj, "toString", target)
      if !is_callable(func) {
        return throw_type_error("not a function")
      }
      call_value_with_this(func, [], target)
    }
    BuiltinFunction::ObjectValueOf => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      to_object(target)
    }
    BuiltinFunction::ObjectHasOwnProperty => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      let name = property_key_name(key)
      let obj = to_object(target)
      Bool(has_own_property(obj, name))
    }
    BuiltinFunction::ObjectPropertyIsEnumerable => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      let name = property_key_name(key)
      let obj = to_object(target)
      let desc = get_own_property_descriptor(obj, name)
      match desc {
        Undefined => Bool(false)
        _ =>
          if !is_object_like(desc) {
            throw_type_error("invalid property descriptor")
          } else {
            Bool(is_truthy(property_get(desc, "enumerable")))
          }
      }
    }
    BuiltinFunction::ObjectIsPrototypeOf => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let value = if args.is_empty() { Undefined } else { args[0] }
      if !is_object_like(value) {
        return Bool(false)
      }
      let obj = to_object(target)
      let mut current = value
      while true {
        match get_proto_of_value_checked(current) {
          Some(proto) => {
            if strict_eq(proto, obj) {
              return Bool(true)
            }
            current = proto
          }
          None => return Bool(false)
        }
      }
      Bool(false)
    }
    BuiltinFunction::ObjectProtoGet => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      object_proto_get(target)
    }
    BuiltinFunction::ObjectProtoSet => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let proto_value = if args.is_empty() { Undefined } else { args[0] }
      object_proto_set(target, proto_value)
    }
    BuiltinFunction::ObjectDefineGetter => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      let getter = if args.length() > 1 { args[1] } else { Undefined }
      object_define_accessor(target, key, getter, false)
    }
    BuiltinFunction::ObjectDefineSetter => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      let setter = if args.length() > 1 { args[1] } else { Undefined }
      object_define_accessor(target, key, setter, true)
    }
    BuiltinFunction::ObjectLookupGetter => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      object_lookup_accessor(target, key, false)
    }
    BuiltinFunction::ObjectLookupSetter => {
      let target = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let key = if args.is_empty() { Undefined } else { args[0] }
      object_lookup_accessor(target, key, true)
    }
    BuiltinFunction::ReflectApply => {
      if args.length() < 3 {
        return throw_type_error("invalid arguments")
      }
      let target = args[0]
      if !is_callable(target) {
        return throw_type_error("not a function")
      }
      let this_arg = args[1]
      let array_arg = args[2]
      if !is_object_like(array_arg) {
        return throw_type_error("invalid arguments")
      }
      let argv = build_arg_list(array_arg)
      call_value_with_this(target, argv, this_arg)
    }
    BuiltinFunction::ReflectConstruct => {
      let target = if args.is_empty() { Undefined } else { args[0] }
      let array_arg = if args.length() > 1 { args[1] } else { Undefined }
      let new_target = if args.length() > 2 { args[2] } else { target }
      if !is_constructor_value(target) {
        return throw_not_constructor(target)
      }
      if !is_constructor_value(new_target) {
        return throw_not_constructor(new_target)
      }
      if !is_object_like(array_arg) {
        return throw_type_error("invalid arguments")
      }
      let argv = build_arg_list(array_arg)
      call_constructor(target, argv, new_target~)
    }
    BuiltinFunction::ReflectGet => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let receiver = if args.length() > 2 { args[2] } else { target }
      let name = property_key_name(key)
      property_get_with_receiver(target, name, receiver)
    }
    BuiltinFunction::ReflectGetOwnPropertyDescriptor => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let name = property_key_name(key)
      get_own_property_descriptor(target, name)
    }
    BuiltinFunction::ReflectGetPrototypeOf => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      match get_proto_of_value_checked(target) {
        Some(proto) => proto
        None => Null
      }
    }
    BuiltinFunction::ReflectDefineProperty => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let desc = if args.length() > 2 { args[2] } else { Undefined }
      Bool(define_property_value(target, key, desc, false))
    }
    BuiltinFunction::ReflectHas => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let name = property_key_name(key)
      Bool(has_property_value(target, name))
    }
    BuiltinFunction::ReflectIsExtensible => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      Bool(is_extensible_value_checked(target))
    }
    BuiltinFunction::ReflectSet => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let value = if args.length() > 2 { args[2] } else { Undefined }
      let receiver = if args.length() > 3 { args[3] } else { target }
      let name = property_key_name(key)
      Bool(set_value_with_receiver(target, name, value, receiver))
    }
    BuiltinFunction::ReflectSetPrototypeOf => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let proto_value = if args.length() > 1 { args[1] } else { Undefined }
      match proto_value {
        _ if is_object_like(proto_value) =>
          Bool(set_proto_of_value(target, Some(proto_value), false))
        Null => Bool(set_proto_of_value(target, None, false))
        _ => throw_type_error("not a prototype")
      }
    }
    BuiltinFunction::ReflectDeleteProperty => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let key = if args.length() > 1 { args[1] } else { Undefined }
      let name = property_key_name(key)
      Bool(delete_property_value(target, name))
    }
    BuiltinFunction::ReflectPreventExtensions => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      Bool(prevent_extensions_value(target))
    }
    BuiltinFunction::ReflectOwnKeys => {
      if args.is_empty() {
        return throw_type_error("not an object")
      }
      let target = args[0]
      if !is_object_like(target) {
        return throw_type_error("not an object")
      }
      let values : Array[Value?] = match target {
        Object(raw) =>
          match raw.proxy_data {
            Some(data) => {
              let keys = proxy_own_keys_values(data)
              let items : Array[Value?] = []
              for key in keys {
                items.push(Some(key))
              }
              items
            }
            None => {
              let keys = own_string_keys(target)
              let symbols = own_symbol_keys(target, true)
              let items : Array[Value?] = []
              for name in keys {
                items.push(Some(String(name)))
              }
              for symbol in symbols {
                items.push(Some(Symbol(symbol)))
              }
              items
            }
          }
        _ => {
          let keys = own_string_keys(target)
          let symbols = own_symbol_keys(target, true)
          let items : Array[Value?] = []
          for name in keys {
            items.push(Some(String(name)))
          }
          for symbol in symbols {
            items.push(Some(Symbol(symbol)))
          }
          items
        }
      }
      new_array_value(values)
    }
    _ => throw_type_error("invalid builtin")
  }
}
