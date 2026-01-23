///|
fn call_builtin_collection(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::MapConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::MapGroupBy => {
      let iterable = if args.is_empty() { Undefined } else { args[0] }
      let callback = if args.length() > 1 { args[1] } else { Undefined }
      map_group_by(iterable, callback)
    }
    BuiltinFunction::MapGet => {
      let (_, data) = require_map(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      map_get_value(data, key)
    }
    BuiltinFunction::MapGetOrInsert => {
      let (_, data) = require_map(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      let value = if args.length() > 1 { args[1] } else { Undefined }
      map_get_or_insert(data, key, value, false)
    }
    BuiltinFunction::MapGetOrInsertComputed => {
      let (_, data) = require_map(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      let callback = if args.length() > 1 { args[1] } else { Undefined }
      map_get_or_insert(data, key, callback, true)
    }
    BuiltinFunction::MapSet => {
      let (obj, data) = require_map(builtin.this_value)
      if args.length() >= 2 {
        map_set_value(data, args[0], args[1])
      } else if args.length() == 1 {
        map_set_value(data, args[0], Undefined)
      } else {
        map_set_value(data, Undefined, Undefined)
      }
      Object(obj)
    }
    BuiltinFunction::MapHas => {
      let (_, data) = require_map(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      Bool(map_has_key(data, key))
    }
    BuiltinFunction::MapDelete => {
      let (_, data) = require_map(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      Bool(map_delete_key(data, key))
    }
    BuiltinFunction::MapClear => {
      let (_, data) = require_map(builtin.this_value)
      let mut index = 0
      while index < data.entries.length() {
        match data.entries[index] {
          Some((key, value)) => {
            rc_decref_value(key)
            rc_decref_value(value)
          }
          None => ()
        }
        data.entries[index] = None
        index = index + 1
      }
      Undefined
    }
    BuiltinFunction::MapForEach => {
      let (obj, data) = require_map(builtin.this_value)
      if args.is_empty() {
        return Undefined
      }
      let callback = args[0]
      if !is_callable(callback) {
        return throw_type_error("not a function")
      }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      let mut index = 0
      while index < data.entries.length() {
        match data.entries[index] {
          Some((key, value)) => {
            let _ = call_value_with_this(
              callback,
              [value, key, Object(obj)],
              this_arg,
            )

          }
          None => ()
        }
        index = index + 1
      }
      Undefined
    }
    BuiltinFunction::MapSize => {
      let (_, data) = require_map(builtin.this_value)
      Number(Double::from_int(map_entry_count(data)))
    }
    BuiltinFunction::MapKeys => {
      let (_, data) = require_map(builtin.this_value)
      new_map_iterator(data, 0)
    }
    BuiltinFunction::MapValues => {
      let (_, data) = require_map(builtin.this_value)
      new_map_iterator(data, 1)
    }
    BuiltinFunction::MapEntries => {
      let (_, data) = require_map(builtin.this_value)
      new_map_iterator(data, 2)
    }
    BuiltinFunction::MapIteratorNext => map_iterator_next(builtin.this_value)
    BuiltinFunction::SetConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::SetAdd => {
      let (obj, data) = require_set(builtin.this_value)
      let value = if args.is_empty() { Undefined } else { args[0] }
      set_add_value(data, value)
      Object(obj)
    }
    BuiltinFunction::SetHas => {
      let (_, data) = require_set(builtin.this_value)
      let value = if args.is_empty() { Undefined } else { args[0] }
      Bool(set_has_value(data, value))
    }
    BuiltinFunction::SetDelete => {
      let (_, data) = require_set(builtin.this_value)
      let value = if args.is_empty() { Undefined } else { args[0] }
      Bool(set_delete_value(data, value))
    }
    BuiltinFunction::SetClear => {
      let (_, data) = require_set(builtin.this_value)
      let mut index = 0
      while index < data.entries.length() {
        match data.entries[index] {
          Some(entry) => rc_decref_value(entry)
          None => ()
        }
        data.entries[index] = None
        index = index + 1
      }
      Undefined
    }
    BuiltinFunction::SetForEach => {
      let (obj, data) = require_set(builtin.this_value)
      if args.is_empty() {
        return Undefined
      }
      let callback = args[0]
      if !is_callable(callback) {
        return throw_type_error("not a function")
      }
      let this_arg = if args.length() > 1 { args[1] } else { Undefined }
      let mut index = 0
      while index < data.entries.length() {
        match data.entries[index] {
          Some(entry) => {
            let _ = call_value_with_this(
              callback,
              [entry, entry, Object(obj)],
              this_arg,
            )

          }
          None => ()
        }
        index = index + 1
      }
      Undefined
    }
    BuiltinFunction::SetSize => {
      let (_, data) = require_set(builtin.this_value)
      Number(Double::from_int(set_entry_count(data)))
    }
    BuiltinFunction::SetValues => {
      let (_, data) = require_set(builtin.this_value)
      new_set_iterator(data, 0)
    }
    BuiltinFunction::SetEntries => {
      let (_, data) = require_set(builtin.this_value)
      new_set_iterator(data, 1)
    }
    BuiltinFunction::SetIsDisjointFrom => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_is_disjoint_from(builtin.this_value, other)
    }
    BuiltinFunction::SetIsSubsetOf => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_is_subset_of(builtin.this_value, other)
    }
    BuiltinFunction::SetIsSupersetOf => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_is_superset_of(builtin.this_value, other)
    }
    BuiltinFunction::SetIntersection => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_intersection(builtin.this_value, other)
    }
    BuiltinFunction::SetDifference => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_difference(builtin.this_value, other)
    }
    BuiltinFunction::SetSymmetricDifference => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_symmetric_difference(builtin.this_value, other)
    }
    BuiltinFunction::SetUnion => {
      let other = if args.is_empty() { Undefined } else { args[0] }
      set_union(builtin.this_value, other)
    }
    BuiltinFunction::SetIteratorNext => set_iterator_next(builtin.this_value)
    BuiltinFunction::WeakMapConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::WeakMapGet => {
      let (_, data) = require_weakmap(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(key) {
        return Undefined
      }
      weakmap_get_value(data, key)
    }
    BuiltinFunction::WeakMapGetOrInsert => {
      let (_, data) = require_weakmap(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(key) {
        return throw_type_error("invalid WeakMap key")
      }
      let value = if args.length() > 1 { args[1] } else { Undefined }
      weakmap_get_or_insert(data, key, value, false)
    }
    BuiltinFunction::WeakMapGetOrInsertComputed => {
      let (_, data) = require_weakmap(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(key) {
        return throw_type_error("invalid WeakMap key")
      }
      let callback = if args.length() > 1 { args[1] } else { Undefined }
      weakmap_get_or_insert(data, key, callback, true)
    }
    BuiltinFunction::WeakMapSet => {
      let (obj, data) = require_weakmap(builtin.this_value)
      if args.is_empty() {
        return throw_type_error("invalid WeakMap key")
      }
      let key = args[0]
      if !is_weakref_target(key) {
        return throw_type_error("invalid WeakMap key")
      }
      let value = if args.length() > 1 { args[1] } else { Undefined }
      weakmap_set_value(data, key, value)
      Object(obj)
    }
    BuiltinFunction::WeakMapHas => {
      let (_, data) = require_weakmap(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(key) {
        return Bool(false)
      }
      Bool(weakmap_has_key(data, key))
    }
    BuiltinFunction::WeakMapDelete => {
      let (_, data) = require_weakmap(builtin.this_value)
      let key = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(key) {
        return Bool(false)
      }
      Bool(weakmap_delete_key(data, key))
    }
    BuiltinFunction::WeakSetConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::WeakSetAdd => {
      let (obj, data) = require_weakset(builtin.this_value)
      if args.is_empty() || !is_weakref_target(args[0]) {
        return throw_type_error("invalid WeakSet key")
      }
      weakset_add_value(data, args[0])
      Object(obj)
    }
    BuiltinFunction::WeakSetHas => {
      let (_, data) = require_weakset(builtin.this_value)
      let value = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(value) {
        return Bool(false)
      }
      Bool(weakset_has_value(data, value))
    }
    BuiltinFunction::WeakSetDelete => {
      let (_, data) = require_weakset(builtin.this_value)
      let value = if args.is_empty() { Undefined } else { args[0] }
      if !is_weakref_target(value) {
        return Bool(false)
      }
      Bool(weakset_delete_value(data, value))
    }
    BuiltinFunction::WeakRefConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::WeakRefDeref => {
      let data = require_weakref(builtin.this_value)
      match data.target {
        Undefined => Undefined
        _ =>
          if is_live_value_now(data.target) {
            data.target
          } else {
            Undefined
          }
      }
    }
    BuiltinFunction::FinalizationRegistryConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::FinalizationRegistryRegister => {
      let data = require_finreg(builtin.this_value)
      if args.is_empty() {
        return throw_type_error("invalid target")
      }
      let target = args[0]
      if !is_weakref_target(target) {
        return throw_type_error("invalid target")
      }
      let held = if args.length() > 1 { args[1] } else { Undefined }
      if strict_eq(target, held) {
        return throw_type_error("held value cannot be the target")
      }
      let token = if args.length() >= 3 { args[2] } else { Undefined }
      match token {
        Undefined => ()
        _ =>
          if !is_weakref_target(token) {
            return throw_type_error("invalid unregister token")
          }
      }
      data.entries.push(FinalizationEntry::{ target, held_value: held, token })
      Undefined
    }
    BuiltinFunction::FinalizationRegistryUnregister => {
      let data = require_finreg(builtin.this_value)
      if args.is_empty() {
        return Bool(false)
      }
      let token = args[0]
      if !is_weakref_target(token) {
        return throw_type_error("invalid unregister token")
      }
      let mut removed = false
      let mut i = 0
      while i < data.entries.length() {
        let entry = data.entries[i]
        if strict_eq(entry.token, token) {
          let _ = data.entries.remove(i)
          removed = true
        } else {
          i = i + 1
        }
      }
      Bool(removed)
    }
    _ => throw_type_error("invalid builtin")
  }
}
