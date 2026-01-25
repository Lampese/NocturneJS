///|
fn new_message_event(data : Value) -> Value {
  let obj = new_object_value()
  match obj {
    Object(event) => props_set(event.props, "data", property_data(data))
    _ => ()
  }
  obj
}

///|
fn worker_port_info(this_value : Value?) -> (Int, Bool) raise {
  match this_value {
    Some(Object(obj)) => {
      let id_value = property_get(Object(obj), "__worker_id")
      let port_value = property_get(Object(obj), "__worker_port")
      match (id_value, port_value) {
        (Number(id_num), Number(port_num)) => {
          let worker_id = to_int32(id_num)
          let to_main = Double::trunc(port_num) != 0.0
          (worker_id, to_main)
        }
        _ => {
          let _ = throw_type_error("invalid worker")
          (0, false)
        }
      }
    }
    _ => {
      let _ = throw_type_error("invalid worker")
      (0, false)
    }
  }
}

///|
fn new_worker_port_value(worker_id : Int, to_main : Bool) -> Value {
  let obj = new_object_value()
  match obj {
    Object(port) => {
      props_set(port.props, 
        "__worker_id",
        property_data_non_enum(Number(Double::from_int(worker_id))),
      )
      let port_kind = if to_main { 1.0 } else { 0.0 }
      props_set(port.props, "__worker_port", property_data_non_enum(Number(port_kind)))
      props_set(port.props, 
        "postMessage",
        property_data_non_enum(
          new_builtin_value(BuiltinFunction::WorkerPostMessage),
        ),
      )
      props_set(port.props, "onmessage", property_data_non_enum(Null))
    }
    _ => ()
  }
  obj
}

///|
fn deliver_worker_message_main(
  state : WorkerState,
  message : Value,
) -> Unit raise {
  let handler = property_get(Object(state.worker_obj), "onmessage")
  if is_callable(handler) {
    let event = new_message_event(message)
    let result = call_value_with_this(
      handler,
      [event],
      Object(state.worker_obj),
    )
    gc_maybe_collect_with_root(result)
  }
}

///|
fn deliver_worker_message_worker(
  state : WorkerState,
  message : Value,
) -> Unit raise {
  let handler = property_get(Object(state.parent_obj), "onmessage")
  if is_callable(handler) {
    let event = new_message_event(message)
    let result = with_root_env(state.env, fn() raise {
      call_value_with_this(handler, [event], Object(state.parent_obj))
    })
    gc_maybe_collect_with_root(result)
  }
}

///|
fn clone_worker_value(value : Value) -> Value raise {
  let seen : Map[Int, Value] = Map::new()
  clone_worker_value_inner(value, seen)
}

///|
fn clone_array_buffer_object(
  obj : ObjectValue,
  data : ArrayBufferData,
  seen : Map[Int, Value],
) -> ObjectValue {
  match seen.get(obj.id) {
    Some(Object(existing)) => return existing
    Some(_) => return obj
    None => ()
  }
  if data.shared {
    let value = Object(obj)
    seen.set(obj.id, value)
    return obj
  }
  let clone_value = new_array_buffer_value_with_options(
    data.bytes.length(),
    data.max_byte_length,
    data.resizable,
  )
  seen.set(obj.id, clone_value)
  match clone_value {
    Object(new_obj) =>
      match new_obj.array_buffer_data {
        Some(new_data) => {
          let mut i = 0
          while i < data.bytes.length() {
            new_data.bytes[i] = data.bytes[i]
            i = i + 1
          }
          new_obj
        }
        None => new_obj
      }
    _ => new_object_struct(None)
  }
}

///|
fn clone_worker_value_inner(
  value : Value,
  seen : Map[Int, Value],
) -> Value raise {
  match value {
    Undefined | Null | Bool(_) | Number(_) | String(_) | BigInt(_) => value
    Symbol(_) => {
      let _ = throw_type_error("unsupported type")
      Undefined
    }
    Function(_) | Builtin(_) | BoundFunction(_) | Arguments(_) => {
      let _ = throw_type_error("unsupported type")
      Undefined
    }
    Array(arr) =>
      match seen.get(arr.id) {
        Some(existing) => existing
        None =>
          match arr.typed_array_data {
            Some(data) => {
              let buffer_obj = match data.buffer.array_buffer_data {
                Some(buf) => clone_array_buffer_object(data.buffer, buf, seen)
                None => data.buffer
              }
              let clone_value = new_typed_array_view(
                data.kind,
                buffer_obj,
                data.byte_offset,
                data.length,
                data.length_tracking,
              )
              seen.set(arr.id, clone_value)
              clone_value
            }
            None => {
              let len = arr.elements.length()
              let elements : Array[Value?] = []
              let mut i = 0
              while i < len {
                elements.push(None)
                i = i + 1
              }
              let clone_value = new_array_value(elements)
              seen.set(arr.id, clone_value)
              match clone_value {
                Array(target) => {
                  let mut j = 0
                  while j < len {
                    match arr.elements[j] {
                      Some(entry) =>
                        target.elements[j] = Some(
                          clone_worker_value_inner(entry, seen),
                        )
                      None => ()
                    }
                    j = j + 1
                  }
                }
                _ => ()
              }
              clone_value
            }
          }
      }
    Object(obj) =>
      match obj.array_buffer_data {
        Some(data) => {
          let clone_obj = clone_array_buffer_object(obj, data, seen)
          Object(clone_obj)
        }
        None =>
          match obj.date_data {
            Some(value) => new_date_value(value)
            None =>
              match obj.string_data {
                Some(text) => new_string_object(text)
                None =>
                  match obj.number_data {
                    Some(num) => new_number_object(num)
                    None =>
                      match obj.bool_data {
                        Some(flag) => new_bool_object(flag)
                        None =>
                          match obj.bigint_data {
                            Some(num) => new_bigint_object(num)
                            None =>
                              match obj.symbol_data {
                                Some(sym) => new_symbol_object(sym)
                                None =>
                                  match
                                    (
                                      obj.regexp_data,
                                      obj.generator_data,
                                      obj.async_generator_data,
                                      obj.proxy_data,
                                      obj.map_data,
                                      obj.set_data,
                                      obj.map_iter_data,
                                      obj.set_iter_data,
                                      obj.weakmap_data,
                                      obj.weakset_data,
                                      obj.weakref_data,
                                      obj.finreg_data,
                                    ) {
                                    (Some(_), _, _, _, _, _, _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, Some(_), _, _, _, _, _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, Some(_), _, _, _, _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, Some(_), _, _, _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, Some(_), _, _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, Some(_), _, _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, Some(_), _, _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, _, Some(_), _, _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, _, _, Some(_), _, _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, _, _, _, Some(_), _, _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, _, _, _, _, Some(_), _) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    (_, _, _, _, _, _, _, _, _, _, _, Some(_)) => {
                                      let _ = throw_type_error(
                                        "unsupported type",
                                      )
                                      Undefined
                                    }
                                    _ =>
                                      match seen.get(obj.id) {
                                        Some(existing) => existing
                                        None => {
                                          let clone_value = new_object_value_with_proto(
                                            obj.proto,
                                          )
                                          seen.set(obj.id, clone_value)
                                          match clone_value {
                                            Object(target) =>
                                              props_iter_raise(obj.props, fn(key, prop) raise {
                                                if is_symbol_prop_key(key) ||
                                                  is_private_prop_key(key) ||
                                                  !prop.enumerable {
                                                  return ()
                                                }
                                                let cloned = clone_worker_value_inner(
                                                  prop.value,
                                                  seen,
                                                )
                                                props_set(target.props, 
                                                  key,
                                                  property_data(cloned),
                                                )
                                              })
                                            _ => ()
                                          }
                                          clone_value
                                        }
                                      }
                                  }
                              }
                          }
                      }
                  }
              }
          }
      }
  }
}

///|
fn set_worker_parent(env : Env, parent_value : Value) -> Unit raise {
  let os_value = env_get(env, "os")
  match os_value {
    Object(os_obj) => {
      let worker_ctor = property_get(Object(os_obj), "Worker")
      let _ = property_set(worker_ctor, "parent", parent_value, env.strict)

    }
    _ => ()
  }
}

///|
fn create_worker_from_args(args : Array[Value]) -> Value raise {
  if args.is_empty() {
    return throw_type_error("missing filename")
  }
  let path = to_string_value(args[0])
  let full_path = resolve_path_from_source(path)
  let source = @fs.read_file_to_string(full_path) catch {
    _ => {
      let _ = throw_reference_error("could not load '\{path}'")
      ""
    }
  }
  let script = parse_script_with_pos(source)
  let worker_id = next_worker_id()
  let worker_value = new_worker_port_value(worker_id, false)
  let parent_value = new_worker_port_value(worker_id, true)
  let worker_env = clone_root_env()
  let worker_obj = match worker_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  let parent_obj = match parent_value {
    Object(obj) => obj
    _ => new_object_struct(None)
  }
  let state = WorkerState::{
    id: worker_id,
    env: worker_env,
    worker_obj,
    parent_obj,
    queue_to_main: [],
    queue_to_worker: [],
  }
  worker_state_set(state)
  let _ = set_worker_parent(worker_env, parent_value)
  let _ = with_root_env(worker_env, fn() raise {
    if script.is_module {
      let _ = eval_module_script(script, full_path)
      Undefined
    } else {
      with_source_path(full_path, fn() raise { eval_script(worker_env, script) })
    }
  })
  worker_value
}
