///|
fn destructure_array_pattern(
  env : Env,
  pattern : Array[@engine.ArrayPatternElem],
  value : Value,
  assign : (String, Value) -> Unit raise,
) -> Unit raise {
  let _ = with_gc_frame(fn() raise {
    let (iterator, next_method) = get_iterator_from_value(value)
    let _ = gc_root_push(iterator)
    let _ = gc_root_push(next_method)
    let mut done = false
    for elem in pattern {
      match elem {
        @engine.ArrayPatternElem::Hole =>
          if !done {
            let (is_done, _) = iterator_step_value(iterator, next_method)
            done = is_done
          }
        @engine.ArrayPatternElem::Bind(binding, default) => {
          let mut target_info : (Array[Value], (Value) -> Unit raise)? = None
          match binding {
            @engine.VarBinding::Target(expr) =>
              target_info = Some(
                prepare_assignment_target_with_roots(env, expr) catch {
                  err => {
                    if !done {
                      iterator_close_on_error(iterator)
                    }
                    raise err
                  }
                },
              )
            _ => ()
          }
          let mut element = Undefined
          if !done {
            let (is_done, item) = iterator_step_value(iterator, next_method)
            done = is_done
            if !is_done {
              element = item
            }
          }
          try {
            match target_info {
              Some(info) => {
                let _ = with_gc_frame(fn() raise {
                  let (roots, assigner) = info
                  for root in roots {
                    let _ = gc_root_push(root)

                  }
                  match default {
                    Some(expr) =>
                      if element is Undefined {
                        let value = match binding {
                          @engine.VarBinding::Name(name) =>
                            eval_named_expr(env, expr, name, None)
                          _ => eval_expr(env, expr)
                        }
                        element = value
                      }
                    None => ()
                  }
                  let _ = gc_root_push(element)
                  assigner(element)
                  Undefined
                })

              }
              None => {
                let _ = with_gc_frame(fn() raise {
                  match default {
                    Some(expr) =>
                      if element is Undefined {
                        let value = match binding {
                          @engine.VarBinding::Name(name) =>
                            eval_named_expr(env, expr, name, None)
                          _ => eval_expr(env, expr)
                        }
                        element = value
                      }
                    None => ()
                  }
                  let _ = gc_root_push(element)
                  assign_binding_value(env, binding, element, assign)
                  Undefined
                })

              }
            }
          } catch {
            err => {
              if !done {
                iterator_close_on_error(iterator)
              }
              raise err
            }
          }
        }
        @engine.ArrayPatternElem::Rest(binding) => {
          let mut target_info : (Array[Value], (Value) -> Unit raise)? = None
          match binding {
            @engine.VarBinding::Target(expr) =>
              target_info = Some(
                prepare_assignment_target_with_roots(env, expr) catch {
                  err => {
                    if !done {
                      iterator_close_on_error(iterator)
                    }
                    raise err
                  }
                },
              )
            _ => ()
          }
          let rest_values : Array[Value?] = []
          if !done {
            for {
              let (is_done, item) = iterator_step_value(iterator, next_method)
              if is_done {
                done = true
                break
              }
              rest_values.push(Some(item))
            }
          }
          try {
            match target_info {
              Some(info) => {
                let _ = with_gc_frame(fn() raise {
                  let (roots, assigner) = info
                  for root in roots {
                    let _ = gc_root_push(root)

                  }
                  let rest_value = new_array_value(rest_values)
                  let _ = gc_root_push(rest_value)
                  assigner(rest_value)
                  Undefined
                })

              }
              None => {
                let _ = with_gc_frame(fn() raise {
                  let rest_value = new_array_value(rest_values)
                  let _ = gc_root_push(rest_value)
                  assign_binding_value(env, binding, rest_value, assign)
                  Undefined
                })

              }
            }
          } catch {
            err => {
              if !done {
                iterator_close_on_error(iterator)
              }
              raise err
            }
          }
          done = true
        }
      }
    }
    if !done {
      iterator_close(iterator)
    }
    Undefined
  })

}

///|
fn own_enumerable_string_keys(target : Value) -> Array[String] raise {
  let names_value = object_keys(target)
  match names_value {
    Array(arr) => {
      let names : Array[String] = []
      for entry in arr.elements {
        match entry {
          Some(String(name)) => names.push(name)
          _ => ()
        }
      }
      names
    }
    _ => []
  }
}

///|
fn own_string_keys(target : Value) -> Array[String] raise {
  let names_value = object_get_own_property_names(target)
  match names_value {
    Array(arr) => {
      let names : Array[String] = []
      for entry in arr.elements {
        match entry {
          Some(String(name)) => names.push(name)
          _ => ()
        }
      }
      names
    }
    _ => []
  }
}

///|
fn enumerable_string_keys_in_chain(target : Value) -> Array[String] raise {
  let mut current = to_object(target)
  let names : Array[String] = []
  let seen : Map[String, Bool] = Map::new()
  while true {
    let enum_keys = own_enumerable_string_keys(current)
    for key in enum_keys {
      if !seen.contains(key) {
        seen.set(key, true)
        names.push(key)
      }
    }
    let all_keys = own_string_keys(current)
    for key in all_keys {
      if !seen.contains(key) {
        seen.set(key, true)
      }
    }
    let proto = get_proto_of_value(current)
    match proto {
      Some(obj) => current = obj
      None => break
    }
  }
  names
}

///|
fn is_enumerable_key_in_chain(target : Value, key : String) -> Bool raise {
  let mut current = to_object(target)
  while true {
    let desc = get_own_property_descriptor(current, key)
    match desc {
      Undefined => ()
      _ =>
        if !is_object_like(desc) {
          let _ = throw_type_error("invalid property descriptor")
          return false
        } else {
          let enumerable_value = property_get(desc, "enumerable")
          return is_truthy(enumerable_value)
        }
    }
    let proto = get_proto_of_value(current)
    match proto {
      Some(obj) => current = obj
      None => return false
    }
  }
  false
}

///|
fn destructure_object_pattern(
  env : Env,
  pattern : Array[@engine.ObjectPatternElem],
  value : Value,
  assign : (String, Value) -> Unit raise,
) -> Unit raise {
  let _ = with_gc_frame(fn() raise {
    let obj = to_object(value)
    let _ = gc_root_push(obj)
    let bound : Array[String] = []
    for elem in pattern {
      match elem {
        @engine.ObjectPatternElem::Bind(key, binding, default) => {
          let key_name = match key {
            @engine.ObjKey::Computed(expr) => {
              let base = gc_root_depth()
              try {
                let key_value = eval_expr(env, expr)
                let _ = gc_root_push(key_value)
                let name = property_key_name(key_value)
                gc_root_pop_to(base)
                name
              } catch {
                err => {
                  gc_root_pop_to(base)
                  raise err
                }
              }
            }
            _ => obj_key_simple_name(key)
          }
          bound.push(key_name)
          let mut target_info : (Array[Value], (Value) -> Unit raise)? = None
          match binding {
            @engine.VarBinding::Target(expr) =>
              target_info = Some(
                prepare_assignment_target_with_roots(env, expr),
              )
            _ => ()
          }
          let mut with_obj : Value? = None
          let mut with_has : Bool? = None
          match binding {
            @engine.VarBinding::Name(name) =>
              match env.with_object {
                Some(obj) => {
                  with_obj = Some(obj)
                  with_has = Some(has_property_value(obj, name))
                }
                None => ()
              }
            _ => ()
          }
          let mut prop_value = property_get(obj, key_name)
          match target_info {
            Some(info) => {
              let _ = with_gc_frame(fn() raise {
                let (roots, assigner) = info
                for root in roots {
                  let _ = gc_root_push(root)

                }
                match default {
                  Some(expr) =>
                    if prop_value is Undefined {
                      let value = match binding {
                        @engine.VarBinding::Name(name) =>
                          eval_named_expr(env, expr, name, None)
                        _ => eval_expr(env, expr)
                      }
                      prop_value = value
                    }
                  None => ()
                }
                let _ = gc_root_push(prop_value)
                assigner(prop_value)
                Undefined
              })

            }
            None => {
              let _ = with_gc_frame(fn() raise {
                match default {
                  Some(expr) =>
                    if prop_value is Undefined {
                      let value = match binding {
                        @engine.VarBinding::Name(name) =>
                          eval_named_expr(env, expr, name, None)
                        _ => eval_expr(env, expr)
                      }
                      prop_value = value
                    }
                  None => ()
                }
                let _ = gc_root_push(prop_value)
                match binding {
                  @engine.VarBinding::Name(name) =>
                    match with_obj {
                      Some(target) =>
                        match with_has {
                          Some(has_with) =>
                            if has_with {
                              let _ = property_set(
                                target,
                                name,
                                prop_value,
                                env.strict,
                              )

                            } else {
                              env_set_with_strict_no_with(
                                env,
                                name,
                                prop_value,
                                env.strict,
                              )
                            }
                          None =>
                            assign_binding_value(
                              env, binding, prop_value, assign,
                            )
                        }
                      None =>
                        assign_binding_value(env, binding, prop_value, assign)
                    }
                  _ => assign_binding_value(env, binding, prop_value, assign)
                }
                Undefined
              })

            }
          }
        }
        @engine.ObjectPatternElem::Rest(binding) => {
          let mut target_info : (Array[Value], (Value) -> Unit raise)? = None
          match binding {
            @engine.VarBinding::Target(expr) =>
              target_info = Some(
                prepare_assignment_target_with_roots(env, expr),
              )
            _ => ()
          }
          match target_info {
            Some(info) => {
              let _ = with_gc_frame(fn() raise {
                let (roots, assigner) = info
                for root in roots {
                  let _ = gc_root_push(root)

                }
                let rest_value = new_object_value()
                match rest_value {
                  Object(rest_obj) =>
                    copy_data_properties_excluding(rest_obj, obj, bound)
                  _ => ()
                }
                let _ = gc_root_push(rest_value)
                assigner(rest_value)
                Undefined
              })

            }
            None => {
              let _ = with_gc_frame(fn() raise {
                let rest_value = new_object_value()
                match rest_value {
                  Object(rest_obj) =>
                    copy_data_properties_excluding(rest_obj, obj, bound)
                  _ => ()
                }
                let _ = gc_root_push(rest_value)
                assign_binding_value(env, binding, rest_value, assign)
                Undefined
              })

            }
          }
        }
      }
    }
    Undefined
  })

}
