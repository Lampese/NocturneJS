///|
fn class_eval_expr(env : Env, expr : @engine.Expr) -> Value raise {
  if bytecode_class_eval_enabled() {
    bytecode_eval_expr(env, expr)
  } else {
    eval_expr(env, expr)
  }
}

///|
fn class_obj_key_property_name(
  env : Env,
  key : @engine.ObjKey,
) -> (String, Value?) raise {
  match key {
    @engine.ObjKey::Computed(expr) => {
      let value = class_eval_expr(env, expr)
      (property_key_name(value), Some(value))
    }
    _ => (obj_key_simple_name(key), None)
  }
}

///|
fn class_eval_block(env : Env, stmts : Array[@engine.Stmt]) -> Value raise {
  if bytecode_class_eval_enabled() {
    bytecode_eval_block(env, stmts)
  } else {
    eval_block(env, stmts)
  }
}

///|
fn gen_class_eval_expr_cont(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  if bytecode_class_eval_enabled() {
    gen_bytecode_eval_expr_cont(env, expr, k)
  } else {
    gen_eval_expr_cont(env, expr, k)
  }
}

///|
fn gen_class_eval_block(
  env : Env,
  stmts : Array[@engine.Stmt],
) -> GenStep raise {
  if bytecode_class_eval_enabled() {
    gen_bytecode_eval_block(env, stmts)
  } else {
    gen_eval_block(env, stmts)
  }
}

///|
fn eval_class(
  env : Env,
  class_def : @engine.ClassDef,
  name_hint? : String? = None,
) -> Value raise {
  let class_env = Env::new(Some(env))
  match class_def.name {
    Some(name) => {
      env_declare_uninitialized(class_env, name)
      class_env.readonly_bindings.set(name, true)
    }
    None => ()
  }
  let mut super_value : Value? = None
  match class_def.super_class {
    Some(expr) => super_value = Some(class_eval_expr(class_env, expr))
    None => ()
  }
  match super_value {
    Some(value) =>
      match value {
        Null => ()
        _ =>
          if !is_constructor_value(value) {
            let _ = throw_type_error("not a constructor")

          }
      }
    None => ()
  }
  for class_member in class_def.body {
    match class_member {
      @engine.ClassMember::Method(@engine.ObjKey::Private(name), _, _) =>
        if class_env.private_bindings.get(name) is None {
          ignore(env_define_private_name(class_env, name))
        }
      @engine.ClassMember::Getter(@engine.ObjKey::Private(name), _, _) =>
        if class_env.private_bindings.get(name) is None {
          ignore(env_define_private_name(class_env, name))
        }
      @engine.ClassMember::Setter(@engine.ObjKey::Private(name), _, _) =>
        if class_env.private_bindings.get(name) is None {
          ignore(env_define_private_name(class_env, name))
        }
      @engine.ClassMember::Field(@engine.ObjKey::Private(name), _, _) =>
        if class_env.private_bindings.get(name) is None {
          ignore(env_define_private_name(class_env, name))
        }
      _ => ()
    }
  }
  let mut constructor_def : @engine.FunctionDef? = None
  for class_member in class_def.body {
    match class_member {
      @engine.ClassMember::Method(@engine.ObjKey::Ident(name), func, is_static)
      | @engine.ClassMember::Method(
        @engine.ObjKey::String(name),
        func,
        is_static
      ) =>
        if !is_static && name == "constructor" {
          constructor_def = Some(func)
        }
      _ => ()
    }
  }
  let ctor_def = match constructor_def {
    Some(func) =>
      match class_def.name {
        Some(class_name) =>
          @engine.FunctionDef::new(
            Some(class_name),
            func.params,
            func.param_inits,
            func.body,
            true,
            func.is_generator,
            func.is_async,
            false,
            func.offset,
            func.end_offset,
          )
        None =>
          @engine.FunctionDef::new(
            None,
            func.params,
            func.param_inits,
            func.body,
            true,
            func.is_generator,
            func.is_async,
            false,
            func.offset,
            func.end_offset,
          )
      }
    None =>
      @engine.FunctionDef::new(
        class_def.name,
        ([] : Array[@engine.ParamBinding]),
        ([] : Array[@engine.Expr?]),
        ([] : Array[@engine.Stmt]),
        true,
        false,
        false,
        false,
        class_def.offset,
        class_def.end_offset,
      )
  }
  let ctor_func = to_function_value(class_env, ctor_def, false)
  ctor_func.is_class_constructor = true
  if class_def.name is None {
    match name_hint {
      Some(name) => set_function_name(ctor_func, name)
      None => ()
    }
  }
  match current_source() {
    Some(source) => {
      let end_offset = if class_def.end_offset > class_def.offset {
        class_def.end_offset
      } else {
        class_def.offset
      }
      ctor_func.source = Some(
        slice_source_range(source, class_def.offset, end_offset),
      )
    }
    None => ()
  }
  let field_env = Env::new(Some(class_env))
  env_set_local(field_env, "this", Value::Function(ctor_func))
  field_env.new_target = Undefined
  match super_value {
    Some(value) => {
      set_function_super_constructor(ctor_func, Some(value))
      match value {
        Null => ()
        _ => {
          rc_replace_optional_value(ctor_func.proto, Some(value))
          ctor_func.proto = Some(value)
        }
      }
    }
    None => ()
  }
  if constructor_def is None {
    match super_value {
      Some(_) => ctor_func.is_default_constructor = true
      None => ()
    }
  }
  ensure_function_prototype(ctor_func)
  let proto_value = property_get(Value::Function(ctor_func), "prototype")
  let proto_obj = match proto_value {
    Object(obj) => obj
    _ => {
      let fresh = new_object_value()
      match fresh {
        Object(obj) => {
          props_set(ctor_func.props, 
            "prototype",
            property_data_non_enum_non_config(fresh),
          )
          obj
        }
        _ => return Value::Function(ctor_func)
      }
    }
  }
  set_function_home_object(ctor_func, Some(Value::Object(proto_obj)))
  match super_value {
    Some(Null) => {
      rc_replace_optional_value(proto_obj.proto, None)
      proto_obj.proto = None
    }
    Some(value) => {
      let base_proto_value = property_get(value, "prototype")
      if is_object_like(base_proto_value) {
        rc_replace_optional_value(proto_obj.proto, Some(base_proto_value))
        proto_obj.proto = Some(base_proto_value)
      } else if base_proto_value is Null {
        rc_replace_optional_value(proto_obj.proto, None)
        proto_obj.proto = None
      } else {
        let _ = throw_type_error("parent prototype must be an object or null")

      }
    }
    None => ()
  }
  let static_super_ctor = static_super_constructor_for_env(class_env, ctor_func)
  let instance_fields : Array[ClassFieldRecord] = []
  let static_inits : Array[ClassStaticInit] = []
  let private_instance_props : Map[String, Property] = Map::new()
  let _ = with_gc_frame(fn() raise {
    let _ = gc_root_push(Value::Function(ctor_func))
    let _ = gc_root_push(Value::Object(proto_obj))
    match static_super_ctor {
      Some(value) => {
        let _ = gc_root_push(value)

      }
      None => ()
    }
    for class_member in class_def.body {
      let mut skip_member = false
      match class_member {
        @engine.ClassMember::Method(key, _, is_static) =>
          if !is_static {
            match key {
              @engine.ObjKey::Ident(name) =>
                if name == "constructor" {
                  skip_member = true
                }
              _ => ()
            }
          }
        _ => ()
      }
      if skip_member {
        continue
      }
      let _ = with_gc_frame(fn() raise {
        match class_member {
          @engine.ClassMember::Method(key, func, is_static) =>
            match key {
              @engine.ObjKey::Private(name) => {
                let key_name = env_private_key_checked(class_env, name)
                let func_value = to_function_value(class_env, func, false)
                let func = Value::Function(func_value)
                let _ = gc_root_push(func)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key("#" + name, None, None),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                  define_private_property(
                    Value::Function(ctor_func),
                    key_name,
                    property_data_readonly(func),
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                  props_set(private_instance_props, 
                    key_name,
                    property_data_readonly(func),
                  )
                }
              }
              _ => {
                let (name, key_value) = class_obj_key_property_name(class_env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(class_env, func, false)
                let func = Value::Function(func_value)
                let _ = gc_root_push(func)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, None),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                }
                let target = if is_static {
                  Value::Function(ctor_func)
                } else {
                  Value::Object(proto_obj)
                }
                define_class_method(target, name, func_value)
              }
            }
          @engine.ClassMember::Getter(key, func, is_static) =>
            match key {
              @engine.ObjKey::Private(name) => {
                let key_name = env_private_key_checked(class_env, name)
                let func_value = to_function_value(class_env, func, false)
                let getter = Value::Function(func_value)
                let _ = gc_root_push(getter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key("#" + name, None, Some("get ")),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                  set_accessor_property(
                    ctor_func.props,
                    key_name,
                    Some(getter),
                    None,
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                  set_accessor_property(
                    private_instance_props,
                    key_name,
                    Some(getter),
                    None,
                  )
                }
              }
              _ => {
                let (name, key_value) = class_obj_key_property_name(class_env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(class_env, func, false)
                let getter = Value::Function(func_value)
                let _ = gc_root_push(getter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, Some("get ")),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                }
                let target = if is_static {
                  Value::Function(ctor_func)
                } else {
                  Value::Object(proto_obj)
                }
                define_class_accessor(target, name, Some(getter), None)
              }
            }
          @engine.ClassMember::Setter(key, func, is_static) =>
            match key {
              @engine.ObjKey::Private(name) => {
                let key_name = env_private_key_checked(class_env, name)
                let func_value = to_function_value(class_env, func, false)
                let setter = Value::Function(func_value)
                let _ = gc_root_push(setter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key("#" + name, None, Some("set ")),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                  set_accessor_property(
                    ctor_func.props,
                    key_name,
                    None,
                    Some(setter),
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                  set_accessor_property(
                    private_instance_props,
                    key_name,
                    None,
                    Some(setter),
                  )
                }
              }
              _ => {
                let (name, key_value) = class_obj_key_property_name(class_env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let func_value = to_function_value(class_env, func, false)
                let setter = Value::Function(func_value)
                let _ = gc_root_push(setter)
                func_value.is_constructor = false
                set_function_name(
                  func_value,
                  function_name_from_key(name, key_value, Some("set ")),
                )
                if is_static {
                  set_function_super_constructor(func_value, static_super_ctor)
                  set_function_home_object(
                    func_value,
                    Some(Value::Function(ctor_func)),
                  )
                } else {
                  set_function_home_object(
                    func_value,
                    Some(Value::Object(proto_obj)),
                  )
                }
                let target = if is_static {
                  Value::Function(ctor_func)
                } else {
                  Value::Object(proto_obj)
                }
                define_class_accessor(target, name, None, Some(setter))
              }
            }
          @engine.ClassMember::Field(key, init, is_static) =>
            match key {
              @engine.ObjKey::Private(name) => {
                let key_name = env_private_key_checked(class_env, name)
                let record = ClassFieldRecord::{
                  is_private: true,
                  name: key_name,
                  display_name: "#" + name,
                  init,
                }
                if is_static {
                  static_inits.push(ClassStaticInit::Field(record))
                } else {
                  instance_fields.push(record)
                }
              }
              _ => {
                let (name, key_value) = class_obj_key_property_name(class_env, key)
                match key_value {
                  Some(key_value_item) => {
                    let _ = gc_root_push(key_value_item)

                  }
                  None => ()
                }
                let record = ClassFieldRecord::{
                  is_private: false,
                  name,
                  display_name: function_name_from_key(name, key_value, None),
                  init,
                }
                if is_static {
                  static_inits.push(ClassStaticInit::Field(record))
                } else {
                  instance_fields.push(record)
                }
              }
            }
          @engine.ClassMember::StaticBlock(stmts) =>
            static_inits.push(ClassStaticInit::Block(stmts))
        }
        Undefined
      })

    }
    Undefined
  })
  match class_def.name {
    Some(name) =>
      if class_env.uninitialized_bindings.contains(name) {
        env_initialize_binding(class_env, name, Value::Function(ctor_func))
      } else {
        env_define_readonly(class_env, name, Value::Function(ctor_func), true)
      }
    None => ()
  }
  if !static_inits.is_empty() {
    for init in static_inits {
      match init {
        ClassStaticInit::Field(field) => {
          let should_set_name = match field.init {
            Some(expr) => is_anonymous_function_definition(expr)
            None => false
          }
          let value = match field.init {
            Some(expr) =>
              with_static_super_context(ctor_func, static_super_ctor, fn() raise {
                eval_class_field_initializer(field_env, expr)
              })
            None => Undefined
          }
          if should_set_name {
            set_anonymous_function_name(value, field.display_name, None)
          }
          if field.is_private {
            define_private_property(
              Value::Function(ctor_func),
              field.name,
              property_data(value),
            )
          } else {
            let desc = property_descriptor_object(property_data(value))
            let _ = define_property(
              Value::Function(ctor_func),
              prop_key_value_from_name(field.name),
              desc,
            )

          }
        }
        ClassStaticInit::Block(stmts) => {
          let block_env = Env::new(Some(field_env))
          block_env.var_env = Some(block_env)
          env_set_local(block_env, "this", Value::Function(ctor_func))
          block_env.new_target = Undefined
          hoist_decls(block_env, stmts, false, false)
          ignore(
            with_static_super_context(ctor_func, static_super_ctor, fn() raise {
              class_eval_block(block_env, stmts)
            }),
          )
        }
      }
    }
  }
  ctor_func.private_instance_props = private_instance_props
  ctor_func.instance_fields = instance_fields
  Value::Function(ctor_func)
}

///|
fn gen_eval_class_static_inits(
  field_env : Env,
  index : Int,
  ctor_func : FunctionValue,
  static_super_ctor : Value?,
  static_inits : Array[ClassStaticInit],
  instance_fields : Array[ClassFieldRecord],
  private_instance_props : Map[String, Property],
) -> GenStep raise {
  if index >= static_inits.length() {
    ctor_func.private_instance_props = private_instance_props
    ctor_func.instance_fields = instance_fields
    return Done(Normal(Value::Function(ctor_func)))
  }
  match static_inits[index] {
    ClassStaticInit::Field(field) => {
      let should_set_name = match field.init {
        Some(expr) => is_anonymous_function_definition(expr)
        None => false
      }
      match field.init {
        Some(expr) => {
          let step = with_static_super_context_step(
            ctor_func,
            static_super_ctor,
            fn() raise { gen_eval_class_field_initializer(field_env, expr) },
          )
          gen_bind(step, fn(control) raise {
            match control {
              Normal(value) => {
                if should_set_name {
                  set_anonymous_function_name(value, field.display_name, None)
                }
                if field.is_private {
                  define_private_property(
                    Value::Function(ctor_func),
                    field.name,
                    property_data(value),
                  )
                } else {
                  let desc = property_descriptor_object(property_data(value))
                  let _ = define_property(
                    Value::Function(ctor_func),
                    prop_key_value_from_name(field.name),
                    desc,
                  )

                }
                gen_eval_class_static_inits(
                  field_env,
                  index + 1,
                  ctor_func,
                  static_super_ctor,
                  static_inits,
                  instance_fields,
                  private_instance_props,
                )
              }
              Return(value) => Done(Return(value))
              Throw(value) => Done(Throw(value))
              Break(label, value_opt) => Done(Break(label, value_opt))
              Continue(label, value_opt) => Done(Continue(label, value_opt))
            }
          })
        }
        None => {
          let value = Undefined
          if field.is_private {
            define_private_property(
              Value::Function(ctor_func),
              field.name,
              property_data(value),
            )
          } else {
            let desc = property_descriptor_object(property_data(value))
            let _ = define_property(
              Value::Function(ctor_func),
              prop_key_value_from_name(field.name),
              desc,
            )

          }
          gen_eval_class_static_inits(
            field_env,
            index + 1,
            ctor_func,
            static_super_ctor,
            static_inits,
            instance_fields,
            private_instance_props,
          )
        }
      }
    }
    ClassStaticInit::Block(stmts) => {
      let block_env = Env::new(Some(field_env))
      block_env.var_env = Some(block_env)
      env_set_local(block_env, "this", Value::Function(ctor_func))
      block_env.new_target = Undefined
      hoist_decls(block_env, stmts, false, false)
      let step = with_static_super_context_step(ctor_func, static_super_ctor, fn() raise {
        gen_class_eval_block(block_env, stmts)
      })
      gen_bind(step, fn(control) raise {
        match control {
          Normal(_) =>
            gen_eval_class_static_inits(
              field_env,
              index + 1,
              ctor_func,
              static_super_ctor,
              static_inits,
              instance_fields,
              private_instance_props,
            )
          Return(value) => Done(Return(value))
          Throw(value) => Done(Throw(value))
          Break(label, value_opt) => Done(Break(label, value_opt))
          Continue(label, value_opt) => Done(Continue(label, value_opt))
        }
      })
    }
  }
}

///|
fn gen_eval_class_members(
  class_env : Env,
  field_env : Env,
  members : Array[@engine.ClassMember],
  index : Int,
  ctor_func : FunctionValue,
  proto_obj : ObjectValue,
  static_super_ctor : Value?,
  class_name : String?,
  instance_fields : Array[ClassFieldRecord],
  static_inits : Array[ClassStaticInit],
  private_instance_props : Map[String, Property],
) -> GenStep raise {
  if index >= members.length() {
    match class_name {
      Some(name) =>
        if class_env.uninitialized_bindings.contains(name) {
          env_initialize_binding(class_env, name, Value::Function(ctor_func))
        } else {
          env_define_readonly(class_env, name, Value::Function(ctor_func), true)
        }
      None => ()
    }
    return gen_eval_class_static_inits(
      field_env, 0, ctor_func, static_super_ctor, static_inits, instance_fields,
      private_instance_props,
    )
  }
  match members[index] {
    @engine.ClassMember::Method(key, func, is_static) =>
      match key {
        @engine.ObjKey::Private(name) => {
          let key_name = env_private_key_checked(class_env, name)
          let func_value = to_function_value(class_env, func, false)
          let func = Value::Function(func_value)
          let _ = gen_root_push(func)
          func_value.is_constructor = false
          set_function_name(
            func_value,
            function_name_from_key("#" + name, None, None),
          )
          if is_static {
            set_function_super_constructor(func_value, static_super_ctor)
            set_function_home_object(
              func_value,
              Some(Value::Function(ctor_func)),
            )
            define_private_property(
              Value::Function(ctor_func),
              key_name,
              property_data_readonly(func),
            )
          } else {
            set_function_home_object(
              func_value,
              Some(Value::Object(proto_obj)),
            )
            props_set(private_instance_props, key_name, property_data_readonly(func))
          }
          gen_eval_class_members(
            class_env,
            field_env,
            members,
            index + 1,
            ctor_func,
            proto_obj,
            static_super_ctor,
            class_name,
            instance_fields,
            static_inits,
            private_instance_props,
          )
        }
        _ => {
          if !is_static {
            match key {
              @engine.ObjKey::Ident(name) =>
                if name == "constructor" {
                  return gen_eval_class_members(
                    class_env,
                    field_env,
                    members,
                    index + 1,
                    ctor_func,
                    proto_obj,
                    static_super_ctor,
                    class_name,
                    instance_fields,
                    static_inits,
                    private_instance_props,
                  )
                }
              _ => ()
            }
          }
          gen_obj_key_property_name(class_env, key, fn(name, key_value) raise {
            match key_value {
              Some(key_value_item) => {
                let _ = gen_root_push(key_value_item)

              }
              None => ()
            }
            let func_value = to_function_value(class_env, func, false)
            let func = Value::Function(func_value)
            let _ = gen_root_push(func)
            func_value.is_constructor = false
            set_function_name(
              func_value,
              function_name_from_key(name, key_value, None),
            )
            if is_static {
              set_function_super_constructor(func_value, static_super_ctor)
              set_function_home_object(
                func_value,
                Some(Value::Function(ctor_func)),
              )
            } else {
              set_function_home_object(
                func_value,
                Some(Value::Object(proto_obj)),
              )
            }
            let target = if is_static {
              Value::Function(ctor_func)
            } else {
              Value::Object(proto_obj)
            }
            define_class_method(target, name, func_value)
            gen_eval_class_members(
              class_env,
              field_env,
              members,
              index + 1,
              ctor_func,
              proto_obj,
              static_super_ctor,
              class_name,
              instance_fields,
              static_inits,
              private_instance_props,
            )
          })
        }
      }
    @engine.ClassMember::Getter(key, func, is_static) =>
      match key {
        @engine.ObjKey::Private(name) => {
          let key_name = env_private_key_checked(class_env, name)
          let func_value = to_function_value(class_env, func, false)
          let getter = Value::Function(func_value)
          let _ = gen_root_push(getter)
          func_value.is_constructor = false
          set_function_name(
            func_value,
            function_name_from_key("#" + name, None, Some("get ")),
          )
          if is_static {
            set_function_super_constructor(func_value, static_super_ctor)
            set_function_home_object(
              func_value,
              Some(Value::Function(ctor_func)),
            )
            set_accessor_property(ctor_func.props, key_name, Some(getter), None)
          } else {
            set_function_home_object(
              func_value,
              Some(Value::Object(proto_obj)),
            )
            set_accessor_property(
              private_instance_props,
              key_name,
              Some(getter),
              None,
            )
          }
          gen_eval_class_members(
            class_env,
            field_env,
            members,
            index + 1,
            ctor_func,
            proto_obj,
            static_super_ctor,
            class_name,
            instance_fields,
            static_inits,
            private_instance_props,
          )
        }
        _ =>
          gen_obj_key_property_name(class_env, key, fn(name, key_value) raise {
            match key_value {
              Some(key_value_item) => {
                let _ = gen_root_push(key_value_item)

              }
              None => ()
            }
            let func_value = to_function_value(class_env, func, false)
            func_value.is_constructor = false
            set_function_name(
              func_value,
              function_name_from_key(name, key_value, Some("get ")),
            )
            if is_static {
              set_function_super_constructor(func_value, static_super_ctor)
              set_function_home_object(
                func_value,
                Some(Value::Function(ctor_func)),
              )
            } else {
              set_function_home_object(
                func_value,
                Some(Value::Object(proto_obj)),
              )
            }
            let target = if is_static {
              Value::Function(ctor_func)
            } else {
              Value::Object(proto_obj)
            }
            let getter = Value::Function(func_value)
            let _ = gen_root_push(getter)
            define_class_accessor(target, name, Some(getter), None)
            gen_eval_class_members(
              class_env,
              field_env,
              members,
              index + 1,
              ctor_func,
              proto_obj,
              static_super_ctor,
              class_name,
              instance_fields,
              static_inits,
              private_instance_props,
            )
          })
      }
    @engine.ClassMember::Setter(key, func, is_static) =>
      match key {
        @engine.ObjKey::Private(name) => {
          let key_name = env_private_key_checked(class_env, name)
          let func_value = to_function_value(class_env, func, false)
          let setter = Value::Function(func_value)
          let _ = gen_root_push(setter)
          func_value.is_constructor = false
          set_function_name(
            func_value,
            function_name_from_key("#" + name, None, Some("set ")),
          )
          if is_static {
            set_function_super_constructor(func_value, static_super_ctor)
            set_function_home_object(
              func_value,
              Some(Value::Function(ctor_func)),
            )
            set_accessor_property(ctor_func.props, key_name, None, Some(setter))
          } else {
            set_function_home_object(
              func_value,
              Some(Value::Object(proto_obj)),
            )
            set_accessor_property(
              private_instance_props,
              key_name,
              None,
              Some(setter),
            )
          }
          gen_eval_class_members(
            class_env,
            field_env,
            members,
            index + 1,
            ctor_func,
            proto_obj,
            static_super_ctor,
            class_name,
            instance_fields,
            static_inits,
            private_instance_props,
          )
        }
        _ =>
          gen_obj_key_property_name(class_env, key, fn(name, key_value) raise {
            match key_value {
              Some(key_value_item) => {
                let _ = gen_root_push(key_value_item)

              }
              None => ()
            }
            let func_value = to_function_value(class_env, func, false)
            func_value.is_constructor = false
            set_function_name(
              func_value,
              function_name_from_key(name, key_value, Some("set ")),
            )
            if is_static {
              set_function_super_constructor(func_value, static_super_ctor)
              set_function_home_object(
                func_value,
                Some(Value::Function(ctor_func)),
              )
            } else {
              set_function_home_object(
                func_value,
                Some(Value::Object(proto_obj)),
              )
            }
            let target = if is_static {
              Value::Function(ctor_func)
            } else {
              Value::Object(proto_obj)
            }
            let setter = Value::Function(func_value)
            let _ = gen_root_push(setter)
            define_class_accessor(target, name, None, Some(setter))
            gen_eval_class_members(
              class_env,
              field_env,
              members,
              index + 1,
              ctor_func,
              proto_obj,
              static_super_ctor,
              class_name,
              instance_fields,
              static_inits,
              private_instance_props,
            )
          })
      }
    @engine.ClassMember::Field(key, init, is_static) =>
      match key {
        @engine.ObjKey::Private(name) => {
          let key_name = env_private_key_checked(class_env, name)
          let record = ClassFieldRecord::{
            is_private: true,
            name: key_name,
            display_name: "#" + name,
            init,
          }
          if is_static {
            static_inits.push(ClassStaticInit::Field(record))
          } else {
            instance_fields.push(record)
          }
          gen_eval_class_members(
            class_env,
            field_env,
            members,
            index + 1,
            ctor_func,
            proto_obj,
            static_super_ctor,
            class_name,
            instance_fields,
            static_inits,
            private_instance_props,
          )
        }
        _ =>
          gen_obj_key_property_name(class_env, key, fn(name, key_value) raise {
            match key_value {
              Some(key_value_item) => {
                let _ = gen_root_push(key_value_item)

              }
              None => ()
            }
            let record = ClassFieldRecord::{
              is_private: false,
              name,
              display_name: function_name_from_key(name, key_value, None),
              init,
            }
            if is_static {
              static_inits.push(ClassStaticInit::Field(record))
            } else {
              instance_fields.push(record)
            }
            gen_eval_class_members(
              class_env,
              field_env,
              members,
              index + 1,
              ctor_func,
              proto_obj,
              static_super_ctor,
              class_name,
              instance_fields,
              static_inits,
              private_instance_props,
            )
          })
      }
    @engine.ClassMember::StaticBlock(stmts) => {
      static_inits.push(ClassStaticInit::Block(stmts))
      gen_eval_class_members(
        class_env,
        field_env,
        members,
        index + 1,
        ctor_func,
        proto_obj,
        static_super_ctor,
        class_name,
        instance_fields,
        static_inits,
        private_instance_props,
      )
    }
  }
}

///|
fn gen_eval_class(
  env : Env,
  class_def : @engine.ClassDef,
  name_hint? : String? = None,
) -> GenStep raise {
  let class_env = Env::new(Some(env))
  match class_def.name {
    Some(name) => {
      env_declare_uninitialized(class_env, name)
      class_env.readonly_bindings.set(name, true)
    }
    None => ()
  }
  let after_super = fn(super_value : Value?) -> GenStep raise {
    match super_value {
      Some(value) =>
        match value {
          Null => ()
          _ =>
            if !is_constructor_value(value) {
              let _ = throw_type_error("not a constructor")

            }
        }
      None => ()
    }
    for class_member in class_def.body {
      match class_member {
        @engine.ClassMember::Method(@engine.ObjKey::Private(name), _, _) =>
          if class_env.private_bindings.get(name) is None {
            ignore(env_define_private_name(class_env, name))
          }
        @engine.ClassMember::Getter(@engine.ObjKey::Private(name), _, _) =>
          if class_env.private_bindings.get(name) is None {
            ignore(env_define_private_name(class_env, name))
          }
        @engine.ClassMember::Setter(@engine.ObjKey::Private(name), _, _) =>
          if class_env.private_bindings.get(name) is None {
            ignore(env_define_private_name(class_env, name))
          }
        @engine.ClassMember::Field(@engine.ObjKey::Private(name), _, _) =>
          if class_env.private_bindings.get(name) is None {
            ignore(env_define_private_name(class_env, name))
          }
        _ => ()
      }
    }
    let mut constructor_def : @engine.FunctionDef? = None
    for class_member in class_def.body {
      match class_member {
        @engine.ClassMember::Method(
          @engine.ObjKey::Ident(name),
          func,
          is_static
        ) =>
          if !is_static && name == "constructor" {
            constructor_def = Some(func)
          }
        _ => ()
      }
    }
    let ctor_def = match constructor_def {
      Some(func) =>
        match class_def.name {
          Some(class_name) =>
            @engine.FunctionDef::new(
              Some(class_name),
              func.params,
              func.param_inits,
              func.body,
              true,
              func.is_generator,
              func.is_async,
              false,
              func.offset,
              func.end_offset,
            )
          None =>
            @engine.FunctionDef::new(
              None,
              func.params,
              func.param_inits,
              func.body,
              true,
              func.is_generator,
              func.is_async,
              false,
              func.offset,
              func.end_offset,
            )
        }
      None =>
        @engine.FunctionDef::new(
          class_def.name,
          ([] : Array[@engine.ParamBinding]),
          ([] : Array[@engine.Expr?]),
          ([] : Array[@engine.Stmt]),
          true,
          false,
          false,
          false,
          class_def.offset,
          class_def.end_offset,
        )
    }
    let ctor_func = to_function_value(class_env, ctor_def, false)
    ctor_func.is_class_constructor = true
    if class_def.name is None {
      match name_hint {
        Some(name) => set_function_name(ctor_func, name)
        None => ()
      }
    }
    match current_source() {
      Some(source) => {
        let end_offset = if class_def.end_offset > class_def.offset {
          class_def.end_offset
        } else {
          class_def.offset
        }
        ctor_func.source = Some(
          slice_source_range(source, class_def.offset, end_offset),
        )
      }
      None => ()
    }
    let field_env = Env::new(Some(class_env))
    env_set_local(field_env, "this", Value::Function(ctor_func))
    field_env.new_target = Undefined
    match super_value {
      Some(value) => {
        set_function_super_constructor(ctor_func, Some(value))
        match value {
          Null => ()
          _ => {
            rc_replace_optional_value(ctor_func.proto, Some(value))
            ctor_func.proto = Some(value)
          }
        }
      }
      None => ()
    }
    if constructor_def is None {
      match super_value {
        Some(_) => ctor_func.is_default_constructor = true
        None => ()
      }
    }
    ensure_function_prototype(ctor_func)
    let proto_value = property_get(Value::Function(ctor_func), "prototype")
    let proto_obj = match proto_value {
      Object(obj) => obj
      _ => {
        let fresh = new_object_value()
        match fresh {
          Object(obj) => {
            props_set(ctor_func.props, 
              "prototype",
              property_data_non_enum_non_config(fresh),
            )
            obj
          }
          _ => return Done(Normal(Value::Function(ctor_func)))
        }
      }
    }
    set_function_home_object(ctor_func, Some(Value::Object(proto_obj)))
    match super_value {
      Some(Null) => {
        rc_replace_optional_value(proto_obj.proto, None)
        proto_obj.proto = None
      }
      Some(value) => {
        let base_proto_value = property_get(value, "prototype")
        if is_object_like(base_proto_value) {
          rc_replace_optional_value(proto_obj.proto, Some(base_proto_value))
          proto_obj.proto = Some(base_proto_value)
        } else if base_proto_value is Null {
          rc_replace_optional_value(proto_obj.proto, None)
          proto_obj.proto = None
        } else {
          let _ = throw_type_error("parent prototype must be an object or null")

        }
      }
      None => ()
    }
    let static_super_ctor = static_super_constructor_for_env(
      class_env, ctor_func,
    )
    let instance_fields : Array[ClassFieldRecord] = []
    let static_inits : Array[ClassStaticInit] = []
    let private_instance_props : Map[String, Property] = Map::new()
    gen_with_root_frame(fn() raise {
      let _ = gen_root_push(Value::Function(ctor_func))
      let _ = gen_root_push(Value::Object(proto_obj))
      match static_super_ctor {
        Some(value) => {
          let _ = gen_root_push(value)

        }
        None => ()
      }
      gen_eval_class_members(
        class_env,
        field_env,
        class_def.body,
        0,
        ctor_func,
        proto_obj,
        static_super_ctor,
        class_def.name,
        instance_fields,
        static_inits,
        private_instance_props,
      )
    })
  }
  match class_def.super_class {
    Some(expr) =>
      gen_class_eval_expr_cont(class_env, expr, fn(value) raise {
        after_super(Some(value))
      })
    None => after_super(None)
  }
}
