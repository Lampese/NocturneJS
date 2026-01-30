///|
fn obj_key_simple_name(key : @engine.ObjKey) -> String {
  match key {
    @engine.ObjKey::Ident(id) => id
    @engine.ObjKey::Private(id) => "#" + id
    @engine.ObjKey::String(s) => s
    @engine.ObjKey::Number(value) => number_to_string_radix(value, 10)
    @engine.ObjKey::Computed(_) => ""
  }
}

///|
fn obj_key_is_proto(key : @engine.ObjKey) -> Bool {
  match key {
    @engine.ObjKey::Ident(name) => name == "__proto__"
    @engine.ObjKey::String(name) => name == "__proto__"
    _ => false
  }
}

///|
fn gen_obj_key_property_name(
  env : Env,
  key : @engine.ObjKey,
  k : (String, Value?) -> GenStep raise,
) -> GenStep raise {
  match key {
    @engine.ObjKey::Computed(expr) =>
      if bytecode_class_eval_enabled() {
        gen_bytecode_eval_expr_cont(env, expr, fn(value) raise {
          let _ = gen_root_push(value)
          k(property_key_name(value), Some(value))
        })
      } else {
        gen_eval_expr_cont(env, expr, fn(value) raise {
          let _ = gen_root_push(value)
          k(property_key_name(value), Some(value))
        })
      }
    _ => k(obj_key_simple_name(key), None)
  }
}
