///|
fn gen_eval_expr_cont(
  env : Env,
  expr : @engine.Expr,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  gen_bytecode_eval_expr_cont(env, expr, k)
}

///|
fn gen_eval_named_expr_cont(
  env : Env,
  expr : @engine.Expr,
  name : String,
  key_value : Value?,
  k : (Value) -> GenStep raise,
) -> GenStep raise {
  let hint_name = function_name_from_key(name, key_value, None)
  match unwrap_paren(expr) {
    @engine.Expr::ClassExpr(class_def) =>
      if class_def.name is None {
        if expr_contains_yield(expr) {
          return gen_bind(
            gen_eval_class(env, class_def, name_hint=Some(hint_name)),
            fn(control) raise {
              match control {
                Normal(value) => k(value)
                Return(value) => Done(Return(value))
                Throw(value) => Done(Throw(value))
                Break(label, value_opt) => Done(Break(label, value_opt))
                Continue(label, value_opt) => Done(Continue(label, value_opt))
              }
            },
          )
        }
        let value = eval_class(env, class_def, name_hint=Some(hint_name))
        return k(value)
      }
    _ => ()
  }
  gen_eval_expr_cont(env, expr, fn(value) raise {
    if is_anonymous_function_definition(expr) {
      set_anonymous_function_name(value, name, key_value)
    }
    k(value)
  })
}
