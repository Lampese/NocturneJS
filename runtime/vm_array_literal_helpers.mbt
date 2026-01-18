///|
fn eval_array_literal(env : Env, elems : Array[@engine.Expr?]) -> Value raise {
  with_gc_frame(fn() raise {
    let values : Array[Value?] = []
    for elem in elems {
      match elem {
        Some(@engine.Expr::Spread(inner)) => {
          let spread_value = eval_expr(env, inner)
          let _ = gc_root_push(spread_value)
          for value in build_spread_list(spread_value) {
            values.push(Some(value))
            let _ = gc_root_push(value)

          }
        }
        Some(expr) => {
          let value = eval_expr(env, expr)
          values.push(Some(value))
          let _ = gc_root_push(value)

        }
        None => values.push(None)
      }
    }
    let array_proto_value = match value_from_object(array_proto_for_env(env)) {
      Some(value) => Some(value)
      None => value_from_object(array_proto())
    }
    new_array_value_with_proto(values, proto=array_proto_value)
  })
}
