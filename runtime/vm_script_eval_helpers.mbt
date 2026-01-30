///|
fn stmt_updates_completion(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::Empty
    | @engine.Stmt::VarDecl(_, _)
    | @engine.Stmt::FunctionDecl(_)
    | @engine.Stmt::ClassDecl(_) => false
    @engine.Stmt::Block(stmts) => stmt_list_updates_completion(stmts)
    @engine.Stmt::Label(_, inner) => stmt_updates_completion(inner)
    _ => true
  }
}

///|
fn stmt_list_updates_completion(stmts : Array[@engine.Stmt]) -> Bool {
  for stmt in stmts {
    if stmt_updates_completion(stmt) {
      return true
    }
  }
  false
}

///|
fn eval_script_with_strict(
  env : Env,
  script : @engine.Script,
  inherit_strict : Bool,
  var_configurable : Bool,
  eval_skip_outer_var : Bool,
) -> Value raise {
  let old_strict = env.strict
  env.strict = if inherit_strict {
    env.strict || script.is_strict || script.is_module
  } else {
    script.is_strict || script.is_module
  }
  try
    with_source_value(script.source, fn() raise {
      let top_level_func_decls = collect_top_level_function_decls(script.body)
      let func_var_names = compute_annex_b_var_func_decls(
        script.body,
        env.strict,
        @hashmap.new(),
        false,
      )
      with_top_level_function_decls(top_level_func_decls, fn() raise {
        with_annex_b_var_func_decls(func_var_names, fn() raise {
          if env.parent is None && !script.is_module && !var_configurable {
            check_global_script_decls(env, script.body, env.strict)
          }
          let needs_lex_decls = eval_body_has_lexical_decls(script.body)
          if needs_lex_decls {
            eval_predeclare_lexical_decls(env, script.body)
          }
          hoist_decls(env, script.body, var_configurable, eval_skip_outer_var)
          try {
            let bytecode = bytecode_compile_script(script)
            match bytecode {
              Some(func) => {
                let value = bytecode_exec(env, func)
                gc_maybe_collect_with_root(value)
                value
              }
              None => {
                let _ = throw_type_error("bytecode compile failed")
                Undefined
              }
            }
          } catch {
            ThrowSignal(value) => raise ThrowSignal(value)
            err => raise err
          }
        })
      })
    })
  catch {
    err => {
      env.strict = old_strict
      raise err
    }
  } noraise {
    result => {
      env.strict = old_strict
      result
    }
  }
}

///|
fn eval_script(env : Env, script : @engine.Script) -> Value raise {
  eval_script_with_strict(env, script, true, false, false)
}
