///|
fn eval_with_env(env : Env, args : Array[Value]) -> Value raise {
  eval_with_env_mode(env, args, true)
}

///|
fn collect_eval_private_names(env : Env) -> Array[String] {
  let names : Array[String] = []
  let seen : @hashmap.HashMap[String, Bool] = @hashmap.new()
  let mut current : Env? = Some(env)
  while true {
    match current {
      Some(frame) => {
        for name, _ in frame.private_bindings {
          if seen.get(name) is None {
            seen.set(name, true)
            names.push(name)
          }
        }
        current = frame.parent
      }
      None => break
    }
  }
  names
}

///|

///|
fn eval_with_env_mode(
  env : Env,
  args : Array[Value],
  direct : Bool,
) -> Value raise {
  if args.is_empty() {
    return Undefined
  }
  match args[0] {
    String(source) => {
      if @engine.contains_module_syntax(source) {
        let _ = throw_syntax_error("unexpected token")

      }
      let direct_strict = if direct { env.strict } else { false }
      let force_strict = if direct { direct_strict } else { false }
      let script = if direct {
        let private_names = collect_eval_private_names(env)
        parse_script_with_pos_mode_private(
          source, false, force_strict, private_names,
        )
      } else {
        parse_script_with_pos_mode(source, false, force_strict)
      }
      let eval_strict = if direct {
        direct_strict || script.is_strict
      } else {
        script.is_strict
      }
      let eval_skip_outer_var = direct && !eval_strict
      if direct {
        check_eval_var_conflicts(env, script.body)
      }
      let in_class_field_eval = direct && in_class_field_init_context()
      if in_class_field_eval && @engine.script_contains_arguments(script) {
        let _ = throw_syntax_error(
          "arguments not allowed in class field initializer",
        )

      }
      let (allow_new_target_value, allow_super_value, allow_super_call_value) = if direct {
        (env.allow_new_target, env.allow_super, env.allow_super_call)
      } else {
        (false, false, false)
      }
      let mut allow_new_target = allow_new_target_value
      let mut allow_super = allow_super_value
      let mut allow_super_call = allow_super_call_value
      if in_class_field_eval {
        allow_new_target = true
        allow_super = true
        allow_super_call = false
      }
      validate_context_in_script(
        script, allow_new_target, allow_super, allow_super_call,
      )
      let eval_base_env = env
      let var_env = if direct {
        var_env_for(eval_base_env)
      } else {
        eval_base_env
      }
      let eval_has_arguments_binding = if direct {
        match current_function() {
          Some(func) => !func.is_arrow
          None => false
        }
      } else {
        false
      }
      let func_var_names = compute_annex_b_var_func_decls(
        script.body,
        eval_strict,
        @hashmap.new(),
        eval_has_arguments_binding,
      )
      with_annex_b_var_func_decls(func_var_names, fn() raise {
        if direct && !eval_strict {
          eval_check_var_lex_conflicts(
            eval_base_env,
            var_env,
            script.body,
            eval_strict,
          )
        }
        if !eval_strict {
          eval_check_global_var_lex_conflicts(var_env, script.body, eval_strict)
        }
        eval_check_global_function_decls(var_env, script.body, eval_strict)
        let needs_lex_env = eval_body_has_lexical_decls(script.body)
        if direct {
          if eval_strict || needs_lex_env {
            let eval_env = Env::new(Some(eval_base_env))
            let eval_var_env = if eval_strict { eval_env } else { var_env }
            eval_env.var_env = Some(eval_var_env)
            if needs_lex_env {
              eval_predeclare_lexical_decls(eval_env, script.body)
            }
            eval_script_with_strict(
              eval_env, script, direct_strict, true, eval_skip_outer_var,
            )
          } else {
            eval_script_with_strict(
              eval_base_env, script, direct_strict, true, eval_skip_outer_var,
            )
          }
        } else {
          let eval_env = Env::new(Some(var_env))
          let eval_var_env = if eval_strict { eval_env } else { var_env }
          eval_env.var_env = Some(eval_var_env)
          if needs_lex_env {
            eval_predeclare_lexical_decls(eval_env, script.body)
          }
          eval_script_with_strict(
            eval_env, script, false, true, eval_skip_outer_var,
          )
        }
      })
    }
    _ => args[0]
  }
}
