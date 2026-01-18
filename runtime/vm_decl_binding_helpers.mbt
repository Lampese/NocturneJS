///|
fn ensure_strict_binding_name(env : Env, name : String) -> Unit raise {
  if env.strict && (name == "eval" || name == "arguments") {
    let _ = throw_syntax_error("invalid identifier")

  }
}

///|
fn declare_binding_names(env : Env, binding : @engine.VarBinding) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(name) => {
      ensure_strict_binding_name(env, name)
      env_declare(env, name, Undefined)
    }
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            declare_binding_names(env, inner)
          @engine.ArrayPatternElem::Rest(inner) =>
            declare_binding_names(env, inner)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            declare_binding_names(env, inner)
          @engine.ObjectPatternElem::Rest(inner) =>
            declare_binding_names(env, inner)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn declare_var_binding_names(
  env : Env,
  binding : @engine.VarBinding,
  configurable : Bool,
) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(name) => {
      ensure_strict_binding_name(env, name)
      env_declare_var(env, name, configurable)
    }
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            declare_var_binding_names(env, inner, configurable)
          @engine.ArrayPatternElem::Rest(inner) =>
            declare_var_binding_names(env, inner, configurable)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            declare_var_binding_names(env, inner, configurable)
          @engine.ObjectPatternElem::Rest(inner) =>
            declare_var_binding_names(env, inner, configurable)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn has_outer_non_global_var_binding(env : Env, name : String) -> Bool {
  let mut current : Env? = Some(env)
  while true {
    match current {
      Some(frame) => {
        if frame.is_catch_env && frame.bindings.get(name) is Some(_) {
          return true
        }
        match frame.var_env {
          Some(var_env) =>
            if var_env.id == frame.id && frame.parent is Some(_) {
              if frame.bindings.get(name) is Some(_) {
                return true
              }
            }
          None => ()
        }
        current = frame.parent
      }
      None => break
    }
  }
  false
}

///|
fn declare_var_binding_names_skip_outer_var(
  origin_env : Env,
  env : Env,
  binding : @engine.VarBinding,
  configurable : Bool,
) -> Unit raise {
  match binding {
    @engine.VarBinding::Name(name) => {
      ensure_strict_binding_name(env, name)
      if !has_outer_non_global_var_binding(origin_env, name) {
        env_declare_var(env, name, configurable)
      }
    }
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            declare_var_binding_names_skip_outer_var(
              origin_env, env, inner, configurable,
            )
          @engine.ArrayPatternElem::Rest(inner) =>
            declare_var_binding_names_skip_outer_var(
              origin_env, env, inner, configurable,
            )
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            declare_var_binding_names_skip_outer_var(
              origin_env, env, inner, configurable,
            )
          @engine.ObjectPatternElem::Rest(inner) =>
            declare_var_binding_names_skip_outer_var(
              origin_env, env, inner, configurable,
            )
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}

///|
fn declare_binding_uninitialized(
  env : Env,
  binding : @engine.VarBinding,
) -> Unit {
  match binding {
    @engine.VarBinding::Name(name) => env_declare_uninitialized(env, name)
    @engine.VarBinding::ArrayPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ArrayPatternElem::Bind(inner, _) =>
            declare_binding_uninitialized(env, inner)
          @engine.ArrayPatternElem::Rest(inner) =>
            declare_binding_uninitialized(env, inner)
          @engine.ArrayPatternElem::Hole => ()
        }
      }
    @engine.VarBinding::ObjectPattern(pattern) =>
      for elem in pattern {
        match elem {
          @engine.ObjectPatternElem::Bind(_, inner, _) =>
            declare_binding_uninitialized(env, inner)
          @engine.ObjectPatternElem::Rest(inner) =>
            declare_binding_uninitialized(env, inner)
        }
      }
    @engine.VarBinding::Target(_) => ()
  }
}
