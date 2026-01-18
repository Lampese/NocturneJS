///|
fn gc_incref_value(state : GcState, value : Value) -> Unit {
  match value_id(value) {
    Some(id) => gc_meta_incref(state, id)
    None => ()
  }
}

///|
fn gc_decref_value(state : GcState, value : Value) -> Unit {
  match value_id(value) {
    Some(id) => gc_meta_decref(state, id)
    None => ()
  }
}

///|
fn gc_incref_env(state : GcState, env : Env) -> Unit {
  gc_meta_incref(state, env.id)
}

///|
fn gc_decref_env(state : GcState, env : Env) -> Unit {
  gc_meta_decref(state, env.id)
}

///|
fn gc_incref_optional_value(state : GcState, value : Value?) -> Unit {
  match value {
    Some(inner) => gc_incref_value(state, inner)
    None => ()
  }
}

///|
fn gc_incref_optional_object(state : GcState, obj : ObjectValue?) -> Unit {
  match obj {
    Some(inner) => gc_incref_value(state, Object(inner))
    None => ()
  }
}

///|
fn gc_incref_symbol(state : GcState, symbol : SymbolValue) -> Unit {
  gc_meta_incref(state, symbol.id)
}

///|
fn gc_incref_value_ref(state : GcState, cell : Ref[Value?]) -> Unit {
  cell.update(fn(current) {
    gc_incref_optional_value(state, current)
    current
  })
}

///|
fn gc_incref_object_ref(state : GcState, cell : Ref[ObjectValue?]) -> Unit {
  cell.update(fn(current) {
    gc_incref_optional_object(state, current)
    current
  })
}

///|
fn gc_incref_symbol_ref(state : GcState, cell : Ref[SymbolValue?]) -> Unit {
  cell.update(fn(current) {
    match current {
      Some(symbol) => gc_incref_symbol(state, symbol)
      None => ()
    }
    current
  })
}
