///|
let bytecode_cache_ref : Ref[Map[Int, BytecodeFunction]] = Ref::new(Map::new())

///|
fn rc_incref_bytecode_function(code : BytecodeFunction) -> Unit {
  rc_incref_values(code.cpool)
}

///|
fn rc_decref_bytecode_function(code : BytecodeFunction) -> Unit {
  rc_decref_values(code.cpool)
}

///|
fn bytecode_cache_get(id : Int) -> BytecodeFunction? {
  let mut value : BytecodeFunction? = None
  bytecode_cache_ref.update(fn(cache) {
    value = cache.get(id)
    cache
  })
  value
}

///|
fn bytecode_cache_set(id : Int, code : BytecodeFunction) -> Unit {
  bytecode_cache_ref.update(fn(cache) {
    match cache.get(id) {
      Some(existing) => rc_decref_bytecode_function(existing)
      None => ()
    }
    rc_incref_bytecode_function(code)
    cache.set(id, code)
    cache
  })
}

///|
fn bytecode_cache_remove(id : Int) -> Unit {
  bytecode_cache_ref.update(fn(cache) {
    match cache.get(id) {
      Some(existing) => rc_decref_bytecode_function(existing)
      None => ()
    }
    let _ = cache.remove(id)
    cache
  })
}

///|
fn bytecode_cache_clear() -> Unit {
  bytecode_cache_ref.update(fn(cache) {
    for _, code in cache {
      rc_decref_bytecode_function(code)
    }
    Map::new()
  })
}
