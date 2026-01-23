///|
let symbol_registry_ref : Ref[Map[String, SymbolValue]] = Ref::new(Map::new())

///|
let symbol_table_ref : Ref[Map[Int, SymbolValue]] = Ref::new(Map::new())

///|
let symbol_iterator_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_to_string_tag_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_async_iterator_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_has_instance_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_is_concat_spreadable_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_match_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_match_all_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_replace_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_search_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_species_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_split_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_to_primitive_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_unscopables_ref : Ref[SymbolValue?] = Ref::new(None)

///|
let symbol_prop_prefix : String = "@@sym:"

///|
let private_prop_prefix : String = "@@priv:"

///|
fn alloc_symbol_id() -> Int {
  alloc_id()
}

///|
fn symbol_registry_get(key : String) -> SymbolValue? {
  let mut result : SymbolValue? = None
  symbol_registry_ref.update(fn(current) {
    result = current.get(key)
    current
  })
  result
}

///|
fn symbol_registry_set(key : String, symbol : SymbolValue) -> Unit {
  symbol_registry_ref.update(fn(current) {
    match current.get(key) {
      Some(existing) => rc_decref_value(Symbol(existing))
      None => ()
    }
    rc_incref_value(Symbol(symbol))
    current.set(key, symbol)
    current
  })
}

///|
fn symbol_registry_clear() -> Unit {
  symbol_registry_ref.update(fn(current) {
    for _, symbol in current {
      rc_decref_value(Symbol(symbol))
    }
    Map::new()
  })
}

///|
fn symbol_table_set(symbol : SymbolValue) -> Unit {
  symbol_table_ref.update(fn(current) {
    match current.get(symbol.id) {
      Some(existing) => rc_decref_value(Symbol(existing))
      None => ()
    }
    rc_incref_value(Symbol(symbol))
    current.set(symbol.id, symbol)
    current
  })
}

///|
fn symbol_table_remove(id : Int) -> Unit {
  symbol_table_ref.update(fn(current) {
    match current.get(id) {
      Some(existing) => rc_decref_value(Symbol(existing))
      None => ()
    }
    let _ = current.remove(id)
    current
  })
}

///|
fn symbol_table_clear() -> Unit {
  symbol_table_ref.update(fn(current) {
    for _, symbol in current {
      rc_decref_value(Symbol(symbol))
    }
    Map::new()
  })
}

///|
fn register_symbol(symbol : SymbolValue) -> Unit {
  symbol_table_set(symbol)
  register_value(Symbol(symbol))
}

///|
fn symbol_prop_key(symbol : SymbolValue) -> String {
  symbol_prop_prefix + Int::to_string(symbol.id)
}

///|
fn is_symbol_prop_key(name : String) -> Bool {
  name.has_prefix(symbol_prop_prefix)
}

///|
fn private_prop_key(id : Int) -> String {
  private_prop_prefix + Int::to_string(id)
}

///|
fn is_private_prop_key(name : String) -> Bool {
  name.has_prefix(private_prop_prefix)
}

///|
fn symbol_from_prop_key(name : String) -> SymbolValue? {
  if !is_symbol_prop_key(name) {
    return None
  }
  let id_text = name.unsafe_substring(
    start=symbol_prop_prefix.length(),
    end=name.length(),
  )
  try {
    let id = @strconv.parse_int(id_text, base=10)
    let mut value : SymbolValue? = None
    symbol_table_ref.update(fn(current) {
      value = current.get(id)
      current
    })
    value
  } catch {
    _ => None
  }
}

///|
fn symbol_key_from_ref(cell : Ref[SymbolValue?]) -> String? {
  let mut value : SymbolValue? = None
  cell.update(fn(current) {
    value = current
    current
  })
  match value {
    Some(symbol) => Some(symbol_prop_key(symbol))
    None => None
  }
}

///|
fn symbol_iterator_key() -> String? {
  symbol_key_from_ref(symbol_iterator_ref)
}

///|
fn symbol_to_string_tag_key() -> String? {
  symbol_key_from_ref(symbol_to_string_tag_ref)
}

///|
fn symbol_async_iterator_key() -> String? {
  symbol_key_from_ref(symbol_async_iterator_ref)
}

///|
fn symbol_has_instance_key() -> String? {
  symbol_key_from_ref(symbol_has_instance_ref)
}

///|
fn symbol_is_concat_spreadable_key() -> String? {
  symbol_key_from_ref(symbol_is_concat_spreadable_ref)
}

///|
fn symbol_match_key() -> String? {
  symbol_key_from_ref(symbol_match_ref)
}

///|
fn symbol_match_all_key() -> String? {
  symbol_key_from_ref(symbol_match_all_ref)
}

///|
fn symbol_replace_key() -> String? {
  symbol_key_from_ref(symbol_replace_ref)
}

///|
fn symbol_search_key() -> String? {
  symbol_key_from_ref(symbol_search_ref)
}

///|
fn symbol_species_key() -> String? {
  symbol_key_from_ref(symbol_species_ref)
}

///|
fn symbol_split_key() -> String? {
  symbol_key_from_ref(symbol_split_ref)
}

///|
fn symbol_to_primitive_key() -> String? {
  symbol_key_from_ref(symbol_to_primitive_ref)
}

///|
fn symbol_unscopables_key() -> String? {
  symbol_key_from_ref(symbol_unscopables_ref)
}

///|
fn symbol_to_string(symbol : SymbolValue) -> String {
  match symbol.description {
    Some(desc) => "Symbol(" + desc + ")"
    None => "Symbol()"
  }
}

///|
fn symbol_from_value(value : Value) -> SymbolValue? {
  match value {
    Symbol(symbol) => Some(symbol)
    Object(obj) => obj.symbol_data
    _ => None
  }
}

///|
fn rc_replace_optional_symbol(
  old_value : SymbolValue?,
  new_value : SymbolValue?,
) -> Unit {
  match (old_value, new_value) {
    (None, None) => ()
    (Some(old), Some(new)) => rc_replace_value(Symbol(old), Symbol(new))
    (Some(old), None) => rc_decref_value(Symbol(old))
    (None, Some(new)) => rc_incref_value(Symbol(new))
  }
}

///|
fn set_symbol_ref(cell : Ref[SymbolValue?], value : SymbolValue?) -> Unit {
  let mut prev : SymbolValue? = None
  cell.update(fn(current) {
    prev = current
    value
  })
  rc_replace_optional_symbol(prev, value)
}

///|
fn is_symbol_like(value : Value) -> Bool {
  symbol_from_value(value) is Some(_)
}
