///|
fn same_value_zero(a : Value, b : Value) -> Bool {
  match (a, b) {
    (Number(x), Number(y)) =>
      if Double::is_nan(x) && Double::is_nan(y) {
        true
      } else {
        Double::equal(x, y)
      }
    _ => strict_eq(a, b)
  }
}

///|
fn is_negative_zero(value : Double) -> Bool {
  value == 0.0 && Double::reinterpret_as_uint64(value) == 0x8000000000000000UL
}

///|
fn map_normalize_key(key : Value) -> Value {
  match key {
    Number(num) => if is_negative_zero(num) { Number(0.0) } else { key }
    _ => key
  }
}

///|
fn same_value(a : Value, b : Value) -> Bool {
  match (a, b) {
    (Number(x), Number(y)) =>
      if Double::is_nan(x) && Double::is_nan(y) {
        true
      } else if x == 0.0 && y == 0.0 {
        is_negative_zero(x) == is_negative_zero(y)
      } else {
        Double::equal(x, y)
      }
    _ => strict_eq(a, b)
  }
}

///|
fn is_weakref_target(value : Value) -> Bool {
  match value {
    Symbol(symbol) => symbol.key is None
    _ => is_object_like(value)
  }
}
