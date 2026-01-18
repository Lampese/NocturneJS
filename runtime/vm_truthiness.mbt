///|
fn is_html_dda_value(value : Value) -> Bool {
  match value {
    Builtin(builtin) => builtin.is_html_dda
    Object(obj) => obj.is_html_dda
    _ => false
  }
}

///|
fn is_truthy(value : Value) -> Bool {
  if is_html_dda_value(value) {
    return false
  }
  match value {
    Undefined => false
    Null => false
    Bool(v) => v
    Number(v) => !Double::is_nan(v) && v != 0.0
    String(s) => !s.is_empty()
    BigInt(v) => !v.is_zero()
    Symbol(_) => true
    Builtin(_) => true
    Function(_) => true
    BoundFunction(_) => true
    Object(_) => true
    Array(_) => true
    Arguments(_) => true
  }
}

///|
fn is_nullish(value : Value) -> Bool {
  match value {
    Undefined => true
    Null => true
    _ => false
  }
}
