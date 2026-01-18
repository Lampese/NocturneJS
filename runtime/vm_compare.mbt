///|
fn strict_eq(a : Value, b : Value) -> Bool {
  match (a, b) {
    (Undefined, Undefined) => true
    (Null, Null) => true
    (Bool(x), Bool(y)) => x == y
    (Number(x), Number(y)) => Double::equal(x, y)
    (String(x), String(y)) => x == y
    (BigInt(x), BigInt(y)) => x.equal(y)
    (Symbol(x), Symbol(y)) => x.id == y.id
    (Function(x), Function(y)) => x.id == y.id
    (BoundFunction(x), BoundFunction(y)) => x.id == y.id
    (Object(x), Object(y)) => x.id == y.id
    (Builtin(x), Builtin(y)) => x.id == y.id
    (Array(x), Array(y)) => x.id == y.id
    (Arguments(x), Arguments(y)) => x.id == y.id
    _ => false
  }
}

///|
fn loose_eq(a : Value, b : Value) -> Bool raise {
  if strict_eq(a, b) {
    return true
  }
  if is_html_dda_value(a) {
    match b {
      Null | Undefined => return true
      _ => ()
    }
  }
  if is_html_dda_value(b) {
    match a {
      Null | Undefined => return true
      _ => ()
    }
  }
  match (a, b) {
    (Null, Undefined) => true
    (Undefined, Null) => true
    (BigInt(x), Number(y)) => bigint_equal_number(x, y)
    (Number(x), BigInt(y)) => bigint_equal_number(y, x)
    (BigInt(x), String(y)) => bigint_equal_string(x, y)
    (String(x), BigInt(y)) => bigint_equal_string(y, x)
    (BigInt(x), BigInt(y)) => x.equal(y)
    (Bool(_), _) => loose_eq(Number(to_number(a)), b)
    (_, Bool(_)) => loose_eq(a, Number(to_number(b)))
    (Number(_), String(_)) =>
      strict_eq(Number(to_number(a)), Number(to_number(b)))
    (String(_), Number(_)) =>
      strict_eq(Number(to_number(a)), Number(to_number(b)))
    _ =>
      if is_object_like(a) && is_object_like(b) {
        false
      } else if is_object_like(a) {
        let prim = to_primitive_default(a)
        loose_eq(prim, b)
      } else if is_object_like(b) {
        let prim = to_primitive_default(b)
        loose_eq(a, prim)
      } else {
        false
      }
  }
}

///|
fn bigint_equal_number(value : @bigint.BigInt, num : Double) -> Bool {
  match bigint_from_double(num) {
    Some(other) => value.equal(other)
    None => false
  }
}

///|
fn bigint_equal_string(value : @bigint.BigInt, text : String) -> Bool {
  match parse_bigint_string_inner(text) {
    Some(other) => value.equal(other)
    None => false
  }
}

///|
enum CompareOp {
  CmpLt
  CmpLte
  CmpGt
  CmpGte
} derive(Show)

///|
fn compare_values(left : Value, right : Value, op : CompareOp) -> Bool raise {
  let mut l = left
  let mut r = right
  if is_object_like(l) {
    l = to_primitive(l, false)
  }
  if is_object_like(r) {
    r = to_primitive(r, false)
  }
  match (l, r) {
    (String(a), String(b)) => compare_string(a, b, op)
    (BigInt(a), String(text)) =>
      match parse_bigint_string_inner(text) {
        Some(b) => compare_bigint(a, b, op)
        None => false
      }
    (String(text), BigInt(b)) =>
      match parse_bigint_string_inner(text) {
        Some(a) => compare_bigint(a, b, op)
        None => false
      }
    (BigInt(a), BigInt(b)) => compare_bigint(a, b, op)
    (BigInt(a), _) => compare_bigint_number(a, to_number(r), op)
    (_, BigInt(b)) =>
      compare_bigint_number(b, to_number(l), reverse_compare_op(op))
    _ => {
      let l_num = to_number(l)
      let r_num = to_number(r)
      compare_number(l_num, r_num, op)
    }
  }
}

///|
fn compare_string(left : String, right : String, op : CompareOp) -> Bool {
  let cmp = string_lex_compare(left, right)
  match op {
    CmpLt => cmp < 0
    CmpLte => cmp <= 0
    CmpGt => cmp > 0
    CmpGte => cmp >= 0
  }
}

///|
fn string_lex_compare(left : String, right : String) -> Int {
  left.view().lexical_compare(right.view())
}

///|
fn compare_number(left : Double, right : Double, op : CompareOp) -> Bool {
  match op {
    CmpLt => left < right
    CmpLte => left <= right
    CmpGt => left > right
    CmpGte => left >= right
  }
}

///|
fn reverse_compare_op(op : CompareOp) -> CompareOp {
  match op {
    CmpLt => CmpGt
    CmpLte => CmpGte
    CmpGt => CmpLt
    CmpGte => CmpLte
  }
}

///|
fn compare_bigint(
  left : @bigint.BigInt,
  right : @bigint.BigInt,
  op : CompareOp,
) -> Bool {
  let cmp = left.compare(right)
  match op {
    CmpLt => cmp < 0
    CmpLte => cmp <= 0
    CmpGt => cmp > 0
    CmpGte => cmp >= 0
  }
}

///|
fn compare_bigint_number(
  left : @bigint.BigInt,
  right : Double,
  op : CompareOp,
) -> Bool {
  match bigint_number_cmp(left, right) {
    Some(cmp) =>
      match op {
        CmpLt => cmp < 0
        CmpLte => cmp <= 0
        CmpGt => cmp > 0
        CmpGte => cmp >= 0
      }
    None => false
  }
}

///|
fn bigint_number_cmp(left : @bigint.BigInt, right : Double) -> Int? {
  if Double::is_nan(right) {
    return None
  }
  if Double::is_inf(right) {
    return if right > 0.0 { Some(-1) } else { Some(1) }
  }
  let negative = right < 0.0
  let abs_value = Double::abs(right)
  let (mantissa, exp2) = double_to_parts(abs_value)
  let (num, den) = parts_to_fraction(mantissa, exp2)
  let signed_num = if negative { num.neg() } else { num }
  let left_scaled = left.mul(den)
  let cmp = left_scaled.compare(signed_num)
  if cmp < 0 {
    Some(-1)
  } else if cmp > 0 {
    Some(1)
  } else {
    Some(0)
  }
}
