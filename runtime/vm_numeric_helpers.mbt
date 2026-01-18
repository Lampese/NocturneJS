///|
fn to_int32(value : Double) -> Int {
  if Double::is_nan(value) || Double::is_inf(value) {
    return 0
  }
  let trunc = Double::trunc(value)
  let two32 = 4294967296.0
  let mut n = Double::mod(trunc, two32)
  if n < 0.0 {
    n = n + two32
  }
  if n >= 2147483648.0 {
    n = n - two32
  }
  Double::to_int(n)
}

///|
fn to_uint32(value : Double) -> UInt {
  if Double::is_nan(value) || Double::is_inf(value) {
    return UInt::default()
  }
  let trunc = Double::trunc(value)
  let two32 = 4294967296.0
  let mut n = Double::mod(trunc, two32)
  if n < 0.0 {
    n = n + two32
  }
  UInt::trunc_double(n)
}

///|
fn shift_count(value : Value) -> Int raise {
  let count = to_int32(to_number(value))
  Int::land(count, 0x1f)
}

///|
fn bigint_shift_count(value : @bigint.BigInt) -> Int {
  let max_shift = 2147483647
  if value.compare_int(max_shift) > 0 {
    return max_shift
  }
  if value.compare_int(-max_shift) < 0 {
    return -max_shift
  }
  value.to_int()
}

///|
fn bigint_shift_right_floor(
  value : @bigint.BigInt,
  shift : Int,
) -> @bigint.BigInt {
  if shift <= 0 {
    return value
  }
  if value.compare_int(0) >= 0 {
    return value >> shift
  }
  let abs = value.neg()
  let trunc = abs >> shift
  let restored = trunc << shift
  if restored.equal(abs) {
    trunc.neg()
  } else {
    trunc.neg().sub(@bigint.BigInt::from_int(1))
  }
}
