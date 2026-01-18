///|
let math_rand_state_ref : Ref[UInt64] = Ref::new(0x6a09e667f3bcc909UL)

///|
fn imul(left : Double, right : Double) -> Int {
  let a = to_int32(left)
  let b = to_int32(right)
  let product = Int64::from_int(a) * Int64::from_int(b)
  let two32 = 1L << 32
  let mut n = Int64::mod(product, two32)
  if n < 0L {
    n = n + two32
  }
  if n >= 1L << 31 {
    n = n - two32
  }
  Int64::to_int(n)
}

///|
fn math_hypot(args : Array[Value]) -> Double raise {
  if args.is_empty() {
    return 0.0
  }
  let mut r = 0.0
  let mut has_value = false
  let mut saw_nan = false
  let mut saw_inf = false
  let mut index = 0
  while index < args.length() {
    let value = to_number(args[index])
    if Double::is_inf(value) {
      saw_inf = true
    } else if Double::is_nan(value) {
      saw_nan = true
    } else if !has_value {
      r = Double::abs(value)
      has_value = true
    } else {
      r = @math.hypot(r, value)
    }
    index = index + 1
  }
  if saw_inf {
    inf()
  } else if saw_nan {
    nan()
  } else {
    r
  }
}

///|
fn math_cosh_value(value : Double) -> Double {
  if Double::is_nan(value) {
    return value
  }
  let abs_value = Double::abs(value)
  if Double::is_inf(abs_value) {
    return inf()
  }
  // Use libm-style thresholds to match QuickJS behavior.
  if abs_value < 0.34657359027997264 {
    let t = @math.expm1(abs_value)
    let w = 1.0 + t
    return 1.0 + t * t / (2.0 * w)
  }
  if abs_value < 22.0 {
    let t = @math.exp(abs_value)
    return 0.5 * t + 0.5 / t
  }
  if abs_value < 709.78271289338397 {
    return 0.5 * @math.exp(abs_value)
  }
  if abs_value < 710.4758600739439 {
    let w = @math.exp(0.5 * abs_value)
    return 0.5 * w * w
  }
  inf()
}

///|
fn math_max(args : Array[Value]) -> Double raise {
  if args.is_empty() {
    return 0.0 - inf()
  }
  let mut result = to_number(args[0])
  let mut index = 1
  while index < args.length() {
    let value = to_number(args[index])
    if !Double::is_nan(result) {
      if Double::is_nan(value) {
        result = value
      } else if value > result {
        result = value
      } else if value == 0.0 && result == 0.0 {
        if is_negative_zero(result) && !is_negative_zero(value) {
          result = value
        }
      }
    }
    index = index + 1
  }
  result
}

///|
fn math_min(args : Array[Value]) -> Double raise {
  if args.is_empty() {
    return inf()
  }
  let mut result = to_number(args[0])
  let mut index = 1
  while index < args.length() {
    let value = to_number(args[index])
    if !Double::is_nan(result) {
      if Double::is_nan(value) {
        result = value
      } else if value < result {
        result = value
      } else if value == 0.0 && result == 0.0 {
        if !is_negative_zero(result) && is_negative_zero(value) {
          result = value
        }
      }
    }
    index = index + 1
  }
  result
}

///|
fn math_round_value(num : Double) -> Double {
  let mut bits = Double::reinterpret_as_uint64(num)
  let e = UInt64::to_int((bits >> 52) & 0x7ffUL)
  if e < 1023 {
    if e == 1022 && bits != 0xbfe0000000000000UL {
      bits = (bits & (1UL << 63)) | (1023UL << 52)
    } else {
      bits = bits & (1UL << 63)
    }
    return UInt64::reinterpret_as_double(bits)
  }
  if e < 1023 + 52 {
    let s = bits >> 63
    let shift = 52 - (e - 1023)
    let one = 1UL << shift
    let frac_mask = one - 1UL
    bits = bits + (one >> 1) - s
    bits = bits & UInt64::lnot(frac_mask)
  }
  UInt64::reinterpret_as_double(bits)
}

///|
fn math_trunc_value(num : Double) -> Double {
  if Double::is_nan(num) || Double::is_inf(num) || num == 0.0 {
    return num
  }
  let truncated = Double::trunc(num)
  if truncated == 0.0 && (num < 0.0 || is_negative_zero(num)) {
    negative_zero()
  } else {
    truncated
  }
}

///|
fn math_sign_value(num : Double) -> Double {
  if Double::is_nan(num) {
    return nan()
  }
  if num == 0.0 {
    return if is_negative_zero(num) { negative_zero() } else { 0.0 }
  }
  if num > 0.0 {
    1.0
  } else {
    -1.0
  }
}

///|
fn math_clz32_value(num : Double) -> Int {
  let value = UInt::land(to_uint32(num), 0xffffffffU)
  if value == 0U {
    return 32
  }
  let mut count = 0
  let mut mask = 0x80000000U
  while UInt::land(value, mask) == 0U {
    count = count + 1
    mask = mask >> 1
  }
  count
}

///|
fn math_random_value() -> Double {
  let mut value = 0.0
  let mask = (1UL << 53) - 1UL
  let denom = Double::convert_uint64(1UL << 53)
  math_rand_state_ref.update(fn(state) {
    let next = state * 6364136223846793005UL + 1UL
    let mantissa = (next >> 11) & mask
    value = Double::convert_uint64(mantissa) / denom
    next
  })
  value
}

///|
fn math_f16round_value(num : Double) -> Double {
  let bits = double_to_float16_bits(num)
  float16_bits_to_double(bits)
}

///|
priv enum SumPreciseStateEnum {
  Finite
  Infinity
  MinusInfinity
  Nan
} derive(Eq)

///|
priv struct SumPreciseState {
  mut state : SumPreciseStateEnum
  mut counter : Int
  mut n_limbs : Int
  acc : Array[Int64]
}

///|
let sp_limb_bits : Int = 56

///|
let sp_rnd_bits : Int = sp_limb_bits - 53

///|
let sum_precise_acc_len : Int = 39

///|
let sum_precise_counter_init : Int = 250

///|
let sp_limb_mask_u64 : UInt64 = (1UL << sp_limb_bits) - 1UL

///|
let sp_limb_mask_i64 : Int64 = (1L << sp_limb_bits) - 1L

///|
fn sum_precise_init() -> SumPreciseState {
  SumPreciseState::{
    state: SumPreciseStateEnum::Finite,
    counter: sum_precise_counter_init,
    n_limbs: 0,
    acc: Array::make(sum_precise_acc_len, 0L),
  }
}

///|
fn sum_precise_renorm(state : SumPreciseState) -> Unit {
  let mut carry : Int64 = 0L
  let mut i = 0
  while i < state.n_limbs {
    let v = state.acc[i] + carry
    state.acc[i] = Int64::land(v, sp_limb_mask_i64)
    carry = v >> sp_limb_bits
    i = i + 1
  }
  if carry != 0L && state.n_limbs < sum_precise_acc_len {
    state.acc[state.n_limbs] = carry
    state.n_limbs = state.n_limbs + 1
  }
}

///|
fn sum_precise_add(state : SumPreciseState, value : Double) -> Unit {
  let bits = Double::reinterpret_as_uint64(value)
  let sgn = bits >> 63 != 0UL
  let e = UInt64::to_int((bits >> 52) & 0x7ffUL)
  let mut m = bits & ((1UL << 52) - 1UL)
  if e == 2047 {
    if m == 0UL {
      if state.state == SumPreciseStateEnum::Nan ||
        (state.state == SumPreciseStateEnum::MinusInfinity && !sgn) ||
        (state.state == SumPreciseStateEnum::Infinity && sgn) {
        state.state = SumPreciseStateEnum::Nan
      } else {
        state.state = if sgn {
          SumPreciseStateEnum::MinusInfinity
        } else {
          SumPreciseStateEnum::Infinity
        }
      }
    } else {
      state.state = SumPreciseStateEnum::Nan
    }
    return
  }
  let mut shift : Int = 0
  let mut p : Int = 0
  if e == 0 {
    if m == 0UL {
      if state.n_limbs == 0 && !sgn {
        state.n_limbs = 1
      }
      return
    }
  } else {
    m = m | (1UL << 52)
    shift = e - 1
    p = shift / sp_limb_bits
    shift = shift % sp_limb_bits
  }
  let a0 = (m << shift) & sp_limb_mask_u64
  let a1 = m >> (sp_limb_bits - shift)
  let a0_i = a0.reinterpret_as_int64()
  let a1_i = a1.reinterpret_as_int64()
  if !sgn {
    state.acc[p] = state.acc[p] + a0_i
    state.acc[p + 1] = state.acc[p + 1] + a1_i
  } else {
    state.acc[p] = state.acc[p] - a0_i
    state.acc[p + 1] = state.acc[p + 1] - a1_i
  }
  state.n_limbs = Int::max(state.n_limbs, p + 2)
  state.counter = state.counter - 1
  if state.counter == 0 {
    state.counter = sum_precise_counter_init
    sum_precise_renorm(state)
  }
}

///|
fn sum_precise_get_result(state : SumPreciseState) -> Double {
  match state.state {
    SumPreciseStateEnum::Finite => ()
    SumPreciseStateEnum::Infinity => return inf()
    SumPreciseStateEnum::MinusInfinity => return -inf()
    SumPreciseStateEnum::Nan => return nan()
  }
  sum_precise_renorm(state)
  let mut n = state.n_limbs
  if n == 0 {
    return -0.0
  }
  while n > 0 && state.acc[n - 1] == 0L {
    n = n - 1
  }
  if n == 0 {
    return 0.0
  }
  let is_neg = state.acc[n - 1] < 0L
  if is_neg {
    let mut carry : Int64 = 1L
    let mut i = 0
    while i < n - 1 {
      let v = sp_limb_mask_i64 - state.acc[i] + carry
      state.acc[i] = Int64::land(v, sp_limb_mask_i64)
      carry = v >> sp_limb_bits
      i = i + 1
    }
    state.acc[n - 1] = -state.acc[n - 1] + carry - 1L
    while n > 1 && state.acc[n - 1] == 0L {
      n = n - 1
    }
  }
  if n == 1 && state.acc[0] < 1L << 52 {
    let sign_bit = if is_neg { 1UL << 63 } else { 0UL }
    let mantissa = state.acc[0].reinterpret_as_uint64()
    return UInt64::reinterpret_as_double(sign_bit | mantissa)
  }
  let mut e = n * sp_limb_bits
  let mut p = n - 1
  let mut m = state.acc[p].reinterpret_as_uint64()
  let shift = m.clz() - (64 - sp_limb_bits)
  e = e - shift - 52
  if shift != 0 {
    m = m << shift
    if p > 0 {
      p = p - 1
      let shift1 = sp_limb_bits - shift
      let prev = state.acc[p].reinterpret_as_uint64()
      let nz = prev & ((1UL << shift1) - 1UL)
      let sticky = if nz != 0UL { 1UL } else { 0UL }
      m = m | (prev >> shift1) | sticky
    }
  }
  if (m & ((1UL << sp_rnd_bits) - 1UL)) == 1UL << (sp_rnd_bits - 1) {
    while p > 0 {
      p = p - 1
      if state.acc[p] != 0L {
        m = m | 1UL
        break
      }
    }
  }
  let addend = (1UL << (sp_rnd_bits - 1)) - 1UL + ((m >> sp_rnd_bits) & 1UL)
  m = (m + addend) >> sp_rnd_bits
  if m == 1UL << 53 {
    e = e + 1
  }
  let sign_bit = if is_neg { 1UL << 63 } else { 0UL }
  if e >= 2047 {
    return UInt64::reinterpret_as_double(sign_bit | (2047UL << 52))
  }
  let exp_bits = Int::to_uint64(e) << 52
  let mantissa = m & ((1UL << 52) - 1UL)
  UInt64::reinterpret_as_double(sign_bit | exp_bits | mantissa)
}

///|
fn math_sum_precise(args : Array[Value]) -> Value raise {
  let value = if args.is_empty() { Undefined } else { args[0] }
  let (iterator, next_method) = get_iterator_from_value(value)
  let state = sum_precise_init()
  while true {
    let (done, item) = iterator_step_value(iterator, next_method)
    if done {
      break
    }
    match item {
      Number(value) => sum_precise_add(state, value)
      _ => {
        let _ = iterator_close_on_error(iterator)
        return throw_type_error("not a number")
      }
    }
  }
  Number(sum_precise_get_result(state))
}
