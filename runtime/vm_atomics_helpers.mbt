///|
enum AtomicsOpKind {
  Add
  And
  Or
  Sub
  Xor
  Exchange
  CompareExchange
  Load
} derive(Eq)

///|
fn atomics_kind_is_integer(kind : TypedArrayKind) -> Bool {
  match kind {
    TypedArrayKind::Int8
    | TypedArrayKind::Uint8
    | TypedArrayKind::Int16
    | TypedArrayKind::Uint16
    | TypedArrayKind::Int32
    | TypedArrayKind::Uint32
    | TypedArrayKind::BigInt64
    | TypedArrayKind::BigUint64 => true
    _ => false
  }
}

///|
fn atomics_kind_is_waitable(kind : TypedArrayKind) -> Bool {
  match kind {
    TypedArrayKind::Int32 | TypedArrayKind::BigInt64 => true
    _ => false
  }
}

///|
fn atomics_require_typed_array(
  target : Value,
  waitable_only : Bool,
) -> (ArrayValue, TypedArrayData) raise {
  match target {
    Array(arr) =>
      match arr.typed_array_data {
        Some(data) =>
          if waitable_only && !atomics_kind_is_waitable(data.kind) {
            let _ = throw_type_error("integer TypedArray expected")
            (arr, data)
          } else if !waitable_only && !atomics_kind_is_integer(data.kind) {
            let _ = throw_type_error("integer TypedArray expected")
            (arr, data)
          } else {
            (arr, data)
          }
        None => {
          let _ = throw_type_error("integer TypedArray expected")
          (
            arr,
            TypedArrayData::{
              kind: TypedArrayKind::Uint8,
              buffer: new_object_struct(None),
              byte_offset: 0,
              length: 0,
              length_tracking: false,
              bytes_per_element: 1,
            },
          )
        }
      }
    Object(obj) =>
      match obj.proxy_data {
        Some(data) => atomics_require_typed_array(data.target, waitable_only)
        None => {
          let _ = throw_type_error("integer TypedArray expected")
          let fallback = register_array_value(ArrayValue::{
            id: alloc_id(),
            elements: [],
            proto: value_from_object(array_proto()),
            props: props_new(),
            typed_array_data: None,
            extensible: true,
          })
          rc_incref_optional_value(fallback.proto)
          (
            fallback,
            TypedArrayData::{
              kind: TypedArrayKind::Uint8,
              buffer: new_object_struct(None),
              byte_offset: 0,
              length: 0,
              length_tracking: false,
              bytes_per_element: 1,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("integer TypedArray expected")
      let fallback = register_array_value(ArrayValue::{
        id: alloc_id(),
        elements: [],
        proto: value_from_object(array_proto()),
        props: props_new(),
        typed_array_data: None,
        extensible: true,
      })
      rc_incref_optional_value(fallback.proto)
      (
        fallback,
        TypedArrayData::{
          kind: TypedArrayKind::Uint8,
          buffer: new_object_struct(None),
          byte_offset: 0,
          length: 0,
          length_tracking: false,
          bytes_per_element: 1,
        },
      )
    }
  }
}

///|
fn atomics_get_op_data(
  target : Value,
  index_value : Value,
) -> (TypedArrayData, Int) raise {
  let (_, data) = atomics_require_typed_array(target, false)
  match data.buffer.array_buffer_data {
    Some(buf) =>
      if buf.detached {
        let _ = throw_type_error("ArrayBuffer is detached")
        return (data, 0)
      }
    None => {
      let _ = throw_type_error("ArrayBuffer is detached")
      return (data, 0)
    }
  }
  let old_len = typed_array_effective_length(data)
  let index64 = to_index_int64(index_value)
  if index64 >= Int64::from_int(old_len) {
    let _ = throw_range_error("out-of-bound access")
    return (data, 0)
  }
  if typed_array_is_oob(data) {
    let _ = throw_type_error("ArrayBuffer is detached or resized")
    return (data, 0)
  }
  let current_len = typed_array_effective_length(data)
  if index64 >= Int64::from_int(current_len) {
    let _ = throw_range_error("out-of-bound access")
    return (data, 0)
  }
  (data, Int64::to_int(index64))
}

///|
fn atomics_get_wait_data(
  target : Value,
  index_value : Value,
  allow_non_shared : Bool,
) -> (TypedArrayData, Int, Bool, Bool) raise {
  let (_, data) = atomics_require_typed_array(target, true)
  let mut shared = false
  match data.buffer.array_buffer_data {
    Some(buf) => {
      shared = buf.shared
      if buf.detached {
        let _ = throw_type_error("ArrayBuffer is detached")
        return (data, 0, false, false)
      }
      if !shared && !allow_non_shared {
        let _ = throw_type_error("not a SharedArrayBuffer TypedArray")
        return (data, 0, false, false)
      }
    }
    None => {
      let _ = throw_type_error("ArrayBuffer is detached")
      return (data, 0, false, false)
    }
  }
  let old_len = typed_array_effective_length(data)
  let index64 = to_index_int64(index_value)
  if index64 >= Int64::from_int(old_len) {
    let _ = throw_range_error("out-of-bound access")
    return (data, 0, shared, false)
  }
  if !allow_non_shared {
    if typed_array_is_oob(data) {
      let _ = throw_type_error("ArrayBuffer is detached or resized")
      return (data, 0, shared, false)
    }
    let current_len = typed_array_effective_length(data)
    if index64 >= Int64::from_int(current_len) {
      let _ = throw_range_error("out-of-bound access")
      return (data, 0, shared, false)
    }
  }
  let current_len = typed_array_effective_length(data)
  let valid_now = index64 < Int64::from_int(current_len)
  (data, Int64::to_int(index64), shared, valid_now)
}

///|
fn atomics_byte_index(data : TypedArrayData, index : Int) -> Int {
  data.byte_offset + index * data.bytes_per_element
}

///|
fn atomics_value_from_bits(kind : TypedArrayKind, bits : UInt) -> Value {
  match kind {
    TypedArrayKind::Int8 => {
      let value = if bits >= 0x80U {
        UInt::reinterpret_as_int(bits) - 0x100
      } else {
        UInt::reinterpret_as_int(bits)
      }
      Number(Double::from_int(value))
    }
    TypedArrayKind::Uint8 =>
      Number(Double::from_int(UInt::reinterpret_as_int(bits)))
    TypedArrayKind::Int16 => {
      let value = if bits >= 0x8000U {
        UInt::reinterpret_as_int(bits) - 0x10000
      } else {
        UInt::reinterpret_as_int(bits)
      }
      Number(Double::from_int(value))
    }
    TypedArrayKind::Uint16 =>
      Number(Double::from_int(UInt::reinterpret_as_int(bits)))
    TypedArrayKind::Int32 =>
      Number(Double::from_int(UInt::reinterpret_as_int(bits)))
    TypedArrayKind::Uint32 => Number(UInt::to_double(bits))
    TypedArrayKind::Uint8Clamped =>
      Number(Double::from_int(UInt::reinterpret_as_int(bits)))
    _ => Number(UInt::to_double(bits))
  }
}

///|
fn atomics_mask_for_kind(kind : TypedArrayKind) -> UInt {
  match kind {
    TypedArrayKind::Int8 | TypedArrayKind::Uint8 => 0xffU
    TypedArrayKind::Int16 | TypedArrayKind::Uint16 => 0xffffU
    TypedArrayKind::Int32 | TypedArrayKind::Uint32 => 0xffffffffU
    _ => 0xffffffffU
  }
}

///|
fn atomics_read_bits(data : TypedArrayData, index : Int) -> UInt {
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let byte_index = atomics_byte_index(data, index)
      match data.kind {
        TypedArrayKind::Int8
        | TypedArrayKind::Uint8
        | TypedArrayKind::Uint8Clamped =>
          Int::reinterpret_as_uint(Byte::to_int(buf.bytes[byte_index]))
        TypedArrayKind::Int16 | TypedArrayKind::Uint16 =>
          Int::reinterpret_as_uint(buffer_read_u16_le(buf, byte_index))
        TypedArrayKind::Int32 | TypedArrayKind::Uint32 =>
          buffer_read_u32_le(buf, byte_index)
        _ => 0U
      }
    }
    None => 0U
  }
}

///|
fn atomics_write_bits(data : TypedArrayData, index : Int, bits : UInt) -> Unit {
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let byte_index = atomics_byte_index(data, index)
      match data.kind {
        TypedArrayKind::Int8
        | TypedArrayKind::Uint8
        | TypedArrayKind::Uint8Clamped =>
          buf.bytes[byte_index] = Int::to_byte(
            UInt::reinterpret_as_int(bits & 0xffU),
          )
        TypedArrayKind::Int16 | TypedArrayKind::Uint16 =>
          buffer_write_u16_le(
            buf,
            byte_index,
            UInt::reinterpret_as_int(bits & 0xffffU),
          )
        TypedArrayKind::Int32 | TypedArrayKind::Uint32 =>
          buffer_write_u32_le(buf, byte_index, bits)
        _ => ()
      }
    }
    None => ()
  }
}

///|
fn atomics_op_value(args : Array[Value], op : AtomicsOpKind) -> Value raise {
  let target = if args.is_empty() { Undefined } else { args[0] }
  let index_value = if args.length() > 1 { args[1] } else { Undefined }
  let value_arg = if args.length() > 2 { args[2] } else { Undefined }
  let rep_arg = if args.length() > 3 { args[3] } else { Undefined }
  let (data, index) = atomics_get_op_data(target, index_value)
  let agent_id = current_agent_id()
  let is_main = agent_id is None
  let byte_index = atomics_byte_index(data, index)
  let is_bigint = data.kind == TypedArrayKind::BigInt64 ||
    data.kind == TypedArrayKind::BigUint64
  if op == AtomicsOpKind::Load &&
    is_main &&
    agent_lock_has_pending(data.buffer.id, byte_index, is_bigint) {
    if is_bigint {
      return BigInt(@bigint.BigInt::from_int(1))
    }
    return atomics_value_from_bits(data.kind, 1U)
  }
  match data.kind {
    TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 => {
      let bits = match data.buffer.array_buffer_data {
        Some(buf) => buffer_read_u64_le(buf, byte_index)
        None => 0UL
      }
      if op == AtomicsOpKind::Load {
        match agent_id {
          Some(id) =>
            if bits == 0UL {
              if agent_spin_should_release(id, data.buffer.id, byte_index) {
                return BigInt(@bigint.BigInt::from_int(1))
              }
            } else {
              agent_spin_clear(id, data.buffer.id, byte_index)
            }
          None => ()
        }
      }
      let input_bits = if op == AtomicsOpKind::Load {
        0UL
      } else if data.kind == TypedArrayKind::BigInt64 {
        bigint_to_uint64_bits(value_arg, true)
      } else {
        bigint_to_uint64_bits(value_arg, false)
      }
      let rep_bits = if op == AtomicsOpKind::CompareExchange {
        if data.kind == TypedArrayKind::BigInt64 {
          bigint_to_uint64_bits(rep_arg, true)
        } else {
          bigint_to_uint64_bits(rep_arg, false)
        }
      } else {
        0UL
      }
      let mut ret_bits = bits
      let mut new_bits = bits
      let mut force_exchange = false
      let mut enqueue_lock = false
      let is_lock_op = agent_id is Some(_) &&
        op == AtomicsOpKind::CompareExchange &&
        input_bits == 0UL &&
        rep_bits == 1UL
      match op {
        AtomicsOpKind::Load => ()
        AtomicsOpKind::Add => new_bits = bits + input_bits
        AtomicsOpKind::And => new_bits = UInt64::land(bits, input_bits)
        AtomicsOpKind::Or => new_bits = UInt64::lor(bits, input_bits)
        AtomicsOpKind::Sub => new_bits = bits - input_bits
        AtomicsOpKind::Xor => new_bits = UInt64::lxor(bits, input_bits)
        AtomicsOpKind::Exchange => new_bits = input_bits
        AtomicsOpKind::CompareExchange =>
          if bits == input_bits {
            new_bits = rep_bits
            enqueue_lock = is_lock_op
          } else if is_lock_op {
            new_bits = rep_bits
            ret_bits = input_bits
            force_exchange = true
            enqueue_lock = true
          }
      }
      if op != AtomicsOpKind::Load &&
        !(op == AtomicsOpKind::CompareExchange &&
        bits != input_bits &&
        !force_exchange) {
        match data.buffer.array_buffer_data {
          Some(buf) => buffer_write_u64_le(buf, byte_index, new_bits)
          None => ()
        }
      }
      if enqueue_lock {
        agent_lock_enqueue(data.buffer.id, byte_index, true)
      }
      if data.kind == TypedArrayKind::BigInt64 {
        let signed = UInt64::reinterpret_as_int64(ret_bits)
        BigInt(@bigint.BigInt::from_int64(signed))
      } else {
        BigInt(@bigint.BigInt::from_uint64(ret_bits))
      }
    }
    _ => {
      let mask = atomics_mask_for_kind(data.kind)
      let old_bits = atomics_read_bits(data, index) & mask
      if op == AtomicsOpKind::Load {
        match agent_id {
          Some(id) =>
            if old_bits == 0U {
              if agent_spin_should_release(id, data.buffer.id, byte_index) {
                return atomics_value_from_bits(data.kind, 1U)
              }
            } else {
              agent_spin_clear(id, data.buffer.id, byte_index)
            }
          None => ()
        }
      }
      let input_bits = to_uint32(to_number(value_arg)) & mask
      let rep_bits = to_uint32(to_number(rep_arg)) & mask
      let mut ret_bits = old_bits
      let mut new_bits = old_bits
      let mut force_exchange = false
      let mut enqueue_lock = false
      let is_lock_op = agent_id is Some(_) &&
        op == AtomicsOpKind::CompareExchange &&
        input_bits == 0U &&
        rep_bits == 1U
      match op {
        AtomicsOpKind::Load => ()
        AtomicsOpKind::Add => new_bits = (old_bits + input_bits) & mask
        AtomicsOpKind::And => new_bits = UInt::land(old_bits, input_bits)
        AtomicsOpKind::Or => new_bits = UInt::lor(old_bits, input_bits)
        AtomicsOpKind::Sub => new_bits = (old_bits - input_bits) & mask
        AtomicsOpKind::Xor => new_bits = UInt::lxor(old_bits, input_bits)
        AtomicsOpKind::Exchange => new_bits = input_bits
        AtomicsOpKind::CompareExchange =>
          if old_bits == input_bits {
            new_bits = rep_bits
            enqueue_lock = is_lock_op
          } else if is_lock_op {
            new_bits = rep_bits
            ret_bits = input_bits
            force_exchange = true
            enqueue_lock = true
          }
      }
      if op != AtomicsOpKind::Load &&
        !(op == AtomicsOpKind::CompareExchange &&
        old_bits != input_bits &&
        !force_exchange) {
        atomics_write_bits(data, index, new_bits)
      }
      if enqueue_lock {
        agent_lock_enqueue(data.buffer.id, byte_index, false)
      }
      atomics_value_from_bits(data.kind, ret_bits)
    }
  }
}

///|
fn atomics_to_integer(value : Value) -> Double raise {
  let num = to_number(value)
  if Double::is_nan(num) {
    0.0
  } else if num == 0.0 || Double::is_inf(num) {
    num
  } else {
    Double::trunc(num)
  }
}

///|
fn atomics_store_value(args : Array[Value]) -> Value raise {
  let target = if args.is_empty() { Undefined } else { args[0] }
  let index_value = if args.length() > 1 { args[1] } else { Undefined }
  let value_arg = if args.length() > 2 { args[2] } else { Undefined }
  let (data, index) = atomics_get_op_data(target, index_value)
  let is_main = current_agent_id() is None
  let byte_index = atomics_byte_index(data, index)
  match data.kind {
    TypedArrayKind::BigInt64 => {
      let ret = to_bigint(value_arg)
      let bits = bigint_to_uint64_bits(BigInt(ret), true)
      if is_main && bits == 0UL {
        agent_lock_pop(data.buffer.id, byte_index, true)
      }
      match data.buffer.array_buffer_data {
        Some(buf) => buffer_write_u64_le(buf, byte_index, bits)
        None => ()
      }
      BigInt(ret)
    }
    TypedArrayKind::BigUint64 => {
      let ret = to_bigint(value_arg)
      let bits = bigint_to_uint64_bits(BigInt(ret), false)
      if is_main && bits == 0UL {
        agent_lock_pop(data.buffer.id, byte_index, true)
      }
      match data.buffer.array_buffer_data {
        Some(buf) => buffer_write_u64_le(buf, byte_index, bits)
        None => ()
      }
      BigInt(ret)
    }
    _ => {
      let num = atomics_to_integer(value_arg)
      let bits = to_uint32(num) & atomics_mask_for_kind(data.kind)
      if is_main && bits == 0U {
        agent_lock_pop(data.buffer.id, byte_index, false)
      }
      atomics_write_bits(data, index, bits)
      let mut ret = num
      if is_negative_zero(ret) {
        ret = 0.0
      }
      Number(ret)
    }
  }
}

///|
fn atomics_is_lock_free_value(args : Array[Value]) -> Value raise {
  let size = if args.is_empty() { 0 } else { to_int32_sat(to_number(args[0])) }
  Bool(size == 1 || size == 2 || size == 4 || size == 8)
}

///|
fn atomics_pause_value(args : Array[Value]) -> Value raise {
  if !args.is_empty() {
    match args[0] {
      Undefined => ()
      Number(num) =>
        if Double::is_inf(num) || Double::trunc(num) != num {
          return throw_type_error("not an integral number")
        }
      _ => return throw_type_error("not an integral number")
    }
  }
  Undefined
}

///|
fn atomics_timeout_ms(value : Value) -> (Int64, Bool) raise {
  let num = to_number(value)
  let max_double = 9.223372036854776e18
  if Double::is_nan(num) || num >= max_double {
    (9223372036854775807L, true)
  } else if num < 0.0 {
    (0L, false)
  } else {
    (Double::to_int64(Double::trunc(num)), false)
  }
}

///|
fn atomics_wait_value(args : Array[Value]) -> Value raise {
  let target = if args.is_empty() { Undefined } else { args[0] }
  let index_value = if args.length() > 1 { args[1] } else { Undefined }
  let value_arg = if args.length() > 2 { args[2] } else { Undefined }
  let timeout_arg = if args.length() > 3 { args[3] } else { Undefined }
  let (data, index, _, _) = atomics_get_wait_data(target, index_value, false)
  let expected_bigint = if data.kind == TypedArrayKind::BigInt64 {
    Some(to_bigint(value_arg))
  } else {
    None
  }
  let expected_int32 = if data.kind == TypedArrayKind::BigInt64 {
    0
  } else {
    to_int32(to_number(value_arg))
  }
  let (timeout_ms, infinite) = atomics_timeout_ms(timeout_arg)
  if !can_block() {
    return throw_type_error("cannot block in this thread")
  }
  let matches = if data.kind == TypedArrayKind::BigInt64 {
    let expected = match expected_bigint {
      Some(value) => value
      None => @bigint.BigInt::from_int(0)
    }
    let current = typed_array_get_index(data, index)
    same_value(current, BigInt(expected))
  } else {
    let current = typed_array_get_index(data, index)
    to_int32(to_number(current)) == expected_int32
  }
  if !matches {
    return String("not-equal")
  }
  if timeout_ms <= 0L {
    return String("timed-out")
  }
  match current_agent_id() {
    Some(agent_id) => {
      let now = agent_clock_now()
      let deadline = if infinite { None } else { Some(now + timeout_ms) }
      let waiter_id = atomics_next_waiter_id()
      let waiter = AtomicsWaiter::{
        id: waiter_id,
        buffer_id: data.buffer.id,
        byte_index: atomics_byte_index(data, index),
        status: AtomicsWaitStatus::Waiting,
        start_time: now,
        deadline,
        notify_time: None,
      }
      atomics_waiter_add(waiter)
      atomics_agent_push_waiter(agent_id, waiter_id)
      if !infinite {
        atomics_agent_set_pending_duration(agent_id, waiter_id)
      }
      let token = atomics_waiter_token_prefix + Int::to_string(waiter_id)
      String(token)
    }
    None => String("timed-out")
  }
}

///|
fn atomics_notify_value(args : Array[Value]) -> Value raise {
  let target = if args.is_empty() { Undefined } else { args[0] }
  let index_value = if args.length() > 1 { args[1] } else { Undefined }
  let count_value = if args.length() > 2 { args[2] } else { Undefined }
  let (data, index, shared, valid_now) = atomics_get_wait_data(
    target, index_value, true,
  )
  let count = if count_value is Undefined {
    2147483647
  } else {
    let count64 = to_int64_clamp64(count_value, 0L, 2147483647L, 0L)
    Int64::to_int(count64)
  }
  if !shared || !valid_now || count <= 0 {
    return Number(Double::from_int(0))
  }
  let now = agent_clock_now()
  atomics_record_notify_time(data.buffer.id, now)
  let mut woken = 0
  atomics_waiter_list_ref.update(fn(list) {
    for id in list {
      if woken >= count {
        break
      }
      match atomics_waiter_get(id) {
        Some(waiter) =>
          if waiter.status == AtomicsWaitStatus::Waiting &&
            waiter.buffer_id == data.buffer.id &&
            waiter.byte_index == atomics_byte_index(data, index) {
            atomics_waiter_update_status(id, AtomicsWaitStatus::Ok, Some(now))
            woken = woken + 1
          }
        None => ()
      }
    }
    list
  })
  Number(Double::from_int(woken))
}
