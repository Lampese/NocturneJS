///|
fn builtin_name_atomics(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::AtomicsAdd => "add"
    BuiltinFunction::AtomicsAnd => "and"
    BuiltinFunction::AtomicsOr => "or"
    BuiltinFunction::AtomicsSub => "sub"
    BuiltinFunction::AtomicsXor => "xor"
    BuiltinFunction::AtomicsExchange => "exchange"
    BuiltinFunction::AtomicsCompareExchange => "compareExchange"
    BuiltinFunction::AtomicsLoad => "load"
    BuiltinFunction::AtomicsStore => "store"
    BuiltinFunction::AtomicsIsLockFree => "isLockFree"
    BuiltinFunction::AtomicsPause => "pause"
    BuiltinFunction::AtomicsWait => "wait"
    BuiltinFunction::AtomicsNotify => "notify"
    _ => ""
  }
}

///|
fn builtin_length_atomics(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::AtomicsAdd => 3
    BuiltinFunction::AtomicsAnd => 3
    BuiltinFunction::AtomicsOr => 3
    BuiltinFunction::AtomicsSub => 3
    BuiltinFunction::AtomicsXor => 3
    BuiltinFunction::AtomicsExchange => 3
    BuiltinFunction::AtomicsCompareExchange => 4
    BuiltinFunction::AtomicsLoad => 2
    BuiltinFunction::AtomicsStore => 3
    BuiltinFunction::AtomicsIsLockFree => 1
    BuiltinFunction::AtomicsPause => 0
    BuiltinFunction::AtomicsWait => 4
    BuiltinFunction::AtomicsNotify => 3
    _ => 0
  }
}
