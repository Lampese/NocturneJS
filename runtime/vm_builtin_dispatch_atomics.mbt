///|
fn call_builtin_atomics(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::AtomicsAdd => atomics_op_value(args, AtomicsOpKind::Add)
    BuiltinFunction::AtomicsAnd => atomics_op_value(args, AtomicsOpKind::And)
    BuiltinFunction::AtomicsOr => atomics_op_value(args, AtomicsOpKind::Or)
    BuiltinFunction::AtomicsSub => atomics_op_value(args, AtomicsOpKind::Sub)
    BuiltinFunction::AtomicsXor => atomics_op_value(args, AtomicsOpKind::Xor)
    BuiltinFunction::AtomicsExchange =>
      atomics_op_value(args, AtomicsOpKind::Exchange)
    BuiltinFunction::AtomicsCompareExchange =>
      atomics_op_value(args, AtomicsOpKind::CompareExchange)
    BuiltinFunction::AtomicsLoad => atomics_op_value(args, AtomicsOpKind::Load)
    BuiltinFunction::AtomicsStore => atomics_store_value(args)
    BuiltinFunction::AtomicsIsLockFree => atomics_is_lock_free_value(args)
    BuiltinFunction::AtomicsPause => atomics_pause_value(args)
    BuiltinFunction::AtomicsWait => atomics_wait_value(args)
    BuiltinFunction::AtomicsNotify => atomics_notify_value(args)
    _ => throw_type_error("invalid builtin")
  }
}
