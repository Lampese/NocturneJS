///|
priv struct GcMeta {
  mut ref_count : Int
  mut mark : Bool
  mut in_tmp : Bool
}

///|
struct RcMeta {
  mut ref_count : Int
}

///|
priv struct GcState {
  meta : Map[Int, GcMeta]
  weakrefs : Array[ObjectValue]
  weakmaps : Array[ObjectValue]
  weaksets : Array[ObjectValue]
  finregs : Array[ObjectValue]
}

///|
pub struct GcRegistry {
  envs : Map[Int, Env]
  values : Map[Int, Value]
}

///|
let gc_default_threshold : Int = 256 * 1024

///|
pub struct RuntimeState {
  gc_registry_ref : Ref[GcRegistry]
  gc_alloc_count_ref : Ref[Int]
  gc_threshold_ref : Ref[Int]
  gc_debug_ref : Ref[Bool]
  gc_value_root_ref : Ref[Array[Value]]
  gc_pending_ref : Ref[Bool]
  gc_rc_meta_ref : Ref[Map[Int, RcMeta]]
  print_log_ref : Ref[Array[String]]
  async_done_ref : Ref[Int]
  can_block_ref : Ref[Bool]
}

///|
pub impl Show for RuntimeState with output(self, logger) {
  let _ = self
  logger.write_string("RuntimeState")
}

///|
fn RuntimeState::new() -> RuntimeState {
  RuntimeState::{
    gc_registry_ref: Ref::new(GcRegistry::{
      envs: Map::new(),
      values: Map::new(),
    }),
    gc_alloc_count_ref: Ref::new(0),
    gc_threshold_ref: Ref::new(gc_default_threshold),
    gc_debug_ref: Ref::new(false),
    gc_value_root_ref: Ref::new([]),
    gc_pending_ref: Ref::new(false),
    gc_rc_meta_ref: Ref::new(Map::new()),
    print_log_ref: Ref::new([]),
    async_done_ref: Ref::new(0),
    can_block_ref: Ref::new(true),
  }
}

///|
let current_runtime_ref : Ref[RuntimeState?] = Ref::new(None)

///|
let fallback_runtime_state : RuntimeState = RuntimeState::new()

///|
fn current_runtime_state() -> RuntimeState {
  let mut value : RuntimeState? = None
  current_runtime_ref.update(fn(current) {
    value = current
    current
  })
  match value {
    Some(runtime) => runtime
    None => fallback_runtime_state
  }
}

///|
fn set_current_runtime_state(runtime : RuntimeState) -> RuntimeState? {
  let mut prev : RuntimeState? = None
  current_runtime_ref.update(fn(current) {
    prev = current
    Some(runtime)
  })
  prev
}

///|
fn restore_runtime_state(prev : RuntimeState?) -> Unit {
  current_runtime_ref.update(fn(_) { prev })
}

///|
fn gc_registry_ref() -> Ref[GcRegistry] {
  current_runtime_state().gc_registry_ref
}

///|
fn gc_alloc_count_ref() -> Ref[Int] {
  current_runtime_state().gc_alloc_count_ref
}

///|
fn gc_threshold_ref() -> Ref[Int] {
  current_runtime_state().gc_threshold_ref
}

///|
fn gc_debug_ref() -> Ref[Bool] {
  current_runtime_state().gc_debug_ref
}

///|
fn gc_value_root_ref() -> Ref[Array[Value]] {
  current_runtime_state().gc_value_root_ref
}

///|
fn gc_pending_ref() -> Ref[Bool] {
  current_runtime_state().gc_pending_ref
}

///|
fn gc_rc_meta_ref() -> Ref[Map[Int, RcMeta]] {
  current_runtime_state().gc_rc_meta_ref
}

///|
fn print_log_ref() -> Ref[Array[String]] {
  current_runtime_state().print_log_ref
}

///|
fn async_done_ref() -> Ref[Int] {
  current_runtime_state().async_done_ref
}

///|
fn can_block_ref() -> Ref[Bool] {
  current_runtime_state().can_block_ref
}

///|
fn new_gc_state() -> GcState {
  GcState::{
    meta: Map::new(),
    weakrefs: [],
    weakmaps: [],
    weaksets: [],
    finregs: [],
  }
}
