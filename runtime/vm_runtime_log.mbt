///|
fn record_print_line(line : String) -> Unit {
  print_log_ref().update(fn(log) {
    log.push(line)
    log
  })
  async_done_ref().update(fn(current) {
    if line == "Test262:AsyncTestComplete" {
      current + 1
    } else if line.has_prefix("Test262:AsyncTestFailure") {
      2
    } else {
      current
    }
  })
}

///|
pub fn take_print_log() -> Array[String] {
  let mut out : Array[String] = []
  print_log_ref().update(fn(log) {
    out = log
    []
  })
  out
}

///|
pub fn async_done_count() -> Int {
  let mut value = 0
  async_done_ref().update(fn(current) {
    value = current
    current
  })
  value
}

///|
pub fn Vm::async_done_count(self : Vm) -> Int {
  let mut value = 0
  self.runtime.async_done_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
pub fn Vm::take_print_log(self : Vm) -> Array[String] {
  let mut out : Array[String] = []
  self.runtime.print_log_ref.update(fn(log) {
    out = log
    []
  })
  out
}
