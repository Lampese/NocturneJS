///|
pub struct Host {} derive(Show)

///|
pub fn Host::new() -> Host {
  Host::{  }
}

///|
pub fn Host::install(self : Host, vm : @runtime.Vm) -> @runtime.Vm {
  let _ = self
  vm
}
