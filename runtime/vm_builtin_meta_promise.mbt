///|
fn builtin_name_promise(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::PromiseConstructor => "Promise"
    BuiltinFunction::PromiseResolve => "resolve"
    BuiltinFunction::PromiseReject => "reject"
    BuiltinFunction::PromiseThen => "then"
    BuiltinFunction::PromiseCatch => "catch"
    BuiltinFunction::PromiseFinally => "finally"
    BuiltinFunction::PromiseAll => "all"
    BuiltinFunction::PromiseAllSettled => "allSettled"
    BuiltinFunction::PromiseAny => "any"
    BuiltinFunction::PromiseRace => "race"
    BuiltinFunction::PromiseTry => "try"
    BuiltinFunction::PromiseWithResolvers => "withResolvers"
    BuiltinFunction::PromiseReactionJob => ""
    BuiltinFunction::PromiseResolveFunction => ""
    BuiltinFunction::PromiseRejectFunction => ""
    BuiltinFunction::PromiseResolveThenableJob => ""
    BuiltinFunction::PromiseAllResolveElement => ""
    BuiltinFunction::PromiseThenFinally => ""
    BuiltinFunction::PromiseFinallyValueThunk => ""
    BuiltinFunction::PromiseFinallyThrower => ""
    BuiltinFunction::PromiseCapabilityExecutor => ""
    _ => ""
  }
}

///|
fn builtin_length_promise(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::PromiseConstructor => 1
    BuiltinFunction::PromiseResolve => 1
    BuiltinFunction::PromiseReject => 1
    BuiltinFunction::PromiseThen => 2
    BuiltinFunction::PromiseCatch => 1
    BuiltinFunction::PromiseFinally => 1
    BuiltinFunction::PromiseAll => 1
    BuiltinFunction::PromiseAllSettled => 1
    BuiltinFunction::PromiseAny => 1
    BuiltinFunction::PromiseRace => 1
    BuiltinFunction::PromiseTry => 1
    BuiltinFunction::PromiseWithResolvers => 0
    BuiltinFunction::PromiseReactionJob => 0
    BuiltinFunction::PromiseResolveFunction => 1
    BuiltinFunction::PromiseRejectFunction => 1
    BuiltinFunction::PromiseResolveThenableJob => 0
    BuiltinFunction::PromiseAllResolveElement => 1
    BuiltinFunction::PromiseThenFinally => 1
    BuiltinFunction::PromiseFinallyValueThunk => 0
    BuiltinFunction::PromiseFinallyThrower => 0
    BuiltinFunction::PromiseCapabilityExecutor => 2
    _ => 0
  }
}
