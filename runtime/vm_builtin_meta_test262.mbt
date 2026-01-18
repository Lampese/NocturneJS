///|
fn builtin_name_test262(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::Test262DetachArrayBuffer => "detachArrayBuffer"
    BuiltinFunction::Test262EvalScript => "evalScript"
    BuiltinFunction::Test262CodePointRange => "codePointRange"
    BuiltinFunction::Test262CreateRealm => "createRealm"
    BuiltinFunction::Test262Gc => "gc"
    BuiltinFunction::Test262IsHTMLDDA => "IsHTMLDDA"
    BuiltinFunction::Test262AgentStart => "start"
    BuiltinFunction::Test262AgentGetReport => "getReport"
    BuiltinFunction::Test262AgentBroadcast => "broadcast"
    BuiltinFunction::Test262AgentReport => "report"
    BuiltinFunction::Test262AgentLeaving => "leaving"
    BuiltinFunction::Test262AgentReceiveBroadcast => "receiveBroadcast"
    BuiltinFunction::Test262AgentSleep => "sleep"
    BuiltinFunction::Test262AgentMonotonicNow => "monotonicNow"
    _ => ""
  }
}

///|
fn builtin_length_test262(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::Test262DetachArrayBuffer => 1
    BuiltinFunction::Test262EvalScript => 1
    BuiltinFunction::Test262CodePointRange => 2
    BuiltinFunction::Test262CreateRealm => 0
    BuiltinFunction::Test262Gc => 0
    BuiltinFunction::Test262IsHTMLDDA => 0
    BuiltinFunction::Test262AgentStart => 1
    BuiltinFunction::Test262AgentGetReport => 0
    BuiltinFunction::Test262AgentBroadcast => 2
    BuiltinFunction::Test262AgentReport => 1
    BuiltinFunction::Test262AgentLeaving => 0
    BuiltinFunction::Test262AgentReceiveBroadcast => 1
    BuiltinFunction::Test262AgentSleep => 1
    BuiltinFunction::Test262AgentMonotonicNow => 0
    _ => 0
  }
}
