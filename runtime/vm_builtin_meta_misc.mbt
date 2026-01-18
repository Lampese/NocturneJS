///|
fn builtin_name_misc(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::FunctionCall => "call"
    BuiltinFunction::FunctionApply => "apply"
    BuiltinFunction::FunctionBind => "bind"
    BuiltinFunction::FunctionToString => "toString"
    BuiltinFunction::FunctionProtoLineNumber => "get lineNumber"
    BuiltinFunction::FunctionProtoColumnNumber => "get columnNumber"
    BuiltinFunction::FunctionSymbolHasInstance => "[Symbol.hasInstance]"
    BuiltinFunction::FunctionConstructor => "Function"
    BuiltinFunction::Escape => "escape"
    BuiltinFunction::Unescape => "unescape"
    BuiltinFunction::SpeciesGetter => "get [Symbol.species]"
    BuiltinFunction::WorkerConstructor => "Worker"
    BuiltinFunction::WorkerPostMessage => "postMessage"
    BuiltinFunction::StdSprintf => "sprintf"
    BuiltinFunction::StdLoadScript => "loadScript"
    BuiltinFunction::StdTmpfile => "tmpfile"
    BuiltinFunction::StdOpen => "open"
    BuiltinFunction::StdLoadFile => "loadFile"
    BuiltinFunction::StdPopen => "popen"
    BuiltinFunction::StdParseExtJSON => "parseExtJSON"
    BuiltinFunction::StdFdopen => "fdopen"
    BuiltinFunction::StdGc => "gc"
    BuiltinFunction::Print => "print"
    _ => ""
  }
}

///|
fn builtin_length_misc(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::FunctionCall => 1
    BuiltinFunction::FunctionApply => 2
    BuiltinFunction::FunctionBind => 1
    BuiltinFunction::FunctionToString => 0
    BuiltinFunction::FunctionProtoLineNumber => 0
    BuiltinFunction::FunctionProtoColumnNumber => 0
    BuiltinFunction::FunctionSymbolHasInstance => 1
    BuiltinFunction::FunctionConstructor => 1
    BuiltinFunction::Escape => 1
    BuiltinFunction::Unescape => 1
    BuiltinFunction::SpeciesGetter => 0
    BuiltinFunction::WorkerConstructor => 1
    BuiltinFunction::WorkerPostMessage => 1
    BuiltinFunction::StdSprintf => 1
    BuiltinFunction::StdLoadScript => 1
    BuiltinFunction::StdTmpfile => 0
    BuiltinFunction::StdOpen => 2
    BuiltinFunction::StdLoadFile => 1
    BuiltinFunction::StdPopen => 2
    BuiltinFunction::StdParseExtJSON => 1
    BuiltinFunction::StdFdopen => 2
    BuiltinFunction::StdGc => 0
    BuiltinFunction::Print => 1
    _ => 0
  }
}
