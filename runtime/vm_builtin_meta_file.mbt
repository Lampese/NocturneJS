///|
fn builtin_name_file(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::FilePuts => "puts"
    BuiltinFunction::FilePutByte => "putByte"
    BuiltinFunction::FileSeek => "seek"
    BuiltinFunction::FileTell => "tell"
    BuiltinFunction::FileRead => "read"
    BuiltinFunction::FileReadAsString => "readAsString"
    BuiltinFunction::FileGetByte => "getByte"
    BuiltinFunction::FileGetline => "getline"
    BuiltinFunction::FileEof => "eof"
    BuiltinFunction::FileClose => "close"
    _ => ""
  }
}

///|
fn builtin_length_file(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::FilePuts => 1
    BuiltinFunction::FilePutByte => 1
    BuiltinFunction::FileSeek => 2
    BuiltinFunction::FileTell => 0
    BuiltinFunction::FileRead => 3
    BuiltinFunction::FileReadAsString => 0
    BuiltinFunction::FileGetByte => 0
    BuiltinFunction::FileGetline => 0
    BuiltinFunction::FileEof => 0
    BuiltinFunction::FileClose => 0
    _ => 0
  }
}
