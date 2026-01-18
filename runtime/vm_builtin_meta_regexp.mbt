///|
fn builtin_name_regexp(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::RegExpConstructor => "RegExp"
    BuiltinFunction::RegExpExec => "exec"
    BuiltinFunction::RegExpTest => "test"
    BuiltinFunction::RegExpToString => "toString"
    BuiltinFunction::RegExpCompile => "compile"
    BuiltinFunction::RegExpEscape => "escape"
    BuiltinFunction::RegExpGetFlags => "get flags"
    BuiltinFunction::RegExpGetSource => "get source"
    BuiltinFunction::RegExpGetGlobal => "get global"
    BuiltinFunction::RegExpGetIgnoreCase => "get ignoreCase"
    BuiltinFunction::RegExpGetMultiline => "get multiline"
    BuiltinFunction::RegExpGetDotAll => "get dotAll"
    BuiltinFunction::RegExpGetUnicode => "get unicode"
    BuiltinFunction::RegExpGetUnicodeSets => "get unicodeSets"
    BuiltinFunction::RegExpGetSticky => "get sticky"
    BuiltinFunction::RegExpGetHasIndices => "get hasIndices"
    BuiltinFunction::RegExpSymbolMatch => "[Symbol.match]"
    BuiltinFunction::RegExpSymbolMatchAll => "[Symbol.matchAll]"
    BuiltinFunction::RegExpSymbolReplace => "[Symbol.replace]"
    BuiltinFunction::RegExpSymbolSearch => "[Symbol.search]"
    BuiltinFunction::RegExpSymbolSplit => "[Symbol.split]"
    BuiltinFunction::RegExpStringIteratorNext => "next"
    _ => ""
  }
}

///|
fn builtin_length_regexp(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::RegExpConstructor => 2
    BuiltinFunction::RegExpExec => 1
    BuiltinFunction::RegExpTest => 1
    BuiltinFunction::RegExpToString => 0
    BuiltinFunction::RegExpCompile => 2
    BuiltinFunction::RegExpEscape => 1
    BuiltinFunction::RegExpGetFlags => 0
    BuiltinFunction::RegExpGetSource => 0
    BuiltinFunction::RegExpGetGlobal => 0
    BuiltinFunction::RegExpGetIgnoreCase => 0
    BuiltinFunction::RegExpGetMultiline => 0
    BuiltinFunction::RegExpGetDotAll => 0
    BuiltinFunction::RegExpGetUnicode => 0
    BuiltinFunction::RegExpGetUnicodeSets => 0
    BuiltinFunction::RegExpGetSticky => 0
    BuiltinFunction::RegExpGetHasIndices => 0
    BuiltinFunction::RegExpSymbolMatch => 1
    BuiltinFunction::RegExpSymbolMatchAll => 1
    BuiltinFunction::RegExpSymbolReplace => 2
    BuiltinFunction::RegExpSymbolSearch => 1
    BuiltinFunction::RegExpSymbolSplit => 2
    BuiltinFunction::RegExpStringIteratorNext => 0
    _ => 0
  }
}
