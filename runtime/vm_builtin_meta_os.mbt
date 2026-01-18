///|
fn builtin_name_os(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::OsSetTimeout => "setTimeout"
    BuiltinFunction::OsClearTimeout => "clearTimeout"
    BuiltinFunction::OsOpen => "open"
    BuiltinFunction::OsClose => "close"
    BuiltinFunction::OsRead => "read"
    BuiltinFunction::OsWrite => "write"
    BuiltinFunction::OsSeek => "seek"
    BuiltinFunction::OsReaddir => "readdir"
    BuiltinFunction::OsStat => "stat"
    BuiltinFunction::OsLstat => "lstat"
    BuiltinFunction::OsReadlink => "readlink"
    BuiltinFunction::OsSymlink => "symlink"
    BuiltinFunction::OsMkdir => "mkdir"
    BuiltinFunction::OsRemove => "remove"
    BuiltinFunction::OsUtimes => "utimes"
    BuiltinFunction::OsGetcwd => "getcwd"
    BuiltinFunction::OsRealpath => "realpath"
    BuiltinFunction::OsIsatty => "isatty"
    BuiltinFunction::OsExec => "exec"
    BuiltinFunction::OsPipe => "pipe"
    BuiltinFunction::OsWaitpid => "waitpid"
    BuiltinFunction::OsKill => "kill"
    _ => ""
  }
}

///|
fn builtin_length_os(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::OsSetTimeout => 2
    BuiltinFunction::OsClearTimeout => 1
    BuiltinFunction::OsOpen => 2
    BuiltinFunction::OsClose => 1
    BuiltinFunction::OsRead => 4
    BuiltinFunction::OsWrite => 4
    BuiltinFunction::OsSeek => 3
    BuiltinFunction::OsReaddir => 1
    BuiltinFunction::OsStat => 1
    BuiltinFunction::OsLstat => 1
    BuiltinFunction::OsReadlink => 1
    BuiltinFunction::OsSymlink => 2
    BuiltinFunction::OsMkdir => 2
    BuiltinFunction::OsRemove => 1
    BuiltinFunction::OsUtimes => 3
    BuiltinFunction::OsGetcwd => 0
    BuiltinFunction::OsRealpath => 1
    BuiltinFunction::OsIsatty => 1
    BuiltinFunction::OsExec => 2
    BuiltinFunction::OsPipe => 0
    BuiltinFunction::OsWaitpid => 2
    BuiltinFunction::OsKill => 2
    _ => 0
  }
}
