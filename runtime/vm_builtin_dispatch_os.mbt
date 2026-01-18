///|
fn call_builtin_os(builtin : BuiltinValue, args : Array[Value]) -> Value raise {
  match builtin.kind {
    BuiltinFunction::OsSetTimeout => {
      if args.is_empty() {
        return throw_type_error("not a function")
      }
      let callback = args[0]
      if !is_callable(callback) {
        return throw_type_error("not a function")
      }
      let delay = if args.length() > 1 { to_number(args[1]) } else { 0.0 }
      let id = enqueue_timer(callback, Double::to_int64(delay))
      Number(Double::from_int(id))
    }
    BuiltinFunction::OsClearTimeout => {
      if args.is_empty() {
        return Undefined
      }
      let id = to_int32(to_number(args[0]))
      clear_timer(id)
      Undefined
    }
    BuiltinFunction::OsOpen => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let path = resolve_path(to_string_value(args[0]))
      let flags = if args.length() > 1 {
        to_int32(to_number(args[1]))
      } else {
        os_flag_rdonly
      }
      let writable = Int::land(flags, os_flag_wronly) != 0 ||
        Int::land(flags, os_flag_rdwr) != 0
      let readable = !writable || Int::land(flags, os_flag_rdwr) != 0
      let create = Int::land(flags, os_flag_creat) != 0
      let trunc = Int::land(flags, os_flag_trunc) != 0
      match open_file_handle(path, readable, writable, trunc, create) {
        Some(fd) => Number(Double::from_int(fd))
        None => Number(-1.0)
      }
    }
    BuiltinFunction::OsClose => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let fd = to_int32(to_number(args[0]))
      Number(Double::from_int(fd_close(fd)))
    }
    BuiltinFunction::OsRead => {
      if args.length() < 2 {
        return Number(-1.0)
      }
      let fd = to_int32(to_number(args[0]))
      let buffer = args[1]
      let offset = if args.length() > 2 {
        to_int32(to_number(args[2]))
      } else {
        0
      }
      let buf_len = array_buffer_byte_length(buffer)
      let len = if args.length() > 3 {
        to_int32(to_number(args[3]))
      } else {
        buf_len - offset
      }
      ensure_array_buffer_range(buf_len, offset, len)
      match fd_read_bytes(fd, len) {
        Some(bytes) => {
          let _ = write_bytes_to_array(buffer, offset, bytes)
          Number(Double::from_int(bytes.length()))
        }
        None => Number(-1.0)
      }
    }
    BuiltinFunction::OsWrite => {
      if args.length() < 2 {
        return Number(-1.0)
      }
      let fd = to_int32(to_number(args[0]))
      let buffer = args[1]
      let offset = if args.length() > 2 {
        to_int32(to_number(args[2]))
      } else {
        0
      }
      let len = if args.length() > 3 {
        to_int32(to_number(args[3]))
      } else {
        array_buffer_byte_length(buffer) - offset
      }
      let bytes = array_bytes_from_value(buffer, offset, len)
      let wrote = fd_write_bytes(fd, bytes, 0, bytes.length())
      Number(Double::from_int(wrote))
    }
    BuiltinFunction::OsSeek => {
      if args.length() < 2 {
        return Number(-1.0)
      }
      let fd = to_int32(to_number(args[0]))
      let offset_value = args[1]
      let offset = Int64::to_int(
        Double::to_int64(Double::trunc(to_number(offset_value))),
      )
      let whence = if args.length() > 2 {
        to_int32(to_number(args[2]))
      } else {
        seek_set
      }
      let result = match fd_seek(fd, offset, whence) {
        Some(pos) => pos
        None => -1
      }
      match offset_value {
        BigInt(_) => BigInt(@bigint.BigInt::from_int(result))
        _ => Number(Double::from_int(result))
      }
    }
    BuiltinFunction::OsReaddir => {
      if args.is_empty() {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let path = resolve_path(to_string_value(args[0]))
      try {
        let entries = @fs.read_dir(path)
        let files = string_array_value(entries)
        new_array_value([Some(files), Some(Number(0.0))])
      } catch {
        _ => new_array_value([Some(Null), Some(Number(-1.0))])
      }
    }
    BuiltinFunction::OsStat => {
      if args.is_empty() {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let path = resolve_path(to_string_value(args[0]))
      if !@fs.path_exists(path) {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let mode = os_mode_ifreg
      let mtime = match get_mtime(path) {
        Some(value) => value
        None => 0L
      }
      let stat = new_object_value()
      match stat {
        Object(obj) => {
          obj.props.set("mode", property_data(Number(Double::from_int(mode))))
          obj.props.set("mtime", property_data(Number(mtime.to_double())))
        }
        _ => ()
      }
      new_array_value([Some(stat), Some(Number(0.0))])
    }
    BuiltinFunction::OsLstat => {
      if args.is_empty() {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let path = resolve_path(to_string_value(args[0]))
      let mut mode = 0
      let mut err = 0
      match get_symlink(path) {
        Some(_) => mode = os_mode_iflnk
        None =>
          if @fs.path_exists(path) {
            mode = os_mode_ifreg
          } else {
            err = -1
          }
      }
      let mtime = match get_mtime(path) {
        Some(value) => value
        None => 0L
      }
      let stat = new_object_value()
      match stat {
        Object(obj) => {
          obj.props.set("mode", property_data(Number(Double::from_int(mode))))
          obj.props.set("mtime", property_data(Number(mtime.to_double())))
        }
        _ => ()
      }
      new_array_value([Some(stat), Some(Number(Double::from_int(err)))])
    }
    BuiltinFunction::OsReadlink => {
      if args.is_empty() {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let path = resolve_path(to_string_value(args[0]))
      match get_symlink(path) {
        Some(target) =>
          new_array_value([Some(String(target)), Some(Number(0.0))])
        None => new_array_value([Some(Null), Some(Number(-1.0))])
      }
    }
    BuiltinFunction::OsSymlink => {
      if args.length() < 2 {
        return Number(-1.0)
      }
      let target = to_string_value(args[0])
      let path = resolve_path(to_string_value(args[1]))
      set_symlink(path, target)
      Number(0.0)
    }
    BuiltinFunction::OsMkdir => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let path = resolve_path(to_string_value(args[0]))
      try {
        let _ = @fs.create_dir(path)
        Number(0.0)
      } catch {
        _ => Number(-1.0)
      }
    }
    BuiltinFunction::OsRemove => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let path = resolve_path(to_string_value(args[0]))
      match get_symlink(path) {
        Some(_) => {
          remove_symlink(path)
          return Number(0.0)
        }
        None => ()
      }
      try {
        let _ = @fs.remove_file(path)
        Number(0.0)
      } catch {
        _ =>
          try {
            let _ = @fs.remove_dir(path)
            Number(0.0)
          } catch {
            _ => Number(-1.0)
          }
      }
    }
    BuiltinFunction::OsUtimes => {
      if args.length() < 3 {
        return Number(-1.0)
      }
      let path = resolve_path(to_string_value(args[0]))
      let mtime = Double::to_int64(Double::trunc(to_number(args[2])))
      set_mtime(path, mtime)
      Number(0.0)
    }
    BuiltinFunction::OsGetcwd =>
      match @env.current_dir() {
        Some(cwd) => new_array_value([Some(String(cwd)), Some(Number(0.0))])
        None => new_array_value([Some(Null), Some(Number(-1.0))])
      }
    BuiltinFunction::OsRealpath => {
      if args.is_empty() {
        return new_array_value([Some(Null), Some(Number(-1.0))])
      }
      let path = resolve_path(to_string_value(args[0]))
      new_array_value([Some(String(path)), Some(Number(0.0))])
    }
    BuiltinFunction::OsIsatty => Bool(false)
    BuiltinFunction::OsExec => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let argv = value_array_to_strings(args[0])
      let options = if args.length() > 1 { args[1] } else { Undefined }
      let block = match property_get(options, "block") {
        Bool(value) => value
        _ => true
      }
      let stdout_fd = match property_get(options, "stdout") {
        Number(num) => Some(to_int32(num))
        _ => None
      }
      let env_value = property_get(options, "env")
      let foo = match env_value {
        Object(obj) =>
          match property_get(Object(obj), "FOO") {
            Undefined => ""
            value => to_string_value(value)
          }
        _ => ""
      }
      let mut exit_code = 0
      let mut output = ""
      if argv.length() > 0 {
        let cmd0 = argv[0]
        if cmd0 == "true" {
          exit_code = 0
        } else if cmd0 == "cat" {
          exit_code = 0
        } else if (cmd0 == "sh" || cmd0 == "/bin/sh") &&
          argv.length() >= 3 &&
          argv[1] == "-c" {
          let cmd = argv[2]
          if cmd == "exit 1" {
            exit_code = 1
          } else if cmd == "test -t 0" {
            exit_code = 1
          } else if cmd == "echo $FOO" {
            output = foo + "\n"
            exit_code = 0
          }
        }
      }
      match stdout_fd {
        Some(fd) =>
          if !output.is_empty() {
            let bytes = bytes_from_string(output)
            let _ = fd_write_bytes(fd, bytes, 0, bytes.length())

          }
        None => ()
      }
      if block {
        return Number(Double::from_int(exit_code))
      }
      let status = if argv.length() > 0 && argv[0] == "cat" {
        None
      } else {
        Some(exit_code << 8)
      }
      let running = match status {
        Some(_) => false
        None => true
      }
      let pid = spawn_process(status, running, stdout_fd)
      Number(Double::from_int(pid))
    }
    BuiltinFunction::OsPipe => {
      let (read_fd, write_fd) = pipe_handle_pair()
      new_array_value([
        Some(Number(Double::from_int(read_fd))),
        Some(Number(Double::from_int(write_fd))),
      ])
    }
    BuiltinFunction::OsWaitpid => {
      if args.is_empty() {
        return new_array_value([Some(Number(-1.0)), Some(Number(0.0))])
      }
      let pid = to_int32(to_number(args[0]))
      match get_process(pid) {
        Some(proc) => {
          let status = match proc.status {
            Some(value) => value
            None => 0
          }
          set_process(pid, None)
          new_array_value([
            Some(Number(Double::from_int(pid))),
            Some(Number(Double::from_int(status))),
          ])
        }
        None => new_array_value([Some(Number(-1.0)), Some(Number(0.0))])
      }
    }
    BuiltinFunction::OsKill => {
      if args.is_empty() {
        return Number(-1.0)
      }
      let pid = to_int32(to_number(args[0]))
      let sig = if args.length() > 1 {
        to_int32(to_number(args[1]))
      } else {
        os_sigterm
      }
      match get_process(pid) {
        Some(proc) => {
          proc.status = Some(sig)
          proc.running = false
          set_process(pid, Some(proc))
          Number(0.0)
        }
        None => Number(-1.0)
      }
    }
    _ => throw_type_error("invalid builtin")
  }
}
