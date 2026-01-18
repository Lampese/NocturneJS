///|
fn call_builtin_file(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::FilePuts => {
      let fd = require_file_fd(builtin.this_value)
      let text = if args.is_empty() { "" } else { to_string_value(args[0]) }
      let sep = if args.length() > 1 { to_string_value(args[1]) } else { "" }
      let bytes = bytes_from_string(text + sep)
      let wrote = fd_write_bytes(fd, bytes, 0, bytes.length())
      Number(Double::from_int(wrote))
    }
    BuiltinFunction::FilePutByte => {
      let fd = require_file_fd(builtin.this_value)
      let byte_value = if args.is_empty() { Number(0.0) } else { args[0] }
      let bytes = [byte_from_value(byte_value)]
      let wrote = fd_write_bytes(fd, bytes, 0, bytes.length())
      Number(Double::from_int(wrote))
    }
    BuiltinFunction::FileSeek => {
      let fd = require_file_fd(builtin.this_value)
      let offset_value = if args.is_empty() { Number(0.0) } else { args[0] }
      let offset = to_int32(to_number(offset_value))
      let whence = if args.length() > 1 {
        to_int32(to_number(args[1]))
      } else {
        seek_set
      }
      match fd_seek(fd, offset, whence) {
        Some(pos) => Number(Double::from_int(pos))
        None => Number(-1.0)
      }
    }
    BuiltinFunction::FileTell => {
      let fd = require_file_fd(builtin.this_value)
      match fd_tell(fd) {
        Some(pos) => Number(Double::from_int(pos))
        None => Number(-1.0)
      }
    }
    BuiltinFunction::FileRead => {
      let fd = require_file_fd(builtin.this_value)
      if args.is_empty() {
        return Number(0.0)
      }
      let buffer = args[0]
      let mut offset = 0
      if args.length() > 1 {
        offset = to_int32(to_number(args[1]))
      }
      let buf_len = array_buffer_byte_length(buffer)
      let len = if args.length() > 2 {
        to_int32(to_number(args[2]))
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
    BuiltinFunction::FileReadAsString => {
      let fd = require_file_fd(builtin.this_value)
      match fd_read_bytes(fd, 2147483647) {
        Some(bytes) => String(string_from_bytes(bytes))
        None => String("")
      }
    }
    BuiltinFunction::FileGetByte => {
      let fd = require_file_fd(builtin.this_value)
      match fd_read_bytes(fd, 1) {
        Some(bytes) =>
          if bytes.is_empty() {
            Number(-1.0)
          } else {
            Number(Double::from_int(bytes[0].to_int()))
          }
        None => Number(-1.0)
      }
    }
    BuiltinFunction::FileGetline => {
      let fd = require_file_fd(builtin.this_value)
      match fd_read_line(fd) {
        Some(line) => String(line)
        None => Null
      }
    }
    BuiltinFunction::FileEof => {
      let fd = require_file_fd(builtin.this_value)
      match fd_eof(fd) {
        Some(done) => Bool(done)
        None => Bool(true)
      }
    }
    BuiltinFunction::FileClose => {
      let fd = require_file_fd(builtin.this_value)
      Number(Double::from_int(fd_close(fd)))
    }
    _ => throw_type_error("invalid builtin")
  }
}
