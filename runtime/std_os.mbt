///|
priv enum FdKind {
  File
  PipeRead
  PipeWrite
}

///|
priv struct FileState {
  buffer : Array[Byte]
  mut pos : Int
  mut closed : Bool
  path : String?
  readable : Bool
  writable : Bool
}

///|
priv struct PipeState {
  buffer : Array[Byte]
  mut read_pos : Int
  mut closed_read : Bool
  mut closed_write : Bool
}

///|
priv struct FdHandle {
  kind : FdKind
  file : FileState?
  pipe : PipeState?
}

///|
priv struct ProcessInfo {
  mut status : Int?
  mut running : Bool
  stdout_fd : Int?
}

///|
let fd_table_ref : Ref[Map[Int, FdHandle]] = Ref::new({})

///|
let next_fd_ref : Ref[Int] = Ref::new(3)

///|
let symlink_table_ref : Ref[Map[String, String]] = Ref::new({})

///|
let mtime_table_ref : Ref[Map[String, Int64]] = Ref::new({})

///|
let process_table_ref : Ref[Map[Int, ProcessInfo]] = Ref::new({})

///|
let next_pid_ref : Ref[Int] = Ref::new(1)

///|
let os_flag_rdonly : Int = 0

///|
let os_flag_wronly : Int = 1

///|
let os_flag_rdwr : Int = 2

///|
let os_flag_creat : Int = 64

///|
let os_flag_trunc : Int = 512

///|
let os_mode_ifmt : Int = 61440

///|
let os_mode_ifreg : Int = 32768

///|
let os_mode_iflnk : Int = 40960

///|
let os_sigterm : Int = 15

///|
let seek_set : Int = 0

///|
let seek_cur : Int = 1

///|
let seek_end : Int = 2

///|
fn alloc_fd(handle : FdHandle) -> Int {
  let mut fd = 0
  next_fd_ref.update(fn(current) {
    fd = current
    current + 1
  })
  fd_table_ref.update(fn(table) {
    table.set(fd, handle)
    table
  })
  fd
}

///|
fn get_fd_handle(fd : Int) -> FdHandle? {
  let mut handle : FdHandle? = None
  fd_table_ref.update(fn(table) {
    handle = table.get(fd)
    table
  })
  handle
}

///|
fn set_fd_handle(fd : Int, handle : FdHandle?) -> Unit {
  fd_table_ref.update(fn(table) {
    match handle {
      Some(value) => table.set(fd, value)
      None => table.remove(fd)
    }
    table
  })
}

///|
fn bytes_from_string(text : String) -> Array[Byte] {
  @encoding/utf8.encode(text).to_array()
}

///|
fn string_from_bytes(bytes : Array[Byte]) -> String {
  let data = Bytes::from_array(bytes[:])
  @encoding/utf8.decode(data[:]) catch {
    _ => ""
  }
}

///|
fn open_file_handle(
  path : String,
  readable : Bool,
  writable : Bool,
  truncate : Bool,
  create : Bool,
) -> Int? {
  if !create && !@fs.path_exists(path) {
    return None
  }
  let mut buffer : Array[Byte] = []
  if !truncate && @fs.path_exists(path) {
    try {
      let bytes = @fs.read_file_to_bytes(path)
      buffer = bytes.to_array()
    } catch {
      _ => buffer = []
    }
  }
  let file = FileState::{
    buffer,
    pos: 0,
    closed: false,
    path: Some(path),
    readable,
    writable,
  }
  Some(alloc_fd(FdHandle::{ kind: FdKind::File, file: Some(file), pipe: None }))
}

///|
fn tmp_file_handle() -> Int {
  let file = FileState::{
    buffer: [],
    pos: 0,
    closed: false,
    path: None,
    readable: true,
    writable: true,
  }
  alloc_fd(FdHandle::{ kind: FdKind::File, file: Some(file), pipe: None })
}

///|
fn pipe_handle_pair() -> (Int, Int) {
  let state = PipeState::{
    buffer: [],
    read_pos: 0,
    closed_read: false,
    closed_write: false,
  }
  let read_fd = alloc_fd(FdHandle::{
    kind: FdKind::PipeRead,
    file: None,
    pipe: Some(state),
  })
  let write_fd = alloc_fd(FdHandle::{
    kind: FdKind::PipeWrite,
    file: None,
    pipe: Some(state),
  })
  (read_fd, write_fd)
}

///|
fn file_read_bytes(file : FileState, len : Int) -> Array[Byte] {
  if !file.readable || file.closed {
    return []
  }
  let available = file.buffer.length() - file.pos
  if available <= 0 {
    return []
  }
  let count = Int::min(len, available)
  let out : Array[Byte] = []
  let mut i = 0
  while i < count {
    out.push(file.buffer[file.pos + i])
    i = i + 1
  }
  file.pos = file.pos + count
  out
}

///|
fn ensure_buffer_length(buffer : Array[Byte], len : Int) -> Unit {
  while buffer.length() < len {
    buffer.push(0)
  }
}

///|
fn file_write_bytes(
  file : FileState,
  bytes : Array[Byte],
  offset : Int,
  len : Int,
) -> Int {
  if !file.writable || file.closed {
    return -1
  }
  if len <= 0 {
    return 0
  }
  let target_len = file.pos + len
  ensure_buffer_length(file.buffer, target_len)
  let mut i = 0
  while i < len {
    file.buffer[file.pos + i] = bytes[offset + i]
    i = i + 1
  }
  file.pos = file.pos + len
  len
}

///|
fn file_seek(file : FileState, offset : Int, whence : Int) -> Int {
  let mut pos = file.pos
  if whence == seek_set {
    pos = offset
  } else if whence == seek_cur {
    pos = pos + offset
  } else if whence == seek_end {
    pos = file.buffer.length() + offset
  }
  if pos < 0 {
    pos = 0
  }
  file.pos = pos
  pos
}

///|
fn file_read_line(file : FileState) -> String? {
  if !file.readable || file.closed {
    return None
  }
  let len = file.buffer.length()
  if file.pos >= len {
    return None
  }
  let mut end = file.pos
  while end < len {
    if file.buffer[end] == b'\n' {
      break
    }
    end = end + 1
  }
  let slice : Array[Byte] = []
  let mut i = file.pos
  while i < end {
    slice.push(file.buffer[i])
    i = i + 1
  }
  file.pos = if end < len && file.buffer[end] == b'\n' { end + 1 } else { end }
  Some(string_from_bytes(slice))
}

///|
fn pipe_read_bytes(pipe : PipeState, len : Int) -> Array[Byte] {
  let _ = pipe.closed_read
  let available = pipe.buffer.length() - pipe.read_pos
  if available <= 0 {
    return []
  }
  let count = Int::min(len, available)
  let out : Array[Byte] = []
  let mut i = 0
  while i < count {
    out.push(pipe.buffer[pipe.read_pos + i])
    i = i + 1
  }
  pipe.read_pos = pipe.read_pos + count
  out
}

///|
fn pipe_write_bytes(
  pipe : PipeState,
  bytes : Array[Byte],
  offset : Int,
  len : Int,
) -> Int {
  if pipe.closed_write {
    return -1
  }
  let mut i = 0
  while i < len {
    pipe.buffer.push(bytes[offset + i])
    i = i + 1
  }
  len
}

///|
fn pipe_read_line(pipe : PipeState) -> String? {
  let len = pipe.buffer.length()
  if pipe.read_pos >= len {
    return None
  }
  let mut end = pipe.read_pos
  while end < len {
    if pipe.buffer[end] == b'\n' {
      break
    }
    end = end + 1
  }
  let slice : Array[Byte] = []
  let mut i = pipe.read_pos
  while i < end {
    slice.push(pipe.buffer[i])
    i = i + 1
  }
  pipe.read_pos = if end < len && pipe.buffer[end] == b'\n' {
    end + 1
  } else {
    end
  }
  Some(string_from_bytes(slice))
}

///|
fn fd_read_bytes(fd : Int, len : Int) -> Array[Byte]? {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => Some(file_read_bytes(file, len))
            None => None
          }
        FdKind::PipeRead =>
          match handle.pipe {
            Some(pipe) => Some(pipe_read_bytes(pipe, len))
            None => None
          }
        FdKind::PipeWrite => Some([])
      }
    None => None
  }
}

///|
fn fd_write_bytes(
  fd : Int,
  bytes : Array[Byte],
  offset : Int,
  len : Int,
) -> Int {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => file_write_bytes(file, bytes, offset, len)
            None => -1
          }
        FdKind::PipeWrite =>
          match handle.pipe {
            Some(pipe) => pipe_write_bytes(pipe, bytes, offset, len)
            None => -1
          }
        FdKind::PipeRead => -1
      }
    None => -1
  }
}

///|
fn fd_seek(fd : Int, offset : Int, whence : Int) -> Int? {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => Some(file_seek(file, offset, whence))
            None => None
          }
        _ => None
      }
    None => None
  }
}

///|
fn fd_tell(fd : Int) -> Int? {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => Some(file.pos)
            None => None
          }
        FdKind::PipeRead =>
          match handle.pipe {
            Some(pipe) => Some(pipe.read_pos)
            None => None
          }
        FdKind::PipeWrite => Some(0)
      }
    None => None
  }
}

///|
fn fd_read_line(fd : Int) -> String? {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => file_read_line(file)
            None => None
          }
        FdKind::PipeRead =>
          match handle.pipe {
            Some(pipe) => pipe_read_line(pipe)
            None => None
          }
        FdKind::PipeWrite => None
      }
    None => None
  }
}

///|
fn fd_eof(fd : Int) -> Bool? {
  match get_fd_handle(fd) {
    Some(handle) =>
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => Some(file.pos >= file.buffer.length())
            None => None
          }
        FdKind::PipeRead =>
          match handle.pipe {
            Some(pipe) =>
              Some(pipe.read_pos >= pipe.buffer.length() && pipe.closed_write)
            None => None
          }
        FdKind::PipeWrite => Some(false)
      }
    None => None
  }
}

///|
fn fd_close(fd : Int) -> Int {
  match get_fd_handle(fd) {
    Some(handle) => {
      match handle.kind {
        FdKind::File =>
          match handle.file {
            Some(file) => {
              if file.closed {
                set_fd_handle(fd, None)
                return 0
              }
              file.closed = true
              match file.path {
                Some(path) =>
                  if file.writable {
                    let bytes = Bytes::from_array(file.buffer[:])
                    @fs.write_bytes_to_file(path, bytes) catch {
                      _ => {
                        set_fd_handle(fd, None)
                        return -1
                      }
                    }
                  }
                None => ()
              }
            }
            None => ()
          }
        FdKind::PipeRead =>
          match handle.pipe {
            Some(pipe) => pipe.closed_read = true
            None => ()
          }
        FdKind::PipeWrite =>
          match handle.pipe {
            Some(pipe) => pipe.closed_write = true
            None => ()
          }
      }
      set_fd_handle(fd, None)
      0
    }
    None => -1
  }
}

///|
fn set_symlink(path : String, target : String) -> Unit {
  symlink_table_ref.update(fn(table) {
    table.set(path, target)
    table
  })
}

///|
fn get_symlink(path : String) -> String? {
  let mut value : String? = None
  symlink_table_ref.update(fn(table) {
    value = table.get(path)
    table
  })
  value
}

///|
fn remove_symlink(path : String) -> Unit {
  symlink_table_ref.update(fn(table) {
    table.remove(path)
    table
  })
}

///|
fn set_mtime(path : String, mtime : Int64) -> Unit {
  mtime_table_ref.update(fn(table) {
    table.set(path, mtime)
    table
  })
}

///|
fn get_mtime(path : String) -> Int64? {
  let mut value : Int64? = None
  mtime_table_ref.update(fn(table) {
    value = table.get(path)
    table
  })
  value
}

///|
fn alloc_pid(proc : ProcessInfo) -> Int {
  let mut pid = 0
  next_pid_ref.update(fn(current) {
    pid = current
    current + 1
  })
  process_table_ref.update(fn(table) {
    table.set(pid, proc)
    table
  })
  pid
}

///|
fn get_process(pid : Int) -> ProcessInfo? {
  let mut proc : ProcessInfo? = None
  process_table_ref.update(fn(table) {
    proc = table.get(pid)
    table
  })
  proc
}

///|
fn set_process(pid : Int, proc : ProcessInfo?) -> Unit {
  process_table_ref.update(fn(table) {
    match proc {
      Some(value) => table.set(pid, value)
      None => table.remove(pid)
    }
    table
  })
}

///|
fn spawn_process(status : Int?, running : Bool, stdout_fd : Int?) -> Int {
  let proc = ProcessInfo::{ status, running, stdout_fd }
  let _ = proc.running
  let _ = proc.stdout_fd
  alloc_pid(proc)
}
