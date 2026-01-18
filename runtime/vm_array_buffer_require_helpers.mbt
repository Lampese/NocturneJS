///|
fn require_array_buffer(value : Value?) -> (ObjectValue, ArrayBufferData) raise {
  match value {
    Some(Object(obj)) =>
      match obj.array_buffer_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not an ArrayBuffer")
          (
            obj,
            ArrayBufferData::{
              bytes: [],
              detached: true,
              shared: false,
              max_byte_length: 0,
              resizable: false,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not an ArrayBuffer")
      (
        new_object_struct(None),
        ArrayBufferData::{
          bytes: [],
          detached: true,
          shared: false,
          max_byte_length: 0,
          resizable: false,
        },
      )
    }
  }
}

///|
fn require_non_shared_array_buffer(
  value : Value?,
) -> (ObjectValue, ArrayBufferData) raise {
  let (obj, data) = require_array_buffer(value)
  if data.shared {
    let _ = throw_type_error("not an ArrayBuffer")

  }
  (obj, data)
}

///|
fn require_shared_array_buffer(
  value : Value?,
) -> (ObjectValue, ArrayBufferData) raise {
  let (obj, data) = require_array_buffer(value)
  if !data.shared {
    let _ = throw_type_error("not a SharedArrayBuffer")

  }
  (obj, data)
}

///|
fn array_buffer_data_length(data : ArrayBufferData) -> Int {
  if data.detached {
    0
  } else {
    data.bytes.length()
  }
}

///|
fn typed_array_is_detached(data : TypedArrayData) -> Bool {
  match data.buffer.array_buffer_data {
    Some(buf) => buf.detached
    None => true
  }
}

///|
fn typed_array_is_oob(data : TypedArrayData) -> Bool {
  match data.buffer.array_buffer_data {
    Some(buf) => {
      if buf.detached {
        return true
      }
      let len = buf.bytes.length()
      if data.byte_offset > len {
        return true
      }
      if data.length_tracking {
        return false
      }
      data.byte_offset + data.length * data.bytes_per_element > len
    }
    None => true
  }
}

///|
fn typed_array_is_valid_integer_index(
  data : TypedArrayData,
  index : Double,
) -> Bool {
  if Double::is_nan(index) || Double::is_inf(index) {
    return false
  }
  if is_negative_zero(index) {
    return false
  }
  if Double::trunc(index) != index || index < 0.0 {
    return false
  }
  if typed_array_is_oob(data) {
    return false
  }
  let len = typed_array_effective_length(data)
  index < Double::from_int(len)
}

///|
fn typed_array_effective_length(data : TypedArrayData) -> Int {
  if typed_array_is_detached(data) {
    return 0
  }
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let available = buf.bytes.length() - data.byte_offset
      if available <= 0 {
        return 0
      }
      let max_len = available / data.bytes_per_element
      if data.length_tracking {
        max_len
      } else if data.length <= max_len {
        data.length
      } else {
        0
      }
    }
    None => 0
  }
}

///|
fn typed_array_effective_byte_length(data : TypedArrayData) -> Int {
  if typed_array_is_detached(data) {
    0
  } else {
    typed_array_effective_length(data) * data.bytes_per_element
  }
}

///|
fn dataview_is_detached(data : DataViewData) -> Bool {
  match data.buffer.array_buffer_data {
    Some(buf) => buf.detached
    None => true
  }
}

///|
fn dataview_is_oob(data : DataViewData) -> Bool {
  match data.buffer.array_buffer_data {
    Some(buf) => {
      if buf.detached {
        return true
      }
      let len = buf.bytes.length()
      if data.byte_offset > len {
        return true
      }
      if data.length_tracking {
        return false
      }
      data.byte_offset + data.byte_length > len
    }
    None => true
  }
}

///|
fn dataview_effective_length(data : DataViewData) -> Int {
  if dataview_is_detached(data) {
    return 0
  }
  match data.buffer.array_buffer_data {
    Some(buf) => {
      let available = buf.bytes.length() - data.byte_offset
      if available <= 0 {
        return 0
      }
      if data.length_tracking {
        available
      } else {
        data.byte_length
      }
    }
    None => 0
  }
}

///|
fn require_dataview_data(value : Value?) -> (ObjectValue, DataViewData) raise {
  match value {
    Some(Object(obj)) =>
      match obj.dataview_data {
        Some(data) => (obj, data)
        None => {
          let _ = throw_type_error("not a DataView")
          (
            obj,
            DataViewData::{
              buffer: obj,
              byte_offset: 0,
              byte_length: 0,
              length_tracking: false,
            },
          )
        }
      }
    _ => {
      let _ = throw_type_error("not a DataView")
      let fallback = new_object_struct(None)
      (
        fallback,
        DataViewData::{
          buffer: fallback,
          byte_offset: 0,
          byte_length: 0,
          length_tracking: false,
        },
      )
    }
  }
}
