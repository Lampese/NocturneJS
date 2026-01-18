///|
fn call_builtin_arraybuffer(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::DataViewConstructor =>
      throw_type_error("DataView constructor requires 'new'")
    BuiltinFunction::ArrayBufferIsView => {
      let value = if args.is_empty() { Undefined } else { args[0] }
      match value {
        Array(arr) => Bool(arr.typed_array_data is Some(_))
        Object(obj) => Bool(obj.dataview_data is Some(_))
        _ => Bool(false)
      }
    }
    BuiltinFunction::DataViewGetBuffer => {
      let (_, data) = require_dataview_data(builtin.this_value)
      Object(data.buffer)
    }
    BuiltinFunction::DataViewGetByteLength => {
      let (_, data) = require_dataview_data(builtin.this_value)
      if dataview_is_oob(data) {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      Number(Double::from_int(dataview_effective_length(data)))
    }
    BuiltinFunction::DataViewGetByteOffset => {
      let (_, data) = require_dataview_data(builtin.this_value)
      if dataview_is_oob(data) {
        return throw_type_error("ArrayBuffer is detached or resized")
      }
      Number(Double::from_int(data.byte_offset))
    }
    BuiltinFunction::DataViewGetInt8 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Int8)
    }
    BuiltinFunction::DataViewGetUint8 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Uint8)
    }
    BuiltinFunction::DataViewGetInt16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Int16)
    }
    BuiltinFunction::DataViewGetUint16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Uint16)
    }
    BuiltinFunction::DataViewGetInt32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Int32)
    }
    BuiltinFunction::DataViewGetUint32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Uint32)
    }
    BuiltinFunction::DataViewGetBigInt64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::BigInt64)
    }
    BuiltinFunction::DataViewGetBigUint64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::BigUint64)
    }
    BuiltinFunction::DataViewGetFloat16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Float16)
    }
    BuiltinFunction::DataViewGetFloat32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Float32)
    }
    BuiltinFunction::DataViewGetFloat64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_get_value(data, args, DataViewValueKind::Float64)
    }
    BuiltinFunction::DataViewSetInt8 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Int8)
    }
    BuiltinFunction::DataViewSetUint8 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Uint8)
    }
    BuiltinFunction::DataViewSetInt16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Int16)
    }
    BuiltinFunction::DataViewSetUint16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Uint16)
    }
    BuiltinFunction::DataViewSetInt32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Int32)
    }
    BuiltinFunction::DataViewSetUint32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Uint32)
    }
    BuiltinFunction::DataViewSetBigInt64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::BigInt64)
    }
    BuiltinFunction::DataViewSetBigUint64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::BigUint64)
    }
    BuiltinFunction::DataViewSetFloat16 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Float16)
    }
    BuiltinFunction::DataViewSetFloat32 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Float32)
    }
    BuiltinFunction::DataViewSetFloat64 => {
      let (_, data) = require_dataview_data(builtin.this_value)
      dataview_set_value(data, args, DataViewValueKind::Float64)
    }
    BuiltinFunction::ArrayBufferByteLength =>
      array_buffer_byte_length_value(builtin.this_value)
    BuiltinFunction::ArrayBufferMaxByteLength =>
      array_buffer_max_byte_length_value(builtin.this_value)
    BuiltinFunction::ArrayBufferResizable =>
      array_buffer_resizable_value(builtin.this_value)
    BuiltinFunction::ArrayBufferDetached =>
      array_buffer_detached_value(builtin.this_value)
    BuiltinFunction::SharedArrayBufferByteLength =>
      shared_array_buffer_byte_length_value(builtin.this_value)
    BuiltinFunction::SharedArrayBufferMaxByteLength =>
      shared_array_buffer_max_byte_length_value(builtin.this_value)
    BuiltinFunction::SharedArrayBufferGrowable =>
      shared_array_buffer_growable_value(builtin.this_value)
    BuiltinFunction::ArrayBufferSlice =>
      array_buffer_slice_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        false,
      )
    BuiltinFunction::SharedArrayBufferSlice =>
      array_buffer_slice_value(
        match builtin.this_value {
          Some(value) => value
          None => Undefined
        },
        args,
        true,
      )
    BuiltinFunction::ArrayBufferTransfer =>
      array_buffer_transfer_value(builtin.this_value, args, false)
    BuiltinFunction::ArrayBufferTransferToFixedLength =>
      array_buffer_transfer_value(builtin.this_value, args, true)
    BuiltinFunction::ArrayBufferResize => {
      let (obj, data) = require_non_shared_array_buffer(builtin.this_value)
      let size_value = if args.is_empty() { Number(0.0) } else { args[0] }
      let new_len64 = to_index_int64(size_value)
      let max_int = 2147483647
      if new_len64 > Int64::from_int(max_int) {
        return throw_range_error("invalid array buffer length")
      }
      let new_len = Int64::to_int(new_len64)
      array_buffer_resize_value(obj, data, new_len)
    }
    BuiltinFunction::SharedArrayBufferGrow => {
      let (obj, data) = require_shared_array_buffer(builtin.this_value)
      let size_value = if args.is_empty() { Number(0.0) } else { args[0] }
      let new_len64 = to_index_int64(size_value)
      let max_int = 2147483647
      if new_len64 > Int64::from_int(max_int) {
        return throw_range_error("invalid array buffer length")
      }
      let new_len = Int64::to_int(new_len64)
      shared_array_buffer_grow_value(obj, data, new_len)
    }
    BuiltinFunction::ArrayBufferConstructor =>
      throw_type_error("constructor requires 'new'")
    BuiltinFunction::SharedArrayBufferConstructor =>
      throw_type_error("constructor requires 'new'")
    _ => throw_type_error("invalid builtin")
  }
}
