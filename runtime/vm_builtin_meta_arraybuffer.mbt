///|
fn builtin_name_arraybuffer(func : BuiltinFunction) -> String {
  match func {
    BuiltinFunction::ArrayBufferIsView => "isView"
    BuiltinFunction::ArrayBufferByteLength => "get byteLength"
    BuiltinFunction::ArrayBufferMaxByteLength => "get maxByteLength"
    BuiltinFunction::ArrayBufferResizable => "get resizable"
    BuiltinFunction::ArrayBufferDetached => "get detached"
    BuiltinFunction::ArrayBufferConstructor => "ArrayBuffer"
    BuiltinFunction::SharedArrayBufferConstructor => "SharedArrayBuffer"
    BuiltinFunction::ArrayBufferSlice => "slice"
    BuiltinFunction::ArrayBufferTransfer => "transfer"
    BuiltinFunction::ArrayBufferTransferToFixedLength => "transferToFixedLength"
    BuiltinFunction::ArrayBufferResize => "resize"
    BuiltinFunction::SharedArrayBufferByteLength => "get byteLength"
    BuiltinFunction::SharedArrayBufferMaxByteLength => "get maxByteLength"
    BuiltinFunction::SharedArrayBufferGrowable => "get growable"
    BuiltinFunction::SharedArrayBufferGrow => "grow"
    BuiltinFunction::SharedArrayBufferSlice => "slice"
    BuiltinFunction::DataViewConstructor => "DataView"
    BuiltinFunction::DataViewGetBuffer => "get buffer"
    BuiltinFunction::DataViewGetByteLength => "get byteLength"
    BuiltinFunction::DataViewGetByteOffset => "get byteOffset"
    BuiltinFunction::DataViewGetInt8 => "getInt8"
    BuiltinFunction::DataViewGetUint8 => "getUint8"
    BuiltinFunction::DataViewGetInt16 => "getInt16"
    BuiltinFunction::DataViewGetUint16 => "getUint16"
    BuiltinFunction::DataViewGetInt32 => "getInt32"
    BuiltinFunction::DataViewGetUint32 => "getUint32"
    BuiltinFunction::DataViewGetBigInt64 => "getBigInt64"
    BuiltinFunction::DataViewGetBigUint64 => "getBigUint64"
    BuiltinFunction::DataViewGetFloat16 => "getFloat16"
    BuiltinFunction::DataViewGetFloat32 => "getFloat32"
    BuiltinFunction::DataViewGetFloat64 => "getFloat64"
    BuiltinFunction::DataViewSetInt8 => "setInt8"
    BuiltinFunction::DataViewSetUint8 => "setUint8"
    BuiltinFunction::DataViewSetInt16 => "setInt16"
    BuiltinFunction::DataViewSetUint16 => "setUint16"
    BuiltinFunction::DataViewSetInt32 => "setInt32"
    BuiltinFunction::DataViewSetUint32 => "setUint32"
    BuiltinFunction::DataViewSetBigInt64 => "setBigInt64"
    BuiltinFunction::DataViewSetBigUint64 => "setBigUint64"
    BuiltinFunction::DataViewSetFloat16 => "setFloat16"
    BuiltinFunction::DataViewSetFloat32 => "setFloat32"
    BuiltinFunction::DataViewSetFloat64 => "setFloat64"
    _ => ""
  }
}

///|
fn builtin_length_arraybuffer(func : BuiltinFunction) -> Int {
  match func {
    BuiltinFunction::ArrayBufferIsView => 1
    BuiltinFunction::ArrayBufferByteLength => 0
    BuiltinFunction::ArrayBufferMaxByteLength => 0
    BuiltinFunction::ArrayBufferResizable => 0
    BuiltinFunction::ArrayBufferDetached => 0
    BuiltinFunction::ArrayBufferConstructor => 1
    BuiltinFunction::SharedArrayBufferConstructor => 1
    BuiltinFunction::ArrayBufferSlice => 2
    BuiltinFunction::ArrayBufferTransfer => 0
    BuiltinFunction::ArrayBufferTransferToFixedLength => 0
    BuiltinFunction::ArrayBufferResize => 1
    BuiltinFunction::SharedArrayBufferByteLength => 0
    BuiltinFunction::SharedArrayBufferMaxByteLength => 0
    BuiltinFunction::SharedArrayBufferGrowable => 0
    BuiltinFunction::SharedArrayBufferGrow => 1
    BuiltinFunction::SharedArrayBufferSlice => 2
    BuiltinFunction::DataViewConstructor => 1
    BuiltinFunction::DataViewGetBuffer => 0
    BuiltinFunction::DataViewGetByteLength => 0
    BuiltinFunction::DataViewGetByteOffset => 0
    BuiltinFunction::DataViewGetInt8 => 1
    BuiltinFunction::DataViewGetUint8 => 1
    BuiltinFunction::DataViewGetInt16 => 1
    BuiltinFunction::DataViewGetUint16 => 1
    BuiltinFunction::DataViewGetInt32 => 1
    BuiltinFunction::DataViewGetUint32 => 1
    BuiltinFunction::DataViewGetBigInt64 => 1
    BuiltinFunction::DataViewGetBigUint64 => 1
    BuiltinFunction::DataViewGetFloat16 => 1
    BuiltinFunction::DataViewGetFloat32 => 1
    BuiltinFunction::DataViewGetFloat64 => 1
    BuiltinFunction::DataViewSetInt8 => 2
    BuiltinFunction::DataViewSetUint8 => 2
    BuiltinFunction::DataViewSetInt16 => 2
    BuiltinFunction::DataViewSetUint16 => 2
    BuiltinFunction::DataViewSetInt32 => 2
    BuiltinFunction::DataViewSetUint32 => 2
    BuiltinFunction::DataViewSetBigInt64 => 2
    BuiltinFunction::DataViewSetBigUint64 => 2
    BuiltinFunction::DataViewSetFloat16 => 2
    BuiltinFunction::DataViewSetFloat32 => 2
    BuiltinFunction::DataViewSetFloat64 => 2
    _ => 0
  }
}
