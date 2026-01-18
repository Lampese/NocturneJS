///|
fn init_array_buffer_proto(
  object_proto_obj : ObjectValue,
) -> (ObjectValue, Value) {
  let array_buffer_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let array_buffer_proto_obj = match array_buffer_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(array_buffer_proto_ref, Some(array_buffer_proto_obj))
  set_accessor_property_with_enum(
    array_buffer_proto_obj.props,
    "byteLength",
    Some(new_builtin_value(BuiltinFunction::ArrayBufferByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    array_buffer_proto_obj.props,
    "maxByteLength",
    Some(new_builtin_value(BuiltinFunction::ArrayBufferMaxByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    array_buffer_proto_obj.props,
    "resizable",
    Some(new_builtin_value(BuiltinFunction::ArrayBufferResizable)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    array_buffer_proto_obj.props,
    "detached",
    Some(new_builtin_value(BuiltinFunction::ArrayBufferDetached)),
    None,
    false,
  )
  array_buffer_proto_obj.props.set(
    "resize",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayBufferResize),
    ),
  )
  array_buffer_proto_obj.props.set(
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayBufferSlice)),
  )
  array_buffer_proto_obj.props.set(
    "transfer",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayBufferTransfer),
    ),
  )
  array_buffer_proto_obj.props.set(
    "transferToFixedLength",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayBufferTransferToFixedLength),
    ),
  )
  (array_buffer_proto_obj, array_buffer_proto_value)
}

///|
fn init_shared_array_buffer_proto(
  object_proto_obj : ObjectValue,
) -> (ObjectValue, Value) {
  let shared_array_buffer_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let shared_array_buffer_proto_obj = match shared_array_buffer_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(
    shared_array_buffer_proto_ref,
    Some(shared_array_buffer_proto_obj),
  )
  set_accessor_property_with_enum(
    shared_array_buffer_proto_obj.props,
    "byteLength",
    Some(new_builtin_value(BuiltinFunction::SharedArrayBufferByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    shared_array_buffer_proto_obj.props,
    "maxByteLength",
    Some(new_builtin_value(BuiltinFunction::SharedArrayBufferMaxByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    shared_array_buffer_proto_obj.props,
    "growable",
    Some(new_builtin_value(BuiltinFunction::SharedArrayBufferGrowable)),
    None,
    false,
  )
  shared_array_buffer_proto_obj.props.set(
    "grow",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SharedArrayBufferGrow),
    ),
  )
  shared_array_buffer_proto_obj.props.set(
    "slice",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SharedArrayBufferSlice),
    ),
  )
  (shared_array_buffer_proto_obj, shared_array_buffer_proto_value)
}

///|
fn init_dataview_proto(object_proto_obj : ObjectValue) -> (ObjectValue, Value) {
  let dataview_proto_value = new_object_value_with_proto(
    Some(Object(object_proto_obj)),
  )
  let dataview_proto_obj = match dataview_proto_value {
    Object(obj) => obj
    _ => new_object_struct(Some(Object(object_proto_obj)))
  }
  set_proto_ref(dataview_proto_ref, Some(dataview_proto_obj))
  set_accessor_property_with_enum(
    dataview_proto_obj.props,
    "buffer",
    Some(new_builtin_value(BuiltinFunction::DataViewGetBuffer)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    dataview_proto_obj.props,
    "byteLength",
    Some(new_builtin_value(BuiltinFunction::DataViewGetByteLength)),
    None,
    false,
  )
  set_accessor_property_with_enum(
    dataview_proto_obj.props,
    "byteOffset",
    Some(new_builtin_value(BuiltinFunction::DataViewGetByteOffset)),
    None,
    false,
  )
  dataview_proto_obj.props.set(
    "getInt8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt8)),
  )
  dataview_proto_obj.props.set(
    "getUint8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetUint8)),
  )
  dataview_proto_obj.props.set(
    "getInt16",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt16)),
  )
  dataview_proto_obj.props.set(
    "getUint16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetUint16),
    ),
  )
  dataview_proto_obj.props.set(
    "getInt32",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt32)),
  )
  dataview_proto_obj.props.set(
    "getUint32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetUint32),
    ),
  )
  dataview_proto_obj.props.set(
    "getBigInt64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetBigInt64),
    ),
  )
  dataview_proto_obj.props.set(
    "getBigUint64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetBigUint64),
    ),
  )
  dataview_proto_obj.props.set(
    "getFloat16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat16),
    ),
  )
  dataview_proto_obj.props.set(
    "getFloat32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat32),
    ),
  )
  dataview_proto_obj.props.set(
    "getFloat64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat64),
    ),
  )
  dataview_proto_obj.props.set(
    "setInt8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt8)),
  )
  dataview_proto_obj.props.set(
    "setUint8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetUint8)),
  )
  dataview_proto_obj.props.set(
    "setInt16",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt16)),
  )
  dataview_proto_obj.props.set(
    "setUint16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetUint16),
    ),
  )
  dataview_proto_obj.props.set(
    "setInt32",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt32)),
  )
  dataview_proto_obj.props.set(
    "setUint32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetUint32),
    ),
  )
  dataview_proto_obj.props.set(
    "setBigInt64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetBigInt64),
    ),
  )
  dataview_proto_obj.props.set(
    "setBigUint64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetBigUint64),
    ),
  )
  dataview_proto_obj.props.set(
    "setFloat16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat16),
    ),
  )
  dataview_proto_obj.props.set(
    "setFloat32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat32),
    ),
  )
  dataview_proto_obj.props.set(
    "setFloat64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat64),
    ),
  )
  (dataview_proto_obj, dataview_proto_value)
}
