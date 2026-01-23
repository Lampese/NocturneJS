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
  props_set(array_buffer_proto_obj.props, 
    "resize",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayBufferResize),
    ),
  )
  props_set(array_buffer_proto_obj.props, 
    "slice",
    property_data_non_enum(new_builtin_value(BuiltinFunction::ArrayBufferSlice)),
  )
  props_set(array_buffer_proto_obj.props, 
    "transfer",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::ArrayBufferTransfer),
    ),
  )
  props_set(array_buffer_proto_obj.props, 
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
  props_set(shared_array_buffer_proto_obj.props, 
    "grow",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::SharedArrayBufferGrow),
    ),
  )
  props_set(shared_array_buffer_proto_obj.props, 
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
  props_set(dataview_proto_obj.props, 
    "getInt8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt8)),
  )
  props_set(dataview_proto_obj.props, 
    "getUint8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetUint8)),
  )
  props_set(dataview_proto_obj.props, 
    "getInt16",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt16)),
  )
  props_set(dataview_proto_obj.props, 
    "getUint16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetUint16),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getInt32",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewGetInt32)),
  )
  props_set(dataview_proto_obj.props, 
    "getUint32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetUint32),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getBigInt64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetBigInt64),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getBigUint64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetBigUint64),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getFloat16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat16),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getFloat32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat32),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "getFloat64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewGetFloat64),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setInt8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt8)),
  )
  props_set(dataview_proto_obj.props, 
    "setUint8",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetUint8)),
  )
  props_set(dataview_proto_obj.props, 
    "setInt16",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt16)),
  )
  props_set(dataview_proto_obj.props, 
    "setUint16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetUint16),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setInt32",
    property_data_non_enum(new_builtin_value(BuiltinFunction::DataViewSetInt32)),
  )
  props_set(dataview_proto_obj.props, 
    "setUint32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetUint32),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setBigInt64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetBigInt64),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setBigUint64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetBigUint64),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setFloat16",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat16),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setFloat32",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat32),
    ),
  )
  props_set(dataview_proto_obj.props, 
    "setFloat64",
    property_data_non_enum(
      new_builtin_value(BuiltinFunction::DataViewSetFloat64),
    ),
  )
  (dataview_proto_obj, dataview_proto_value)
}
