///|
priv struct BjsonWriteState {
  use_refs : Bool
  refs : Array[Json]
  seen : @hashmap.HashMap[Int, Int]
  stack : @hashmap.HashMap[Int, Bool]
}

///|
fn bjson_tag(tag : String, fields : Array[Json]) -> Json {
  let items : Array[Json] = []
  items.push(Json::string(tag))
  for field in fields {
    items.push(field)
  }
  Json::array(items)
}

///|
fn bjson_number_to_string(value : Double) -> String {
  if Double::is_nan(value) {
    "NaN"
  } else if Double::is_inf(value) {
    if value < 0.0 {
      "-Infinity"
    } else {
      "Infinity"
    }
  } else if is_negative_zero(value) {
    "-0"
  } else {
    Double::to_string(value)
  }
}

///|
fn bjson_number_from_string(text : String) -> Double {
  if text == "NaN" {
    nan()
  } else if text == "Infinity" {
    inf()
  } else if text == "-Infinity" {
    0.0 - inf()
  } else if text == "-0" {
    -0.0
  } else {
    @strconv.parse_double(text) catch {
      _ => nan()
    }
  }
}

///|
fn bjson_value_id(value : Value) -> Int {
  match value {
    Array(arr) => arr.id
    Object(obj) => obj.id
    _ => 0
  }
}

///|
fn bjson_encode_value(value : Value, use_refs : Bool) -> Json raise {
  let state = BjsonWriteState::{
    use_refs,
    refs: [],
    seen: @hashmap.new(),
    stack: @hashmap.new(),
  }
  let root = bjson_encode_node(value, state)
  if use_refs {
    let obj = Map::new()
    obj.set("root", root)
    obj.set("refs", Json::array(state.refs))
    Json::object(obj)
  } else {
    root
  }
}

///|
fn bjson_encode_node(value : Value, state : BjsonWriteState) -> Json raise {
  match value {
    Undefined => bjson_tag("undef", [])
    Null => bjson_tag("null", [])
    Bool(b) => bjson_tag("bool", [Json::boolean(b)])
    Number(n) => bjson_tag("num", [Json::string(bjson_number_to_string(n))])
    String(s) => bjson_tag("str", [Json::string(s)])
    BigInt(b) => bjson_tag("bigint", [Json::string(b.to_string())])
    Symbol(_) => {
      let _ = throw_type_error("unsupported type")
      Json::null()
    }
    Function(_) => {
      let _ = throw_type_error("unsupported type")
      Json::null()
    }
    BoundFunction(_) => {
      let _ = throw_type_error("unsupported type")
      Json::null()
    }
    Builtin(_) => {
      let _ = throw_type_error("unsupported type")
      Json::null()
    }
    Arguments(_) => {
      let _ = throw_type_error("unsupported type")
      Json::null()
    }
    Array(arr) =>
      bjson_encode_ref(Value::Array(arr), state, fn() raise {
        bjson_encode_array(arr, state)
      })
    Object(obj) =>
      bjson_encode_ref(Value::Object(obj), state, fn() raise {
        bjson_encode_object(obj, state)
      })
  }
}

///|
fn bjson_encode_ref(
  value : Value,
  state : BjsonWriteState,
  encode_entry : () -> Json raise,
) -> Json raise {
  let id = bjson_value_id(value)
  if state.use_refs {
    match state.seen.get(id) {
      Some(existing) =>
        bjson_tag("ref", [Json::number(Double::from_int(existing))])
      None => {
        let new_id = state.refs.length()
        state.seen.set(id, new_id)
        state.refs.push(Json::null())
        let entry = encode_entry()
        state.refs[new_id] = entry
        bjson_tag("ref", [Json::number(Double::from_int(new_id))])
      }
    }
  } else {
    match state.stack.get(id) {
      Some(_) => {
        let _ = throw_type_error("circular reference")

      }
      None => ()
    }
    state.stack.set(id, true)
    let entry = encode_entry()
    let _ = state.stack.remove(id)
    entry
  }
}

///|
fn bjson_encode_array(arr : ArrayValue, state : BjsonWriteState) -> Json raise {
  match arr.typed_array_data {
    Some(data) => bjson_encode_typed_array(arr, data, state)
    None => {
      let len = arr.elements.length()
      let elems : Array[Json] = []
      let mut i = 0
      while i < len {
        match arr.elements[i] {
          Some(value) => elems.push(bjson_encode_node(value, state))
          None => elems.push(bjson_tag("hole", []))
        }
        i = i + 1
      }
      bjson_tag("arr", [Json::number(Double::from_int(len)), Json::array(elems)])
    }
  }
}

///|
fn bjson_encode_object(
  obj : ObjectValue,
  state : BjsonWriteState,
) -> Json raise {
  match obj.array_buffer_data {
    Some(data) => bjson_encode_array_buffer(obj, data)
    None =>
      match obj.date_data {
        Some(value) =>
          bjson_tag("date", [Json::string(bjson_number_to_string(value))])
        None =>
          match obj.string_data {
            Some(value) => bjson_tag("str_obj", [Json::string(value)])
            None =>
              match obj.number_data {
                Some(value) =>
                  bjson_tag("num_obj", [
                    Json::string(bjson_number_to_string(value)),
                  ])
                None =>
                  match obj.bool_data {
                    Some(value) => bjson_tag("bool_obj", [Json::boolean(value)])
                    None =>
                      match obj.bigint_data {
                        Some(value) =>
                          bjson_tag("bigint_obj", [
                            Json::string(value.to_string()),
                          ])
                        None =>
                          match
                            (
                              obj.symbol_data,
                              obj.regexp_data,
                              obj.generator_data,
                              obj.proxy_data,
                            ) {
                            (Some(_), _, _, _) => {
                              let _ = throw_type_error("unsupported type")
                              Json::null()
                            }
                            (_, Some(_), _, _) => {
                              let _ = throw_type_error("unsupported type")
                              Json::null()
                            }
                            (_, _, Some(_), _) => {
                              let _ = throw_type_error("unsupported type")
                              Json::null()
                            }
                            (_, _, _, Some(_)) => {
                              let _ = throw_type_error("unsupported type")
                              Json::null()
                            }
                            _ => {
                              let entries : Array[Json] = []
                              for key, prop in obj.props {
                                if is_symbol_prop_key(key) || !prop.enumerable {
                                  continue
                                }
                                let pair : Array[Json] = [
                                  Json::string(key),
                                  bjson_encode_node(prop.value, state),
                                ]
                                entries.push(Json::array(pair))
                              }
                              bjson_tag("obj", [Json::array(entries)])
                            }
                          }
                      }
                  }
              }
          }
      }
  }
}

///|
fn bjson_encode_array_buffer(obj : ObjectValue, data : ArrayBufferData) -> Json {
  let bytes : Array[Json] = []
  for b in data.bytes {
    bytes.push(Json::number(Double::from_int(Byte::to_int(b))))
  }
  let mut max_len = data.bytes.length()
  match obj.props.get("maxByteLength") {
    Some(prop) =>
      match prop.value {
        Number(value) => max_len = to_int32(Double::trunc(value))
        _ => ()
      }
    None => ()
  }
  let mut resizable = false
  match obj.props.get("resizable") {
    Some(prop) =>
      match prop.value {
        Bool(value) => resizable = value
        _ => ()
      }
    None => ()
  }
  bjson_tag("ab", [
    Json::array(bytes),
    Json::number(Double::from_int(max_len)),
    Json::boolean(resizable),
  ])
}

///|
fn bjson_encode_typed_array(
  _arr : ArrayValue,
  data : TypedArrayData,
  state : BjsonWriteState,
) -> Json raise {
  let kind = typed_array_kind_name(data.kind)
  let buffer_node = bjson_encode_node(Object(data.buffer), state)
  bjson_tag("ta", [
    Json::string(kind),
    buffer_node,
    Json::number(Double::from_int(data.byte_offset)),
    Json::number(Double::from_int(data.length)),
  ])
}

///|
fn typed_array_kind_name(kind : TypedArrayKind) -> String {
  match kind {
    TypedArrayKind::Uint8 => "Uint8"
    TypedArrayKind::Int8 => "Int8"
    TypedArrayKind::Uint8Clamped => "Uint8Clamped"
    TypedArrayKind::Uint16 => "Uint16"
    TypedArrayKind::Int16 => "Int16"
    TypedArrayKind::Uint32 => "Uint32"
    TypedArrayKind::Int32 => "Int32"
    TypedArrayKind::BigInt64 => "BigInt64"
    TypedArrayKind::BigUint64 => "BigUint64"
    TypedArrayKind::Float16 => "Float16"
    TypedArrayKind::Float32 => "Float32"
    TypedArrayKind::Float64 => "Float64"
  }
}

///|
fn typed_array_kind_from_name(name : String) -> TypedArrayKind? {
  match name {
    "Uint8" => Some(TypedArrayKind::Uint8)
    "Int8" => Some(TypedArrayKind::Int8)
    "Uint8Clamped" => Some(TypedArrayKind::Uint8Clamped)
    "Uint16" => Some(TypedArrayKind::Uint16)
    "Int16" => Some(TypedArrayKind::Int16)
    "Uint32" => Some(TypedArrayKind::Uint32)
    "Int32" => Some(TypedArrayKind::Int32)
    "BigInt64" => Some(TypedArrayKind::BigInt64)
    "BigUint64" => Some(TypedArrayKind::BigUint64)
    "Float16" => Some(TypedArrayKind::Float16)
    "Float32" => Some(TypedArrayKind::Float32)
    "Float64" => Some(TypedArrayKind::Float64)
    _ => None
  }
}

///|
fn bjson_parse_tag(value : Json) -> (String, Array[Json])? {
  match value {
    Json::Array(items) =>
      if items.is_empty() {
        None
      } else {
        match items[0] {
          Json::String(tag) => {
            let fields : Array[Json] = []
            let mut i = 1
            while i < items.length() {
              fields.push(items[i])
              i = i + 1
            }
            Some((tag, fields))
          }
          _ => None
        }
      }
    _ => None
  }
}

///|
fn bjson_invalid_value() -> Value raise {
  let _ = throw_type_error("invalid bjson")
  Undefined
}

///|
fn bjson_decode_value(value : Json, use_refs : Bool) -> Value raise {
  if use_refs {
    match value {
      Json::Object(entries) =>
        match (entries.get("root"), entries.get("refs")) {
          (Some(root_node), Some(Json::Array(ref_nodes))) => {
            let refs = bjson_decode_refs(ref_nodes)
            bjson_decode_node(root_node, refs, true)
          }
          _ => bjson_invalid_value()
        }
      _ => bjson_invalid_value()
    }
  } else {
    bjson_decode_node(value, [], false)
  }
}

///|
fn bjson_decode_node(
  value : Json,
  refs : Array[Value],
  use_refs : Bool,
) -> Value raise {
  match bjson_parse_tag(value) {
    Some((tag, fields)) =>
      match tag {
        "undef" => Undefined
        "null" => Null
        "bool" =>
          match fields.get(0) {
            Some(Json::True) => Bool(true)
            Some(Json::False) => Bool(false)
            _ => bjson_invalid_value()
          }
        "num" =>
          match fields.get(0) {
            Some(Json::String(text)) => Number(bjson_number_from_string(text))
            _ => bjson_invalid_value()
          }
        "str" =>
          match fields.get(0) {
            Some(Json::String(text)) => String(text)
            _ => bjson_invalid_value()
          }
        "bigint" =>
          match fields.get(0) {
            Some(Json::String(text)) => BigInt(parse_bigint_string(text))
            _ => bjson_invalid_value()
          }
        "date" =>
          match fields.get(0) {
            Some(Json::String(text)) =>
              new_date_value(bjson_number_from_string(text))
            _ => bjson_invalid_value()
          }
        "str_obj" =>
          match fields.get(0) {
            Some(Json::String(text)) => new_string_object(text)
            _ => bjson_invalid_value()
          }
        "num_obj" =>
          match fields.get(0) {
            Some(Json::String(text)) =>
              new_number_object(bjson_number_from_string(text))
            _ => bjson_invalid_value()
          }
        "bool_obj" =>
          match fields.get(0) {
            Some(Json::True) => new_bool_object(true)
            Some(Json::False) => new_bool_object(false)
            _ => bjson_invalid_value()
          }
        "bigint_obj" =>
          match fields.get(0) {
            Some(Json::String(text)) =>
              new_bigint_object(parse_bigint_string(text))
            _ => bjson_invalid_value()
          }
        "arr" => bjson_decode_array_inline(fields, refs, use_refs)
        "obj" => bjson_decode_object_inline(fields, refs, use_refs)
        "ab" => bjson_decode_array_buffer_node(fields)
        "ta" => bjson_decode_typed_array_node(fields, refs, use_refs)
        "ref" =>
          if !use_refs {
            bjson_invalid_value()
          } else {
            match fields.get(0) {
              Some(Json::Number(value, ..)) => {
                let id = to_int32(Double::trunc(value))
                if id < 0 || id >= refs.length() {
                  bjson_invalid_value()
                } else {
                  refs[id]
                }
              }
              _ => bjson_invalid_value()
            }
          }
        _ => bjson_invalid_value()
      }
    None => bjson_invalid_value()
  }
}

///|
fn bjson_decode_refs(ref_nodes : Array[Json]) -> Array[Value] raise {
  let refs : Array[Value] = []
  let mut i = 0
  while i < ref_nodes.length() {
    refs.push(Undefined)
    i = i + 1
  }
  let typed_entries : Array[(Int, Json)] = []
  let mut index = 0
  while index < ref_nodes.length() {
    match bjson_parse_tag(ref_nodes[index]) {
      Some((tag, fields)) =>
        match tag {
          "obj" => refs[index] = new_object_value()
          "arr" => {
            let len = match fields.get(0) {
              Some(Json::Number(value, ..)) => to_int32(Double::trunc(value))
              _ => 0
            }
            let elements = Array::make(len, None)
            refs[index] = new_array_value(elements)
          }
          "ab" => refs[index] = bjson_decode_array_buffer_node(fields)
          "date" =>
            match fields.get(0) {
              Some(Json::String(text)) =>
                refs[index] = new_date_value(bjson_number_from_string(text))
              _ => refs[index] = new_date_value(nan())
            }
          "str_obj" =>
            match fields.get(0) {
              Some(Json::String(text)) => refs[index] = new_string_object(text)
              _ => refs[index] = new_string_object("")
            }
          "num_obj" =>
            match fields.get(0) {
              Some(Json::String(text)) =>
                refs[index] = new_number_object(bjson_number_from_string(text))
              _ => refs[index] = new_number_object(0.0)
            }
          "bool_obj" =>
            match fields.get(0) {
              Some(Json::True) => refs[index] = new_bool_object(true)
              Some(Json::False) => refs[index] = new_bool_object(false)
              _ => refs[index] = new_bool_object(false)
            }
          "bigint_obj" =>
            match fields.get(0) {
              Some(Json::String(text)) =>
                refs[index] = new_bigint_object(parse_bigint_string(text))
              _ => refs[index] = new_bigint_object(@bigint.BigInt::from_int(0))
            }
          "ta" => typed_entries.push((index, ref_nodes[index]))
          _ => {
            let _ = throw_type_error("invalid bjson")

          }
        }
      None => {
        let _ = throw_type_error("invalid bjson")

      }
    }
    index = index + 1
  }
  for entry in typed_entries {
    let (id, node) = entry
    match bjson_parse_tag(node) {
      Some((_, fields)) =>
        refs[id] = bjson_decode_typed_array_node(fields, refs, true)
      None => ()
    }
  }
  let mut fill_index = 0
  while fill_index < ref_nodes.length() {
    match bjson_parse_tag(ref_nodes[fill_index]) {
      Some((tag, fields)) =>
        match tag {
          "obj" => bjson_fill_object(refs[fill_index], fields, refs, true)
          "arr" => bjson_fill_array(refs[fill_index], fields, refs, true)
          _ => ()
        }
      None => ()
    }
    fill_index = fill_index + 1
  }
  refs
}

///|
fn bjson_decode_array_inline(
  fields : Array[Json],
  refs : Array[Value],
  use_refs : Bool,
) -> Value raise {
  let len = match fields.get(0) {
    Some(Json::Number(value, ..)) => to_int32(Double::trunc(value))
    _ => 0
  }
  let elements = Array::make(len, None)
  match fields.get(1) {
    Some(Json::Array(items)) => {
      let mut i = 0
      while i < items.length() && i < len {
        match bjson_parse_tag(items[i]) {
          Some((tag, _)) if tag == "hole" => ()
          _ => elements[i] = Some(bjson_decode_node(items[i], refs, use_refs))
        }
        i = i + 1
      }
    }
    _ => ()
  }
  new_array_value(elements)
}

///|
fn bjson_decode_object_inline(
  fields : Array[Json],
  refs : Array[Value],
  use_refs : Bool,
) -> Value raise {
  let value = new_object_value()
  bjson_fill_object(value, fields, refs, use_refs)
  value
}

///|
fn bjson_fill_object(
  value : Value,
  fields : Array[Json],
  refs : Array[Value],
  use_refs : Bool,
) -> Unit raise {
  match value {
    Object(obj) =>
      match fields.get(0) {
        Some(Json::Array(entries)) =>
          for entry in entries {
            match entry {
              Json::Array(pair) =>
                if pair.length() >= 2 {
                  match pair[0] {
                    Json::String(key) => {
                      let val = bjson_decode_node(pair[1], refs, use_refs)
                      obj.props.set(key, property_data(val))
                    }
                    _ => ()
                  }
                }
              _ => ()
            }
          }
        _ => ()
      }
    _ => ()
  }
}

///|
fn bjson_fill_array(
  value : Value,
  fields : Array[Json],
  refs : Array[Value],
  use_refs : Bool,
) -> Unit raise {
  match value {
    Array(arr) =>
      match fields.get(1) {
        Some(Json::Array(items)) => {
          let mut i = 0
          while i < items.length() && i < arr.elements.length() {
            match bjson_parse_tag(items[i]) {
              Some((tag, _)) if tag == "hole" => ()
              _ =>
                arr.elements[i] = Some(
                  bjson_decode_node(items[i], refs, use_refs),
                )
            }
            i = i + 1
          }
        }
        _ => ()
      }
    _ => ()
  }
}

///|
fn bjson_decode_array_buffer_node(fields : Array[Json]) -> Value {
  let bytes : Array[Byte] = []
  match fields.get(0) {
    Some(Json::Array(items)) =>
      for item in items {
        match item {
          Json::Number(value, ..) =>
            bytes.push(Int::to_byte(to_int32(Double::trunc(value))))
          _ => bytes.push(Int::to_byte(0))
        }
      }
    _ => ()
  }
  let byte_len = bytes.length()
  let max_len = match fields.get(1) {
    Some(Json::Number(value, ..)) => to_int32(Double::trunc(value))
    _ => byte_len
  }
  let resizable = match fields.get(2) {
    Some(Json::True) => true
    _ => false
  }
  let buffer_value = new_array_buffer_value_with_options(
    byte_len, max_len, resizable,
  )
  match buffer_value {
    Object(obj) =>
      match obj.array_buffer_data {
        Some(data) => data.bytes = bytes
        None => ()
      }
    _ => ()
  }
  buffer_value
}

///|
fn bjson_decode_typed_array_node(
  fields : Array[Json],
  refs : Array[Value],
  use_refs : Bool,
) -> Value raise {
  let kind = match fields.get(0) {
    Some(Json::String(name)) =>
      match typed_array_kind_from_name(name) {
        Some(kind) => kind
        None => TypedArrayKind::Uint8
      }
    _ => TypedArrayKind::Uint8
  }
  let buffer_value = match fields.get(1) {
    Some(node) => bjson_decode_node(node, refs, use_refs)
    None => new_array_buffer_value(0)
  }
  let (buffer_obj, _) = require_array_buffer(Some(buffer_value))
  let byte_offset = match fields.get(2) {
    Some(Json::Number(value, ..)) => to_int32(Double::trunc(value))
    _ => 0
  }
  let length = match fields.get(3) {
    Some(Json::Number(value, ..)) => to_int32(Double::trunc(value))
    _ => 0
  }
  new_typed_array_view(kind, buffer_obj, byte_offset, length, false)
}

///|
fn bjson_read_value(args : Array[Value]) -> Value raise {
  if args.length() < 3 {
    return throw_type_error("invalid arguments")
  }
  let buffer_value = args[0]
  let pos = to_int32(Double::trunc(to_number(args[1])))
  let len = to_int32(Double::trunc(to_number(args[2])))
  let use_refs = args.length() > 3 && is_truthy(args[3])
  let (_, data) = require_array_buffer(Some(buffer_value))
  if data.detached {
    return throw_type_error("invalid buffer")
  }
  if pos < 0 || len < 0 || pos + len > data.bytes.length() {
    return throw_range_error("array buffer overflow")
  }
  let slice : Array[Byte] = []
  let mut i = pos
  while i < pos + len {
    slice.push(data.bytes[i])
    i = i + 1
  }
  let text = string_from_bytes(slice)
  let json = @json.parse(text[:]) catch {
    err => {
      let (line, column) = json_parse_error_pos(err)
      raise ThrowSignal(
        new_error_value_with_pos(
          syntax_error_proto(),
          err.to_string(),
          line,
          column + 1,
        ),
      )
      Json::null()
    }
  }
  bjson_decode_value(json, use_refs)
}

///|
fn bjson_write_value(args : Array[Value]) -> Value raise {
  if args.is_empty() {
    return throw_type_error("invalid arguments")
  }
  let use_refs = args.length() > 1 && is_truthy(args[1])
  let json = bjson_encode_value(args[0], use_refs)
  let text = Json::stringify(json)
  let bytes = bytes_from_string(text)
  let buffer_value = new_array_buffer_value(bytes.length())
  match buffer_value {
    Object(obj) =>
      match obj.array_buffer_data {
        Some(data) => data.bytes = bytes
        None => ()
      }
    _ => ()
  }
  buffer_value
}
