///|
fn string_object_get(
  obj : ObjectValue,
  name : String,
  receiver : Value,
) -> Value raise {
  match obj.string_data {
    Some(value) => {
      if name == "length" {
        return Number(Double::from_int(value.length()))
      }
      match parse_array_index(name) {
        Some(index) =>
          if index >= 0 && index < value.length() {
            return String(value.unsafe_substring(start=index, end=index + 1))
          }
        None => ()
      }
      property_get_from_chain(obj.props, obj.proto, name, receiver)
    }
    None => property_get_from_chain(obj.props, obj.proto, name, receiver)
  }
}

///|
fn number_property_get(value : Double, name : String) -> Value raise {
  match number_proto() {
    Some(proto) =>
      property_get_from_chain(proto.props, proto.proto, name, Number(value))
    None => Undefined
  }
}
