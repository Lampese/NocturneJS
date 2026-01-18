///|
fn integer_indexed_element_set(
  data : TypedArrayData,
  index : Double,
  value : Value,
) -> Unit raise {
  let converted = match data.kind {
    TypedArrayKind::BigInt64 | TypedArrayKind::BigUint64 =>
      BigInt(to_bigint(value))
    _ => Number(to_number(value))
  }
  if !typed_array_is_valid_integer_index(data, index) {
    return
  }
  let index_int = Double::to_int(index)
  let _ = typed_array_set_index(data, index_int, converted)

}

///|
fn typed_array_set(
  arr : ArrayValue,
  data : TypedArrayData,
  name : String,
  value : Value,
) -> Bool raise {
  if name == "length" {
    return false
  }
  match canonical_numeric_index_string(name) {
    Some(index) => {
      integer_indexed_element_set(data, index, value)
      true
    }
    None =>
      set_property_with_proto(
        arr.props,
        arr.proto,
        name,
        value,
        Array(arr),
        arr.extensible,
      )
  }
}
