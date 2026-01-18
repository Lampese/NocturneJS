///|
fn sort_int64s(values : Array[Int64]) -> Unit {
  if values.length() < 2 {
    return
  }
  for i = 1; i < values.length(); i = i + 1 {
    let key = values[i]
    let mut j = i - 1
    while j >= 0 && values[j] > key {
      values[j + 1] = values[j]
      j = j - 1
    }
    values[j + 1] = key
  }
}

///|
fn sort_strings_lex(values : Array[String]) -> Unit {
  if values.length() < 2 {
    return
  }
  for i = 1; i < values.length(); i = i + 1 {
    let key = values[i]
    let mut j = i - 1
    while j >= 0 && string_lex_compare(values[j], key) > 0 {
      values[j + 1] = values[j]
      j = j - 1
    }
    values[j + 1] = key
  }
}
