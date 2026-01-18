///|
fn call_builtin_string(
  builtin : BuiltinValue,
  args : Array[Value],
) -> Value raise {
  match builtin.kind {
    BuiltinFunction::StringConstructor => {
      if args.is_empty() {
        return String("")
      }
      match args[0] {
        Symbol(symbol) => String(symbol_to_string(symbol))
        _ => String(to_string_strict(args[0]))
      }
    }
    BuiltinFunction::StringFromCharCode => String(string_from_char_codes(args))
    BuiltinFunction::StringFromCodePoint =>
      String(string_from_code_points(args))
    BuiltinFunction::StringRaw => {
      let callsite = if args.is_empty() {
        to_object(Undefined)
      } else {
        to_object(args[0])
      }
      let raw_value = property_get(callsite, "raw")
      let raw_obj = to_object(raw_value)
      let len_value = property_get(raw_obj, "length")
      let len = to_length_int64(to_number(len_value))
      let mut result = ""
      let mut i = 0L
      while i < len {
        let part = property_get(raw_obj, Int64::to_string(i))
        result = result + to_string_strict(part)
        let arg_index = i + 1L
        if arg_index < len && arg_index < Int64::from_int(args.length()) {
          let arg_pos = Int64::to_int(arg_index)
          result = result + to_string_strict(args[arg_pos])
        }
        i = i + 1L
      }
      String(result)
    }
    BuiltinFunction::StringCharCodeAt => {
      let value = this_to_string_coerce(builtin.this_value)
      let index = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      if index < 0 || index >= value.length() {
        Number(nan())
      } else {
        let code = value.code_unit_at(index)
        Number(Double::from_int(UInt16::to_int(code)))
      }
    }
    BuiltinFunction::StringCharAt => {
      let value = this_to_string_coerce(builtin.this_value)
      let index = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      if index < 0 || index >= value.length() {
        String("")
      } else {
        String(value.unsafe_substring(start=index, end=index + 1))
      }
    }
    BuiltinFunction::StringCodePointAt => {
      let value = this_to_string_coerce(builtin.this_value)
      let index = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      if index < 0 || index >= value.length() {
        Undefined
      } else {
        let first = UInt16::to_int(value.code_unit_at(index))
        if first >= 0xD800 && first <= 0xDBFF && index + 1 < value.length() {
          let second = UInt16::to_int(value.code_unit_at(index + 1))
          if second >= 0xDC00 && second <= 0xDFFF {
            let code_point = 0x10000 +
              ((first - 0xD800) << 10) +
              (second - 0xDC00)
            Number(Double::from_int(code_point))
          } else {
            Number(Double::from_int(first))
          }
        } else {
          Number(Double::from_int(first))
        }
      }
    }
    BuiltinFunction::StringConcat => {
      let value = this_to_string_coerce(builtin.this_value)
      let mut result = value
      for arg in args {
        result = result + to_string_strict(arg)
      }
      String(result)
    }
    BuiltinFunction::StringSubstring => {
      let value = this_to_string_coerce(builtin.this_value)
      let len = value.length()
      let start = if args.is_empty() {
        0
      } else {
        clamp_index(to_number(args[0]), len)
      }
      let end = if args.length() < 2 || args[1] is Undefined {
        len
      } else {
        clamp_index(to_number(args[1]), len)
      }
      let (from, to) = if start > end { (end, start) } else { (start, end) }
      String(value.unsafe_substring(start=from, end=to))
    }
    BuiltinFunction::StringSubstr => {
      let value = this_to_string_coerce(builtin.this_value)
      let len = value.length()
      let start = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      let mut from = start
      if from < 0 {
        from = from + len
        if from < 0 {
          from = 0
        }
      } else if from > len {
        from = len
      }
      let mut count = len - from
      if args.length() > 1 && !(args[1] is Undefined) {
        let limit = to_int32_sat(to_number(args[1]))
        let mut size = limit
        if size < 0 {
          size = 0
        }
        let max_len = len - from
        if size > max_len {
          size = max_len
        }
        count = size
      }
      String(value.unsafe_substring(start=from, end=from + count))
    }
    BuiltinFunction::StringSlice => {
      let value = this_to_string_coerce(builtin.this_value)
      let len = value.length()
      let start = if args.is_empty() {
        0
      } else {
        slice_index(to_number(args[0]), len)
      }
      let end = if args.length() < 2 || args[1] is Undefined {
        len
      } else {
        slice_index(to_number(args[1]), len)
      }
      if end <= start {
        String("")
      } else {
        String(value.unsafe_substring(start~, end~))
      }
    }
    BuiltinFunction::StringIndexOf => {
      let value = this_to_string_coerce(builtin.this_value)
      let search = if args.is_empty() {
        to_string_strict(Undefined)
      } else {
        to_string_strict(args[0])
      }
      let len = value.length()
      let from = if args.length() < 2 {
        0
      } else {
        clamp_index(to_number(args[1]), len)
      }
      let index = string_index_of(value, search, from)
      Number(Double::from_int(index))
    }
    BuiltinFunction::StringLastIndexOf => {
      let value = this_to_string_coerce(builtin.this_value)
      let search = if args.is_empty() {
        to_string_strict(Undefined)
      } else {
        to_string_strict(args[0])
      }
      let len = value.length()
      let from = if args.length() < 2 {
        len
      } else {
        clamp_last_index(to_number(args[1]), len)
      }
      let index = string_last_index_of(value, search, from)
      Number(Double::from_int(index))
    }
    BuiltinFunction::StringSplit => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      if !args.is_empty() && is_object_like(args[0]) {
        match symbol_split_key() {
          Some(key) =>
            match property_get(args[0], key) {
              Undefined | Null => ()
              splitter =>
                return call_value_with_this(
                  splitter,
                  [
                    this_value,
                    if args.length() > 1 {
                      args[1]
                    } else {
                      Undefined
                    },
                  ],
                  args[0],
                )
            }
          None => ()
        }
      }
      let value = this_to_string_coerce(builtin.this_value)
      let limit = if args.length() < 2 || args[1] is Undefined {
        0xffffffffU
      } else {
        to_uint32(to_number(args[1]))
      }
      string_split_value(value, args, limit)
    }
    BuiltinFunction::StringPadStart => {
      let value = this_to_string_coerce(builtin.this_value)
      let target = if args.is_empty() { 0.0 } else { to_number(args[0]) }
      let fill = if args.length() < 2 || args[1] is Undefined {
        " "
      } else {
        to_string_strict(args[1])
      }
      if Double::is_inf(target) || Double::is_nan(target) {
        String(value)
      } else {
        let target_len = Double::to_int(Double::trunc(target))
        String(string_pad_start(value, target_len, fill))
      }
    }
    BuiltinFunction::StringPadEnd => {
      let value = this_to_string_coerce(builtin.this_value)
      let target = if args.is_empty() { 0.0 } else { to_number(args[0]) }
      let fill = if args.length() < 2 || args[1] is Undefined {
        " "
      } else {
        to_string_strict(args[1])
      }
      if Double::is_inf(target) || Double::is_nan(target) {
        String(value)
      } else {
        let target_len = Double::to_int(Double::trunc(target))
        String(string_pad_end(value, target_len, fill))
      }
    }
    BuiltinFunction::StringAt => {
      let value = this_to_string_coerce(builtin.this_value)
      let len = value.length()
      let index = if args.is_empty() {
        0
      } else {
        to_int32_sat(to_number(args[0]))
      }
      let mut idx = index
      if idx < 0 {
        idx = idx + len
      }
      if idx < 0 || idx >= len {
        Undefined
      } else {
        String(value.unsafe_substring(start=idx, end=idx + 1))
      }
    }
    BuiltinFunction::StringIncludes => {
      let value = this_to_string_coerce(builtin.this_value)
      let search_value = if args.is_empty() { Undefined } else { args[0] }
      if is_regexp_value(search_value) {
        let _ = throw_type_error("regexp not supported")

      }
      let search = to_string_strict(search_value)
      let len = value.length()
      let from = if args.length() > 1 && !(args[1] is Undefined) {
        clamp_index(to_number(args[1]), len)
      } else {
        0
      }
      Bool(string_index_of(value, search, from) >= 0)
    }
    BuiltinFunction::StringStartsWith => {
      let value = this_to_string_coerce(builtin.this_value)
      let search_value = if args.is_empty() { Undefined } else { args[0] }
      if is_regexp_value(search_value) {
        let _ = throw_type_error("regexp not supported")

      }
      let search = to_string_strict(search_value)
      let len = value.length()
      let pos = if args.length() > 1 && !(args[1] is Undefined) {
        clamp_index(to_number(args[1]), len)
      } else {
        0
      }
      if search.is_empty() {
        Bool(true)
      } else if pos + search.length() > len {
        Bool(false)
      } else {
        let slice = value.unsafe_substring(start=pos, end=pos + search.length())
        Bool(slice == search)
      }
    }
    BuiltinFunction::StringEndsWith => {
      let value = this_to_string_coerce(builtin.this_value)
      let search_value = if args.is_empty() { Undefined } else { args[0] }
      if is_regexp_value(search_value) {
        let _ = throw_type_error("regexp not supported")

      }
      let search = to_string_strict(search_value)
      let len = value.length()
      let end = if args.length() > 1 && !(args[1] is Undefined) {
        clamp_index(to_number(args[1]), len)
      } else {
        len
      }
      let search_len = search.length()
      if search_len == 0 {
        Bool(true)
      } else if search_len > end {
        Bool(false)
      } else {
        let start = end - search_len
        let slice = value.unsafe_substring(start~, end~)
        Bool(slice == search)
      }
    }
    BuiltinFunction::StringRepeat => {
      let value = this_to_string_coerce(builtin.this_value)
      let count_num = if args.is_empty() {
        to_number(Undefined)
      } else {
        to_number(args[0])
      }
      if Double::is_inf(count_num) || count_num < 0.0 {
        throw_range_error("invalid repeat count")
      } else {
        let count = if Double::is_nan(count_num) {
          0
        } else {
          Int64::to_int(Double::to_int64(Double::trunc(count_num)))
        }
        if count < 0 || count > 2147483647 {
          throw_range_error("invalid repeat count")
        } else if count == 0 {
          String("")
        } else if count == 1 {
          String(value)
        } else {
          let len = value.length()
          if len > 0 && count > js_string_len_max / len {
            throw_range_error("invalid string length")
          } else {
            String(value.repeat(count))
          }
        }
      }
    }
    BuiltinFunction::StringReplace => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      string_replace_value(this_value, args)
    }
    BuiltinFunction::StringReplaceAll => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      string_replace_all_value(this_value, args)
    }
    BuiltinFunction::StringTrim => {
      let value = this_to_string_coerce(builtin.this_value)
      String(trim_js_whitespace(value))
    }
    BuiltinFunction::StringTrimStart => {
      let value = this_to_string_coerce(builtin.this_value)
      String(trim_js_whitespace_start(value))
    }
    BuiltinFunction::StringTrimEnd => {
      let value = this_to_string_coerce(builtin.this_value)
      String(trim_js_whitespace_end(value))
    }
    BuiltinFunction::StringToLowerCase => {
      let value = this_to_string_coerce(builtin.this_value)
      String(string_case_convert(value, true))
    }
    BuiltinFunction::StringToUpperCase => {
      let value = this_to_string_coerce(builtin.this_value)
      String(string_case_convert(value, false))
    }
    BuiltinFunction::StringToLocaleLowerCase => {
      let value = this_to_string_coerce(builtin.this_value)
      String(string_case_convert(value, true))
    }
    BuiltinFunction::StringToLocaleUpperCase => {
      let value = this_to_string_coerce(builtin.this_value)
      String(string_case_convert(value, false))
    }
    BuiltinFunction::StringLocaleCompare => {
      let value = this_to_string_coerce(builtin.this_value)
      let other = if args.is_empty() {
        to_string_strict(Undefined)
      } else {
        to_string_strict(args[0])
      }
      let left = string_normalize_form(value, "NFC")
      let right = string_normalize_form(other, "NFC")
      Number(Double::from_int(string_lex_compare(left, right)))
    }
    BuiltinFunction::StringNormalize => {
      let value = this_to_string_coerce(builtin.this_value)
      let form = if args.is_empty() || args[0] is Undefined {
        "NFC"
      } else {
        to_string_strict(args[0])
      }
      String(string_normalize_form(value, form))
    }
    BuiltinFunction::StringIsWellFormed => {
      let value = this_to_string_coerce(builtin.this_value)
      Bool(string_find_invalid_codepoint(value) < 0)
    }
    BuiltinFunction::StringToWellFormed => {
      let value = this_to_string_coerce(builtin.this_value)
      if string_find_invalid_codepoint(value) < 0 {
        String(value)
      } else {
        String(string_to_well_formed(value))
      }
    }
    BuiltinFunction::StringMatch => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let value = this_to_string_coerce(builtin.this_value)
      let search = if args.is_empty() { Undefined } else { args[0] }
      string_match_value(this_value, value, search)
    }
    BuiltinFunction::StringMatchAll => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let value = this_to_string_coerce(builtin.this_value)
      let search = if args.is_empty() { Undefined } else { args[0] }
      string_match_all_value(this_value, value, search)
    }
    BuiltinFunction::StringSearch => {
      let this_value = match builtin.this_value {
        Some(value) => value
        None => Undefined
      }
      let value = this_to_string_coerce(builtin.this_value)
      let search = if args.is_empty() { Undefined } else { args[0] }
      string_search_value(this_value, value, search)
    }
    BuiltinFunction::StringToString =>
      String(this_to_string(builtin.this_value))
    BuiltinFunction::StringValueOf => String(this_to_string(builtin.this_value))
    BuiltinFunction::StringAnchor =>
      string_create_html(builtin.this_value, args, "a", Some("name"))
    BuiltinFunction::StringBig =>
      string_create_html(builtin.this_value, args, "big", None)
    BuiltinFunction::StringBlink =>
      string_create_html(builtin.this_value, args, "blink", None)
    BuiltinFunction::StringBold =>
      string_create_html(builtin.this_value, args, "b", None)
    BuiltinFunction::StringFixed =>
      string_create_html(builtin.this_value, args, "tt", None)
    BuiltinFunction::StringFontcolor =>
      string_create_html(builtin.this_value, args, "font", Some("color"))
    BuiltinFunction::StringFontsize =>
      string_create_html(builtin.this_value, args, "font", Some("size"))
    BuiltinFunction::StringItalics =>
      string_create_html(builtin.this_value, args, "i", None)
    BuiltinFunction::StringLink =>
      string_create_html(builtin.this_value, args, "a", Some("href"))
    BuiltinFunction::StringSmall =>
      string_create_html(builtin.this_value, args, "small", None)
    BuiltinFunction::StringStrike =>
      string_create_html(builtin.this_value, args, "strike", None)
    BuiltinFunction::StringSub =>
      string_create_html(builtin.this_value, args, "sub", None)
    BuiltinFunction::StringSup =>
      string_create_html(builtin.this_value, args, "sup", None)
    BuiltinFunction::StringIterator => {
      let text = match builtin.this_value {
        Some(Null) | Some(Undefined) | None => {
          let _ = throw_type_error("not a string")
          ""
        }
        Some(value) => to_string_strict(value)
      }
      new_string_iterator(String(text))
    }
    BuiltinFunction::StringIteratorNext =>
      string_iterator_next(builtin.this_value)
    _ => throw_type_error("invalid builtin")
  }
}
