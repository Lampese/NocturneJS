///|
fn offset_to_line_column(source : String, offset : Int) -> (Int, Int) {
  let len = source.length()
  let end = if offset < len { offset } else { len }
  let mut line = 1
  let mut column = 1
  let mut i = 0
  while i < end {
    let unit = UInt16::to_int(source.code_unit_at(i))
    if unit == Char::to_int('\n') {
      line = line + 1
      column = 1
    } else {
      column = column + 1
    }
    i = i + 1
  }
  (line, column)
}

///|
fn syntax_error_proto_for_parse() -> ObjectValue? {
  match current_env() {
    Some(env) =>
      match syntax_error_proto_for_env(env) {
        Some(proto) => Some(proto)
        None => syntax_error_proto()
      }
    None => syntax_error_proto()
  }
}

///|
fn parse_script_with_pos(source : String) -> @engine.Script raise {
  try @engine.parse(source) catch {
    err =>
      match err {
        @engine.ParseError::At(offset~, message~) => {
          let (line, column) = offset_to_line_column(source, offset)
          raise ThrowSignal(
            new_error_value_with_pos(
              syntax_error_proto_for_parse(),
              message,
              line,
              column,
            ),
          )
        }
        _ => raise err
      }
  } noraise {
    script => {
      validate_regexp_in_script(script)
      validate_context_in_script(script, false, false, false)
      script
    }
  }
}

///|
fn parse_script_with_pos_mode(
  source : String,
  force_module : Bool,
  force_strict : Bool,
) -> @engine.Script raise {
  try @engine.parse_with_mode(source, force_module, force_strict) catch {
    err =>
      match err {
        @engine.ParseError::At(offset~, message~) => {
          let (line, column) = offset_to_line_column(source, offset)
          raise ThrowSignal(
            new_error_value_with_pos(
              syntax_error_proto_for_parse(),
              message,
              line,
              column,
            ),
          )
        }
        _ => raise err
      }
  } noraise {
    script => {
      validate_regexp_in_script(script)
      if !force_module && script.is_module {
        let _ = throw_syntax_error("unexpected token")

      }
      script
    }
  }
}

///|
fn parse_script_with_pos_mode_private(
  source : String,
  force_module : Bool,
  force_strict : Bool,
  private_names : Array[String],
) -> @engine.Script raise {
  try
    @engine.parse_with_mode_private(
      source, force_module, force_strict, private_names,
    )
  catch {
    err =>
      match err {
        @engine.ParseError::At(offset~, message~) => {
          let (line, column) = offset_to_line_column(source, offset)
          raise ThrowSignal(
            new_error_value_with_pos(
              syntax_error_proto_for_parse(),
              message,
              line,
              column,
            ),
          )
        }
        _ => raise err
      }
  } noraise {
    script => {
      validate_regexp_in_script(script)
      if !force_module && script.is_module {
        let _ = throw_syntax_error("unexpected token")

      }
      script
    }
  }
}
