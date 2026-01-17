///|
pub struct Lexer {
  source : String
  chars : Array[Char]
} derive(Show)

///|
pub fn Lexer::new(source : String) -> Lexer {
  Lexer::{ source, chars: source.iter().to_array() }
}

///|
pub fn tokenize(source : String) -> Array[Token] {
  tokenize_with_options(source, true)
}

///|
pub fn tokenize_with_options(
  source : String,
  allow_html_comments : Bool,
) -> Array[Token] {
  let lexer = Lexer::new(source)
  tokenize_from(lexer, allow_html_comments)
}

///|
fn tokenize_from(lexer : Lexer, allow_html_comments : Bool) -> Array[Token] {
  let chars = lexer.chars
  let length = chars.length()
  let mut offset = 0
  let mut saw_line_break = false
  let mut line_start = true
  let mut allow_regex = true
  let template_head_stack : Array[Bool] = []
  let template_expr_depth_stack : Array[Int] = []
  let tokens : Array[Token] = []
  while offset < length {
    let ch = chars[offset]
    if offset == 0 && ch == '#' {
      match chars.get(offset + 1) {
        Some('!') => {
          offset = offset + 2
          while offset < length && !is_line_break(chars[offset]) {
            offset = offset + 1
          }
          continue
        }
        _ => ()
      }
    }
    if ch.is_whitespace() || ch == '\uFEFF' {
      if is_line_break(ch) {
        saw_line_break = true
        line_start = true
      }
      offset = offset + 1
      continue
    }
    if allow_html_comments && ch == '<' {
      match
        (chars.get(offset + 1), chars.get(offset + 2), chars.get(offset + 3)) {
        (Some('!'), Some('-'), Some('-')) => {
          offset = offset + 4
          while offset < length {
            let c = chars[offset]
            if is_line_break(c) {
              saw_line_break = true
              line_start = true
              break
            }
            offset = offset + 1
          }
          continue
        }
        _ => ()
      }
    }
    if allow_html_comments && ch == '-' {
      match (chars.get(offset + 1), chars.get(offset + 2)) {
        (Some('-'), Some('>')) =>
          if line_start {
            offset = offset + 3
            while offset < length {
              let c = chars[offset]
              if is_line_break(c) {
                saw_line_break = true
                line_start = true
                break
              }
              offset = offset + 1
            }
            continue
          }
        _ => ()
      }
    }
    if ch == '/' {
      match chars.get(offset + 1) {
        Some('/') => {
          offset = offset + 2
          while offset < length {
            let c = chars[offset]
            if is_line_break(c) {
              saw_line_break = true
              line_start = true
              break
            }
            offset = offset + 1
          }
          continue
        }
        Some('*') => {
          let line_break_before = saw_line_break
          let start = offset
          offset = offset + 2
          let mut closed = false
          while offset + 1 < length {
            let c = chars[offset]
            if is_line_break(c) {
              saw_line_break = true
              line_start = true
            }
            if c == '*' && chars[offset + 1] == '/' {
              offset = offset + 2
              closed = true
              break
            }
            offset = offset + 1
          }
          if !closed {
            tokens.push(Token::{
              kind: Error,
              lexeme: "/*",
              raw_lexeme: None,
              offset: start,
              line_break_before,
            })
            offset = length
          }
          continue
        }
        _ => ()
      }
    }
    let line_break_before = saw_line_break
    saw_line_break = false
    line_start = false
    if ch == '/' && allow_regex {
      match scan_regexp(chars, offset) {
        Some((lexeme, next_offset)) => {
          tokens.push(Token::{
            kind: Regexp,
            lexeme,
            raw_lexeme: None,
            offset,
            line_break_before,
          })
          offset = next_offset
          allow_regex = false
          continue
        }
        None => {
          tokens.push(Token::{
            kind: Error,
            lexeme: ch.to_string(),
            raw_lexeme: None,
            offset,
            line_break_before,
          })
          offset = offset + 1
          allow_regex = true
          continue
        }
      }
    }
    if ch == '"' || ch == '\'' {
      match scan_string(chars, offset) {
        Some((lexeme, next_offset, has_escape)) => {
          let raw_lexeme = if has_escape {
            Some(slice_string(chars, offset, next_offset))
          } else {
            None
          }
          tokens.push(Token::{
            kind: String,
            lexeme,
            raw_lexeme,
            offset,
            line_break_before,
          })
          offset = next_offset
          allow_regex = false
          continue
        }
        None => {
          tokens.push(Token::{
            kind: Error,
            lexeme: ch.to_string(),
            raw_lexeme: None,
            offset,
            line_break_before,
          })
          offset = offset + 1
          allow_regex = true
          continue
        }
      }
    }
    if ch == '`' {
      let line_break_before = saw_line_break
      saw_line_break = false
      line_start = false
      template_head_stack.push(true)
      let head_index = template_head_stack.length() - 1
      match scan_template_chunk(chars, offset, true) {
        Some((lexeme, raw_lexeme, next_offset, has_expr, saw_break)) => {
          let kind = if has_expr { TemplateHead } else { Template }
          tokens.push(Token::{
            kind,
            lexeme,
            raw_lexeme: Some(raw_lexeme),
            offset,
            line_break_before,
          })
          offset = next_offset
          if saw_break {
            saw_line_break = true
            line_start = true
          }
          if has_expr {
            template_head_stack[head_index] = false
            template_expr_depth_stack.push(0)
            allow_regex = true
          } else {
            ignore(template_head_stack.pop())
            allow_regex = false
          }
          continue
        }
        None => {
          tokens.push(Token::{
            kind: Error,
            lexeme: ch.to_string(),
            raw_lexeme: None,
            offset,
            line_break_before,
          })
          offset = offset + 1
          allow_regex = true
          continue
        }
      }
    }
    let is_number_start = ch.is_ascii_digit() ||
      (ch == '.' && next_is_digit(chars, offset))
    if is_number_start {
      let (lexeme, next_offset) = scan_number(chars, offset)
      let invalid_follow = match chars.get(next_offset) {
        Some(next) =>
          is_ident_start(next) || next == '\\' || next.is_ascii_digit()
        None => false
      }
      if invalid_follow {
        tokens.push(Token::{
          kind: Error,
          lexeme,
          raw_lexeme: None,
          offset,
          line_break_before,
        })
        offset = next_offset
        allow_regex = true
        continue
      }
      tokens.push(Token::{
        kind: Number,
        lexeme,
        raw_lexeme: None,
        offset,
        line_break_before,
      })
      offset = next_offset
      allow_regex = false
      continue
    }
    if ch == '#' {
      match chars.get(offset + 1) {
        Some(next) =>
          if is_ident_start(next) || next == '\\' {
            match scan_identifier(chars, offset + 1) {
              Some((lexeme, next_offset, _)) => {
                tokens.push(Token::{
                  kind: PrivateName,
                  lexeme,
                  raw_lexeme: None,
                  offset,
                  line_break_before,
                })
                offset = next_offset
                allow_regex = false
                continue
              }
              None => ()
            }
          }
        None => ()
      }
    }
    if is_ident_start(ch) || ch == '\\' {
      match scan_identifier(chars, offset) {
        Some((lexeme, next_offset, has_escape)) => {
          let kind = if has_escape {
            TokenKind::Ident
          } else {
            match keyword_kind(lexeme) {
              Some(kind) => kind
              None => TokenKind::Ident
            }
          }
          let raw_lexeme = if has_escape {
            Some(slice_string(chars, offset, next_offset))
          } else {
            None
          }
          tokens.push(Token::{
            kind,
            lexeme,
            raw_lexeme,
            offset,
            line_break_before,
          })
          offset = next_offset
          allow_regex = !token_ends_expression(kind)
          continue
        }
        None => ()
      }
    }
    match scan_punct(chars, offset, line_break_before) {
      Some((token, next_offset)) => {
        allow_regex = !token_ends_expression(token.kind)
        tokens.push(token)
        offset = next_offset
        if !template_expr_depth_stack.is_empty() {
          match token.kind {
            Punct('{') => {
              let idx = template_expr_depth_stack.length() - 1
              template_expr_depth_stack[idx] = template_expr_depth_stack[idx] +
                1
            }
            Punct('}') => {
              let idx = template_expr_depth_stack.length() - 1
              if template_expr_depth_stack[idx] == 0 {
                ignore(template_expr_depth_stack.pop())
                if !template_head_stack.is_empty() {
                  let line_break_before = saw_line_break
                  saw_line_break = false
                  line_start = false
                  let head_index = template_head_stack.length() - 1
                  let is_head = template_head_stack[head_index]
                  match scan_template_chunk(chars, offset, false) {
                    Some((lexeme, raw_lexeme, next_offset, has_expr, saw_break)) => {
                      let kind = if has_expr {
                        if is_head {
                          TemplateHead
                        } else {
                          TemplateMiddle
                        }
                      } else if is_head {
                        Template
                      } else {
                        TemplateTail
                      }
                      tokens.push(Token::{
                        kind,
                        lexeme,
                        raw_lexeme: Some(raw_lexeme),
                        offset,
                        line_break_before,
                      })
                      offset = next_offset
                      if saw_break {
                        saw_line_break = true
                        line_start = true
                      }
                      if has_expr {
                        template_head_stack[head_index] = false
                        template_expr_depth_stack.push(0)
                        allow_regex = true
                      } else {
                        ignore(template_head_stack.pop())
                        allow_regex = false
                      }
                    }
                    None => {
                      tokens.push(Token::{
                        kind: Error,
                        lexeme: "}".to_string(),
                        raw_lexeme: None,
                        offset,
                        line_break_before,
                      })
                      offset = offset + 1
                      allow_regex = true
                    }
                  }
                }
              } else {
                template_expr_depth_stack[idx] = template_expr_depth_stack[idx] -
                  1
              }
            }
            _ => ()
          }
        }
        continue
      }
      None => {
        let lexeme = ch.to_string()
        tokens.push(Token::{
          kind: Error,
          lexeme,
          raw_lexeme: None,
          offset,
          line_break_before,
        })
        offset = offset + 1
        allow_regex = true
        continue
      }
    }
  }
  tokens.push(Token::eof(offset, saw_line_break))
  tokens
}

///|
fn scan_punct(
  chars : Array[Char],
  offset : Int,
  line_break_before : Bool,
) -> (Token, Int)? {
  let ch = chars[offset]
  let next = chars.get(offset + 1)
  let next2 = chars.get(offset + 2)
  let next3 = chars.get(offset + 3)
  let start = offset
  if ch == '=' {
    match next {
      Some('=') =>
        match next2 {
          Some('=') => {
            let lexeme = "==="
            return Some(
              (
                Token::{
                  kind: StrictEq,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "=="
            return Some(
              (
                Token::{
                  kind: Eq,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('>') => {
        let lexeme = "=>"
        return Some(
          (
            Token::{
              kind: Arrow,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "="
        return Some(
          (
            Token::{
              kind: Punct('='),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '!' {
    match next {
      Some('=') =>
        match next2 {
          Some('=') => {
            let lexeme = "!=="
            return Some(
              (
                Token::{
                  kind: StrictNeq,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "!="
            return Some(
              (
                Token::{
                  kind: Neq,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      _ => {
        let lexeme = "!"
        return Some(
          (
            Token::{
              kind: Punct('!'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '<' {
    match next {
      Some('<') =>
        match next2 {
          Some('=') => {
            let lexeme = "<<="
            return Some(
              (
                Token::{
                  kind: ShlAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "<<"
            return Some(
              (
                Token::{
                  kind: Shl,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('=') => {
        let lexeme = "<="
        return Some(
          (
            Token::{
              kind: Lte,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "<"
        return Some(
          (
            Token::{
              kind: Lt,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '>' {
    match next {
      Some('>') =>
        match next2 {
          Some('>') =>
            match next3 {
              Some('=') => {
                let lexeme = ">>>="
                return Some(
                  (
                    Token::{
                      kind: ShrAssign,
                      lexeme,
                      raw_lexeme: None,
                      offset: start,
                      line_break_before,
                    },
                    offset + 4,
                  ),
                )
              }
              _ => {
                let lexeme = ">>>"
                return Some(
                  (
                    Token::{
                      kind: Shr,
                      lexeme,
                      raw_lexeme: None,
                      offset: start,
                      line_break_before,
                    },
                    offset + 3,
                  ),
                )
              }
            }
          Some('=') => {
            let lexeme = ">>="
            return Some(
              (
                Token::{
                  kind: SarAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = ">>"
            return Some(
              (
                Token::{
                  kind: Sar,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('=') => {
        let lexeme = ">="
        return Some(
          (
            Token::{
              kind: Gte,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = ">"
        return Some(
          (
            Token::{
              kind: Gt,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '&' {
    match next {
      Some('&') =>
        match next2 {
          Some('=') => {
            let lexeme = "&&="
            return Some(
              (
                Token::{
                  kind: LandAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "&&"
            return Some(
              (
                Token::{
                  kind: Land,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('=') => {
        let lexeme = "&="
        return Some(
          (
            Token::{
              kind: AndAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "&"
        return Some(
          (
            Token::{
              kind: Punct('&'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '|' {
    match next {
      Some('|') =>
        match next2 {
          Some('=') => {
            let lexeme = "||="
            return Some(
              (
                Token::{
                  kind: LorAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "||"
            return Some(
              (
                Token::{
                  kind: Lor,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('=') => {
        let lexeme = "|="
        return Some(
          (
            Token::{
              kind: OrAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "|"
        return Some(
          (
            Token::{
              kind: Punct('|'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '+' {
    match next {
      Some('+') => {
        let lexeme = "++"
        return Some(
          (
            Token::{
              kind: Inc,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      Some('=') => {
        let lexeme = "+="
        return Some(
          (
            Token::{
              kind: PlusAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "+"
        return Some(
          (
            Token::{
              kind: Punct('+'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '-' {
    match next {
      Some('-') => {
        let lexeme = "--"
        return Some(
          (
            Token::{
              kind: Dec,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      Some('=') => {
        let lexeme = "-="
        return Some(
          (
            Token::{
              kind: MinusAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "-"
        return Some(
          (
            Token::{
              kind: Punct('-'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '*' {
    match next {
      Some('*') =>
        match next2 {
          Some('=') => {
            let lexeme = "**="
            return Some(
              (
                Token::{
                  kind: PowAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "**"
            return Some(
              (
                Token::{
                  kind: Pow,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('=') => {
        let lexeme = "*="
        return Some(
          (
            Token::{
              kind: MulAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "*"
        return Some(
          (
            Token::{
              kind: Punct('*'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '/' {
    match next {
      Some('=') => {
        let lexeme = "/="
        return Some(
          (
            Token::{
              kind: DivAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "/"
        return Some(
          (
            Token::{
              kind: Punct('/'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '%' {
    match next {
      Some('=') => {
        let lexeme = "%="
        return Some(
          (
            Token::{
              kind: ModAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "%"
        return Some(
          (
            Token::{
              kind: Punct('%'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '^' {
    match next {
      Some('=') => {
        let lexeme = "^="
        return Some(
          (
            Token::{
              kind: XorAssign,
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 2,
          ),
        )
      }
      _ => {
        let lexeme = "^"
        return Some(
          (
            Token::{
              kind: Punct('^'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  if ch == '.' {
    match next {
      Some('.') =>
        match next2 {
          Some('.') => {
            let lexeme = "..."
            return Some(
              (
                Token::{
                  kind: Ellipsis,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => ()
        }
      _ => ()
    }
    let lexeme = "."
    return Some(
      (
        Token::{
          kind: Punct('.'),
          lexeme,
          raw_lexeme: None,
          offset: start,
          line_break_before,
        },
        offset + 1,
      ),
    )
  }
  if ch == '?' {
    match next {
      Some('?') =>
        match next2 {
          Some('=') => {
            let lexeme = "??="
            return Some(
              (
                Token::{
                  kind: DoubleQuestionMarkAssign,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 3,
              ),
            )
          }
          _ => {
            let lexeme = "??"
            return Some(
              (
                Token::{
                  kind: DoubleQuestionMark,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      Some('.') =>
        match next2 {
          Some(ch) if ch.is_ascii_digit() => {
            let lexeme = "?"
            return Some(
              (
                Token::{
                  kind: Punct('?'),
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 1,
              ),
            )
          }
          _ => {
            let lexeme = "?."
            return Some(
              (
                Token::{
                  kind: QuestionMarkDot,
                  lexeme,
                  raw_lexeme: None,
                  offset: start,
                  line_break_before,
                },
                offset + 2,
              ),
            )
          }
        }
      _ => {
        let lexeme = "?"
        return Some(
          (
            Token::{
              kind: Punct('?'),
              lexeme,
              raw_lexeme: None,
              offset: start,
              line_break_before,
            },
            offset + 1,
          ),
        )
      }
    }
  }
  let lexeme = ch.to_string()
  Some(
    (
      Token::{
        kind: Punct(ch),
        lexeme,
        raw_lexeme: None,
        offset: start,
        line_break_before,
      },
      offset + 1,
    ),
  )
}

///|
fn is_line_break(ch : Char) -> Bool {
  ch == '\n' || ch == '\r' || ch == '\u2028' || ch == '\u2029'
}

///|
fn is_string_line_break(ch : Char) -> Bool {
  ch == '\n' || ch == '\r'
}

///|
fn is_ident_start(ch : Char) -> Bool {
  if ch.is_ascii() {
    ch.is_ascii_alphabetic() || ch == '_' || ch == '$'
  } else {
    unicode_is_ident_start(ch.to_int())
  }
}

///|
fn is_ident_continue(ch : Char) -> Bool {
  if ch == '\u200C' || ch == '\u200D' {
    return true
  }
  if ch.is_ascii() {
    is_ident_start(ch) || ch.is_ascii_digit()
  } else {
    unicode_is_ident_continue(ch.to_int())
  }
}

///|
fn token_ends_expression(kind : TokenKind) -> Bool {
  match kind {
    Number
    | String
    | Template
    | TemplateTail
    | Ident
    | PrivateName
    | Regexp
    | KeywordTrue
    | KeywordFalse
    | KeywordNull
    | KeywordThis
    | KeywordSuper
    | KeywordAwait
    | KeywordYield
    | Inc
    | Dec => true
    Punct(')') | Punct(']') | Punct('}') => true
    _ => false
  }
}

///|
fn next_is_digit(chars : Array[Char], offset : Int) -> Bool {
  match chars.get(offset + 1) {
    Some(next) => next.is_ascii_digit()
    None => false
  }
}

///|
fn slice_string(chars : Array[Char], start : Int, end : Int) -> String {
  String::from_array(chars.sub(start~, end~))
}

///|
fn hex_digit_value(ch : Char) -> Int? {
  if ch.is_ascii_digit() {
    Some(ch.to_int() - '0'.to_int())
  } else if ch >= 'A' && ch <= 'F' {
    Some(ch.to_int() - 'A'.to_int() + 10)
  } else if ch >= 'a' && ch <= 'f' {
    Some(ch.to_int() - 'a'.to_int() + 10)
  } else {
    None
  }
}

///|
fn parse_unicode_escape(chars : Array[Char], offset : Int) -> (Int, Int)? {
  let len = chars.length()
  if offset + 1 >= len {
    return None
  }
  if chars[offset] != '\\' || chars[offset + 1] != 'u' {
    return None
  }
  if offset + 2 < len && chars[offset + 2] == '{' {
    let mut value : Int64 = 0
    let mut found = false
    let mut i = offset + 3
    let max_value = Int64::from_int(0x10ffff)
    while i < len {
      let ch = chars[i]
      if ch == '}' {
        if !found || value > max_value {
          return None
        }
        return Some((Int64::to_int(value), i + 1))
      }
      match hex_digit_value(ch) {
        Some(digit) => {
          value = value * 16 + Int64::from_int(digit)
          if value > max_value {
            return None
          }
          found = true
        }
        None => return None
      }
      i = i + 1
    }
    None
  } else {
    if offset + 5 >= len {
      return None
    }
    let mut value = 0
    let mut i = offset + 2
    let mut count = 0
    while count < 4 {
      match hex_digit_value(chars[i]) {
        Some(digit) => value = value * 16 + digit
        None => return None
      }
      i = i + 1
      count = count + 1
    }
    Some((value, offset + 6))
  }
}

///|
fn scan_identifier(chars : Array[Char], offset : Int) -> (String, Int, Bool)? {
  let len = chars.length()
  let sb = StringBuilder::new()
  let mut i = offset
  let mut first = true
  let mut used_escape = false
  while i < len {
    let ch = chars[i]
    if ch == '\\' {
      match parse_unicode_escape(chars, i) {
        Some((value, next_i)) => {
          if value > 0x10ffff {
            return None
          }
          let ident_ch = Int::unsafe_to_char(value)
          if first {
            if !is_ident_start(ident_ch) {
              return None
            }
          } else if !is_ident_continue(ident_ch) {
            return None
          }
          sb.write_char(ident_ch)
          i = next_i
          first = false
          used_escape = true
          continue
        }
        None => if first { return None } else { break }
      }
    } else {
      if first {
        if !is_ident_start(ch) {
          return None
        }
      } else if !is_ident_continue(ch) {
        break
      }
      sb.write_char(ch)
      i = i + 1
      first = false
      continue
    }
  }
  if first {
    None
  } else {
    Some((sb.to_string(), i, used_escape))
  }
}

///|
fn scan_string(chars : Array[Char], offset : Int) -> (String, Int, Bool)? {
  let length = chars.length()
  let quote = chars[offset]
  let mut i = offset + 1
  let units : Array[Int] = []
  let mut has_escape = false
  while i < length {
    let c = chars[i]
    if c == quote {
      return Some((string_from_code_units_lossy(units), i + 1, has_escape))
    }
    if is_string_line_break(c) {
      return None
    }
    if c == '\\' {
      has_escape = true
      match decode_escape_units(chars, i, true) {
        Some((escaped, next_i)) => {
          for unit in escaped {
            units.push(unit)
          }
          i = next_i
          continue
        }
        None => return None
      }
    }
    append_codepoint_units(units, c.to_int())
    i = i + 1
  }
  None
}

///|
fn scan_template_chunk(
  chars : Array[Char],
  offset : Int,
  start_is_backtick : Bool,
) -> (String, String, Int, Bool, Bool)? {
  let length = chars.length()
  let mut i = offset
  if start_is_backtick {
    if i >= length || chars[i] != '`' {
      return None
    }
    i = i + 1
  }
  let units : Array[Int] = []
  let raw_units : Array[Int] = []
  let mut saw_line_break = false
  while i < length {
    let c = chars[i]
    if c == '`' {
      let raw = string_from_code_units_lossy(raw_units)
      return Some(
        (string_from_code_units_lossy(units), raw, i + 1, false, saw_line_break),
      )
    }
    if c == '$' && i + 1 < length && chars[i + 1] == '{' {
      let raw = string_from_code_units_lossy(raw_units)
      return Some(
        (string_from_code_units_lossy(units), raw, i + 2, true, saw_line_break),
      )
    }
    if c == '\\' {
      append_codepoint_units(raw_units, c.to_int())
      if i + 1 >= length {
        return None
      }
      let next = chars[i + 1]
      if next == '\n' {
        saw_line_break = true
        append_codepoint_units(raw_units, '\n'.to_int())
        append_codepoint_units(units, '\n'.to_int())
        i = i + 2
        continue
      }
      if next == '\r' {
        saw_line_break = true
        append_codepoint_units(raw_units, '\n'.to_int())
        append_codepoint_units(units, '\n'.to_int())
        if i + 2 < length && chars[i + 2] == '\n' {
          i = i + 3
        } else {
          i = i + 2
        }
        continue
      }
      if next == '\u2028' || next == '\u2029' {
        saw_line_break = true
        append_codepoint_units(raw_units, next.to_int())
        append_codepoint_units(units, next.to_int())
        i = i + 2
        continue
      }
      append_codepoint_units(raw_units, next.to_int())
      append_codepoint_units(units, next.to_int())
      i = i + 2
      continue
    }
    if c == '\r' {
      saw_line_break = true
      append_codepoint_units(units, '\n'.to_int())
      append_codepoint_units(raw_units, '\n'.to_int())
      if i + 1 < length && chars[i + 1] == '\n' {
        i = i + 2
      } else {
        i = i + 1
      }
      continue
    }
    if c == '\n' {
      saw_line_break = true
      append_codepoint_units(units, '\n'.to_int())
      append_codepoint_units(raw_units, '\n'.to_int())
      i = i + 1
      continue
    }
    if c == '\u2028' || c == '\u2029' {
      saw_line_break = true
      append_codepoint_units(units, c.to_int())
      append_codepoint_units(raw_units, c.to_int())
      i = i + 1
      continue
    }
    append_codepoint_units(units, c.to_int())
    append_codepoint_units(raw_units, c.to_int())
    i = i + 1
  }
  None
}

///|
fn scan_regexp(chars : Array[Char], offset : Int) -> (String, Int)? {
  let length = chars.length()
  let mut i = offset + 1
  let mut in_class = false
  let buf : Array[Char] = []
  while i < length {
    let c = chars[i]
    if is_line_break(c) {
      return None
    }
    if c == '\\' {
      match chars.get(i + 1) {
        Some(next) => {
          if is_line_break(next) {
            return None
          }
          buf.push(c)
          buf.push(next)
          i = i + 2
          continue
        }
        None => return None
      }
    }
    if c == '[' {
      in_class = true
      buf.push(c)
      i = i + 1
      continue
    }
    if c == ']' && in_class {
      in_class = false
      buf.push(c)
      i = i + 1
      continue
    }
    if c == '/' && !in_class {
      let mut j = i + 1
      let flags : Array[Char] = []
      while j < length && is_regex_flag(chars[j]) {
        flags.push(chars[j])
        j = j + 1
      }
      let pattern = String::from_iter(buf.iter())
      let flag_text = String::from_iter(flags.iter())
      let lexeme = pattern + "\n" + flag_text
      return Some((lexeme, j))
    }
    buf.push(c)
    i = i + 1
  }
  None
}

///|
pub fn scan_regexp_from(chars : Array[Char], offset : Int) -> (String, Int)? {
  scan_regexp(chars, offset)
}

///|
fn is_regex_flag(ch : Char) -> Bool {
  ch.is_ascii_alphabetic()
}

///|
fn decode_escape_units(
  chars : Array[Char],
  offset : Int,
  allow_legacy_octal : Bool,
) -> (Array[Int], Int)? {
  let length = chars.length()
  if offset + 1 >= length {
    return None
  }
  let ch = chars[offset + 1]
  match ch {
    '\n' => Some(([], offset + 2))
    '\r' => {
      let mut next = offset + 2
      if next < length && chars[next] == '\n' {
        next = next + 1
      }
      Some(([], next))
    }
    '\u2028' => Some(([], offset + 2))
    '\u2029' => Some(([], offset + 2))
    'n' => Some(([0x0a], offset + 2))
    'r' => Some(([0x0d], offset + 2))
    't' => Some(([0x09], offset + 2))
    'b' => Some(([0x08], offset + 2))
    'f' => Some(([0x0c], offset + 2))
    'v' => Some(([0x0b], offset + 2))
    '\\' => Some(([0x5c], offset + 2))
    '\'' => Some(([0x27], offset + 2))
    '"' => Some(([0x22], offset + 2))
    '`' => Some(([0x60], offset + 2))
    '0' =>
      if offset + 2 < length && chars[offset + 2].is_ascii_digit() {
        if !allow_legacy_octal {
          return None
        }
        let (value, next_i) = parse_legacy_octal_escape(chars, offset + 1)
        Some(([value], next_i))
      } else {
        Some(([0x00], offset + 2))
      }
    '1' | '2' | '3' | '4' | '5' | '6' | '7' => {
      if !allow_legacy_octal {
        return None
      }
      let (value, next_i) = parse_legacy_octal_escape(chars, offset + 1)
      Some(([value], next_i))
    }
    '8' | '9' =>
      if !allow_legacy_octal {
        None
      } else {
        Some(([ch.to_int()], offset + 2))
      }
    'x' => {
      if offset + 3 < length {
        match (hex_value(chars[offset + 2]), hex_value(chars[offset + 3])) {
          (Some(a), Some(b)) => {
            let code = (a << 4) | b
            return Some(([code], offset + 4))
          }
          _ => ()
        }
      }
      None
    }
    'u' => {
      if offset + 2 < length && chars[offset + 2] == '{' {
        let mut value : Int64 = 0
        let mut j = offset + 3
        let mut any = false
        let max_value = Int64::from_int(0x10ffff)
        while j < length && chars[j] != '}' {
          match hex_value(chars[j]) {
            Some(digit) => {
              value = (value << 4) | Int64::from_int(digit)
              if value > max_value {
                return None
              }
              any = true
              j = j + 1
            }
            None => break
          }
        }
        if any && value <= max_value && j < length && chars[j] == '}' {
          let units = codepoint_to_units(Int64::to_int(value))
          return Some((units, j + 1))
        }
      } else if offset + 5 < length {
        match
          (
            hex_value(chars[offset + 2]),
            hex_value(chars[offset + 3]),
            hex_value(chars[offset + 4]),
            hex_value(chars[offset + 5]),
          ) {
          (Some(a), Some(b), Some(c), Some(d)) => {
            let code = (a << 12) | (b << 8) | (c << 4) | d
            return Some(([code], offset + 6))
          }
          _ => ()
        }
      }
      None
    }
    _ => Some(([ch.to_int()], offset + 2))
  }
}

///|
pub fn cook_template_raw(raw : String) -> String? {
  let chars = raw.iter().to_array()
  let units : Array[Int] = []
  let mut i = 0
  while i < chars.length() {
    let ch = chars[i]
    if ch == '\\' {
      match decode_escape_units(chars, i, false) {
        Some((escaped, next_i)) => {
          for unit in escaped {
            units.push(unit)
          }
          i = next_i
        }
        None => return None
      }
    } else {
      append_codepoint_units(units, ch.to_int())
      i = i + 1
    }
  }
  Some(string_from_code_units_lossy(units))
}

///|
fn parse_legacy_octal_escape(chars : Array[Char], start : Int) -> (Int, Int) {
  let length = chars.length()
  let mut value = chars[start].to_int() - '0'.to_int()
  let mut i = start + 1
  if i < length && is_oct_digit(chars[i]) {
    value = (value << 3) | (chars[i].to_int() - '0'.to_int())
    i = i + 1
    if value < 32 && i < length && is_oct_digit(chars[i]) {
      value = (value << 3) | (chars[i].to_int() - '0'.to_int())
      i = i + 1
    }
  }
  (value, i)
}

///|
fn hex_value(ch : Char) -> Int? {
  if ch.is_ascii_digit() {
    Some(ch.to_int() - '0'.to_int())
  } else if ch >= 'a' && ch <= 'f' {
    Some(10 + ch.to_int() - 'a'.to_int())
  } else if ch >= 'A' && ch <= 'F' {
    Some(10 + ch.to_int() - 'A'.to_int())
  } else {
    None
  }
}

///|
fn append_codepoint_units(units : Array[Int], code : Int) -> Unit {
  for unit in codepoint_to_units(code) {
    units.push(unit)
  }
}

///|
fn codepoint_to_units(code : Int) -> Array[Int] {
  let result : Array[Int] = []
  if code <= 0xffff {
    result.push(code)
  } else if code <= 0x10ffff {
    let value = code - 0x10000
    let high = 0xd800 + (value >> 10)
    let low = 0xdc00 + (value & 0x3ff)
    result.push(high)
    result.push(low)
  }
  result
}

///|
fn string_from_code_units_lossy(units : Array[Int]) -> String {
  if units.is_empty() {
    return ""
  }
  let sb = StringBuilder::new(size_hint=units.length())
  for unit in units {
    sb.write_char(UInt16::unsafe_to_char(Int::to_uint16(unit)))
  }
  sb.to_string()
}

///|
fn scan_number(chars : Array[Char], offset : Int) -> (String, Int) {
  let length = chars.length()
  let mut i = offset
  if chars[i] == '0' {
    match chars.get(i + 1) {
      Some('x') | Some('X') => {
        i = i + 2
        while i < length && (is_hex_digit(chars[i]) || chars[i] == '_') {
          i = i + 1
        }
        if i < length && chars[i] == 'n' {
          i = i + 1
        }
        return (slice_string(chars, offset, i), i)
      }
      Some('o') | Some('O') => {
        i = i + 2
        while i < length && (is_oct_digit(chars[i]) || chars[i] == '_') {
          i = i + 1
        }
        if i < length && chars[i] == 'n' {
          i = i + 1
        }
        return (slice_string(chars, offset, i), i)
      }
      Some('b') | Some('B') => {
        i = i + 2
        while i < length && (is_bin_digit(chars[i]) || chars[i] == '_') {
          i = i + 1
        }
        if i < length && chars[i] == 'n' {
          i = i + 1
        }
        return (slice_string(chars, offset, i), i)
      }
      _ => ()
    }
  }
  let mut has_dot = false
  let mut has_exp = false
  if chars[i] == '.' {
    has_dot = true
    i = i + 1
    while i < length && (chars[i].is_ascii_digit() || chars[i] == '_') {
      i = i + 1
    }
  } else {
    i = i + 1
    while i < length && (chars[i].is_ascii_digit() || chars[i] == '_') {
      i = i + 1
    }
    if i < length && chars[i] == '.' {
      has_dot = true
      i = i + 1
      while i < length && (chars[i].is_ascii_digit() || chars[i] == '_') {
        i = i + 1
      }
    }
  }
  if i < length && (chars[i] == 'e' || chars[i] == 'E') {
    has_exp = true
    i = i + 1
    match chars.get(i) {
      Some('+') | Some('-') => i = i + 1
      _ => ()
    }
    while i < length && (chars[i].is_ascii_digit() || chars[i] == '_') {
      i = i + 1
    }
  }
  if i < length && chars[i] == 'n' && !has_dot && !has_exp {
    i = i + 1
  }
  (slice_string(chars, offset, i), i)
}

///|
fn is_hex_digit(ch : Char) -> Bool {
  if ch.is_ascii_digit() {
    true
  } else if ch >= 'a' && ch <= 'f' {
    true
  } else if ch >= 'A' && ch <= 'F' {
    true
  } else {
    false
  }
}

///|
fn is_oct_digit(ch : Char) -> Bool {
  ch >= '0' && ch <= '7'
}

///|
fn is_bin_digit(ch : Char) -> Bool {
  ch == '0' || ch == '1'
}

///|
fn keyword_kind(ident : String) -> TokenKind? {
  match ident {
    "null" => Some(KeywordNull)
    "false" => Some(KeywordFalse)
    "true" => Some(KeywordTrue)
    "if" => Some(KeywordIf)
    "else" => Some(KeywordElse)
    "return" => Some(KeywordReturn)
    "var" => Some(KeywordVar)
    "this" => Some(KeywordThis)
    "delete" => Some(KeywordDelete)
    "void" => Some(KeywordVoid)
    "typeof" => Some(KeywordTypeof)
    "new" => Some(KeywordNew)
    "in" => Some(KeywordIn)
    "instanceof" => Some(KeywordInstanceof)
    "do" => Some(KeywordDo)
    "while" => Some(KeywordWhile)
    "for" => Some(KeywordFor)
    "break" => Some(KeywordBreak)
    "continue" => Some(KeywordContinue)
    "switch" => Some(KeywordSwitch)
    "case" => Some(KeywordCase)
    "default" => Some(KeywordDefault)
    "throw" => Some(KeywordThrow)
    "try" => Some(KeywordTry)
    "catch" => Some(KeywordCatch)
    "finally" => Some(KeywordFinally)
    "function" => Some(KeywordFunction)
    "debugger" => Some(KeywordDebugger)
    "with" => Some(KeywordWith)
    "class" => Some(KeywordClass)
    "const" => Some(KeywordConst)
    "enum" => Some(KeywordEnum)
    "export" => Some(KeywordExport)
    "extends" => Some(KeywordExtends)
    "import" => Some(KeywordImport)
    "super" => Some(KeywordSuper)
    "let" => Some(KeywordLet)
    "static" => Some(KeywordStatic)
    "yield" => Some(KeywordYield)
    "await" => Some(KeywordAwait)
    _ => None
  }
}
