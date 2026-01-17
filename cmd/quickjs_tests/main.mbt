///|
fn parse_negative_type(source : String) -> String? {
  let start = source.find("/*---")
  let end = source.find("---*/")
  match (start, end) {
    (Some(start_i), Some(end_i)) => {
      if end_i <= start_i {
        return None
      }
      let block = source.unsafe_substring(start=start_i + 5, end=end_i)
      let lines = block.split("\n")
      let mut in_negative = false
      for line in lines {
        let trimmed_view = line.trim()
        let trimmed = trimmed_view.to_string()
        if trimmed.is_empty() {
          continue
        }
        if trimmed == "negative:" {
          in_negative = true
          continue
        }
        if in_negative {
          if !line.has_prefix(" ") {
            break
          }
          if trimmed.has_prefix("type:") {
            let value = trimmed
              .unsafe_substring(start=5, end=trimmed.length())
              .trim()
              .to_string()
            if value.is_empty() {
              return None
            }
            return Some(value)
          }
        }
      }
      None
    }
    _ => None
  }
}

///|
fn is_worker_module(source : String) -> Bool {
  source.contains("Worker code for test_worker.js")
}

///|
fn escape_js_string(text : String) -> String {
  let sb = StringBuilder::new()
  for ch in text {
    match ch {
      '\\' => sb.write_string("\\\\")
      '"' => sb.write_string("\\\"")
      '\n' => sb.write_string("\\n")
      '\r' => sb.write_string("\\r")
      '\t' => sb.write_string("\\t")
      _ => sb.write_char(ch)
    }
  }
  sb.to_string()
}

///|
async fn main {
  let args = @env.args()
  if args.length() < 2 {
    println("usage: quickjs_tests <path>")
    return
  }
  let path = args[1]
  let source = @fs.read_file(path).text()
  let engine = @qjs.Engine::new()
  if is_worker_module(source) {
    let worker_path = match @env.current_dir() {
      Some(cwd) => if path.has_prefix("/") { path } else { cwd + "/" + path }
      None => path
    }
    let escaped = escape_js_string(worker_path)
    let wrapper = "import * as os from \"os\";\n" +
      "new os.Worker(\"" +
      escaped +
      "\");"
    let wrapper_path = worker_path + ".runner"
    let result = engine.eval_result_with_path(wrapper, wrapper_path) catch {
      err => {
        println("error: \{err}")
        return
      }
    }
    match result {
      Ok(value) => println("ok: \{value}")
      Err(error_value) => println("error: \{engine.format_error(error_value)}")
    }
    return
  }
  let expected_error = parse_negative_type(source)
  let result = engine.eval_result_with_path(source, path) catch {
    err => {
      println("error: \{err}")
      return
    }
  }
  match result {
    Ok(value) =>
      match expected_error {
        Some(expected) =>
          println("error: expected \{expected} but got ok: \{value}")
        None => println("ok: \{value}")
      }
    Err(error_value) =>
      match expected_error {
        Some(expected) =>
          match engine.error_name(error_value) {
            Some(actual) =>
              if actual == expected {
                println("ok: expected \{expected}")
              } else {
                println("error: expected \{expected} but got \{actual}")
              }
            None =>
              println(
                "error: expected \{expected} but got \{engine.format_error(error_value)}",
              )
          }
        None => println("error: \{engine.format_error(error_value)}")
      }
  }
}
