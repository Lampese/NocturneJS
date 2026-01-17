///|
fn print_usage() -> Unit {
  println("usage: njs <path>")
}

///|
async fn main {
  let args = @env.args()
  if args.length() < 2 {
    print_usage()
    return
  }
  let path = args[1]
  let source = @fs.read_file(path).text() catch {
    err => {
      println("error: \{err}")
      return
    }
  }
  let engine = @qjs.Engine::new()
  let result = engine.eval_result_with_path(source, path) catch {
    err => {
      println("error: \{err}")
      return
    }
  }
  match result {
    Ok(_) => ()
    Err(error_value) => println("error: \{engine.format_error(error_value)}")
  }
}
