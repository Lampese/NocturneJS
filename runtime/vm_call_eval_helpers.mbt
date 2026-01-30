///|
fn unwrap_paren(expr : @engine.Expr) -> @engine.Expr {
  match expr {
    Paren(inner) => unwrap_paren(inner)
    _ => expr
  }
}

///|
fn unwrap_paren_with_cover(expr : @engine.Expr) -> (@engine.Expr, Bool) {
  let mut target = expr
  let mut is_cover = false
  let mut keep_unwrap = true
  while keep_unwrap {
    match target {
      Paren(inner) => {
        is_cover = true
        target = inner
      }
      _ => keep_unwrap = false
    }
  }
  (target, is_cover)
}
