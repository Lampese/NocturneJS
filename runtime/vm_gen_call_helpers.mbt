///|
fn gen_eval_member_key(
  env : Env,
  key : @engine.MemberKey,
  k : (String) -> GenStep raise,
) -> GenStep raise {
  match key {
    @engine.MemberKey::Ident(name) => k(name)
    @engine.MemberKey::Private(name) => k(env_private_key_checked(env, name))
    @engine.MemberKey::Computed(expr) =>
      gen_eval_expr_cont(env, expr, fn(value) raise {
        let _ = gen_root_push(value)
        k(property_key_name(value))
      })
  }
}
