///|
fn bytecode_emit_throw(
  builder : BytecodeBuilder,
  op : BytecodeOp,
  message : String,
) -> Unit {
  let idx = bytecode_add_const(builder, String(message))
  bytecode_emit(builder, BytecodeInstr::Const(op, idx))
}

///|
fn bytecode_emit_expr(
  builder : BytecodeBuilder,
  expr : @engine.Expr,
) -> Bool {
  fn bytecode_emit_args(
    builder : BytecodeBuilder,
    args : Array[@engine.Expr],
  ) -> Bool {
    for arg in args {
      match arg {
        @engine.Expr::Spread(_) => return false
        _ =>
          if !bytecode_emit_expr(builder, arg) {
            return false
          }
      }
    }
    true
  }
  fn args_have_spread(args : Array[@engine.Expr]) -> Bool {
    for arg in args {
      match arg {
        @engine.Expr::Spread(_) => return true
        _ => ()
      }
    }
    false
  }
  fn bytecode_emit_args_array(
    builder : BytecodeBuilder,
    args : Array[@engine.Expr],
  ) -> Bool {
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::NewArray, 0))
    for arg in args {
      match arg {
        @engine.Expr::Spread(expr) => {
          if !bytecode_emit_expr(builder, expr) {
            return false
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArraySpread))
        }
        _ => {
          if !bytecode_emit_expr(builder, arg) {
            return false
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArrayPush))
        }
      }
    }
    true
  }
  fn export_name_for_args(args : Array[@engine.Expr]) -> String? {
    if args.is_empty() {
      None
    } else {
      match args[0] {
        @engine.Expr::String(name, _, _) => Some(name)
        _ => None
      }
    }
  }
  fn bytecode_emit_export_args(
    builder : BytecodeBuilder,
    args : Array[@engine.Expr],
  ) -> Bool {
    let export_name = export_name_for_args(args)
    let mut i = 0
    while i < args.length() {
      let arg = args[i]
      match arg {
        @engine.Expr::Spread(_) => return false
        _ =>
          if !bytecode_emit_expr(builder, arg) {
            return false
          }
      }
      if i == 1 {
        match export_name {
          Some(name) =>
            if is_anonymous_function_definition(arg) {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
              )
            }
          None => ()
        }
      }
      i = i + 1
    }
    true
  }
  fn bytecode_emit_export_args_array(
    builder : BytecodeBuilder,
    args : Array[@engine.Expr],
  ) -> Bool {
    let export_name = export_name_for_args(args)
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::NewArray, 0))
    let mut i = 0
    while i < args.length() {
      match args[i] {
        @engine.Expr::Spread(expr) => {
          if !bytecode_emit_expr(builder, expr) {
            return false
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArraySpread))
        }
        _ => {
          if !bytecode_emit_expr(builder, args[i]) {
            return false
          }
          if i == 1 {
            match export_name {
              Some(name) =>
                if is_anonymous_function_definition(args[i]) {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
                  )
                }
              None => ()
            }
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArrayPush))
        }
      }
      i = i + 1
    }
    true
  }
  letrec bytecode_emit_optional_chain_base = fn(
    builder : BytecodeBuilder,
    base : @engine.Expr,
  ) -> Bool {
    match unwrap_paren(base) {
      @engine.Expr::Member(obj_expr, key, _) =>
        match unwrap_paren(obj_expr) {
          Super => {
            match key {
              @engine.MemberKey::Computed(expr) =>
                if !bytecode_emit_expr(builder, expr) {
                  return false
                } else {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SuperGetElem))
                }
              _ => {
                let idx = bytecode_add_super_member(builder, key)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SuperMember, idx),
                )
              }
            }
            let this_idx = bytecode_add_const(builder, String("this"))
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::GetName, this_idx),
            )
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            true
          }
          _ =>
            match key {
              @engine.MemberKey::Ident(name) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                  )
                  true
                } else {
                  false
                }
              @engine.MemberKey::Computed(expr) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
                  true
                } else {
                  false
                }
              @engine.MemberKey::Private(name) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                  )
                  true
                } else {
                  false
                }
            }
        }
      @engine.Expr::OptionalChain(inner_base, inner_segments) =>
        bytecode_emit_optional_chain_core(builder, inner_base, inner_segments)
      _ =>
        if bytecode_emit_expr(builder, base) {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
          true
        } else {
          false
        }
    }
  }
  and bytecode_emit_optional_chain_core = fn(
    builder : BytecodeBuilder,
    base : @engine.Expr,
    segments : Array[@engine.ChainSegment],
  ) -> Bool {
    if !bytecode_emit_optional_chain_base(builder, base) {
      return false
    }
    let nullish_jumps : Array[Int] = []
    for segment in segments {
      match segment {
        @engine.ChainSegment::Member(key, optional, _) => {
          if optional {
            nullish_jumps.push(
              bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish),
            )
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
          match key {
            @engine.MemberKey::Ident(name) => {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::GetProp, idx),
              )
            }
            @engine.MemberKey::Computed(expr) => {
              if !bytecode_emit_expr(builder, expr) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
            }
            @engine.MemberKey::Private(name) => {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
              )
            }
          }
        }
        @engine.ChainSegment::Call(args, optional, _) => {
          if optional {
            nullish_jumps.push(
              bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish),
            )
          }
          if args_have_spread(args) {
            if !bytecode_emit_args_array(builder, args) {
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::CallWithThisArgs))
          } else {
            if !bytecode_emit_args(builder, args) {
              return false
            }
            bytecode_emit(
              builder,
              BytecodeInstr::I32(BytecodeOp::CallWithThis, args.length()),
            )
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        }
      }
    }
    if !nullish_jumps.is_empty() {
      let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
      let nullish_pc = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      let end_pc = bytecode_current_pc(builder)
      for idx in nullish_jumps {
        bytecode_patch_jump(builder, idx, nullish_pc)
      }
      bytecode_patch_jump(builder, end_jump, end_pc)
    }
    true
  }
  fn bytecode_emit_optional_chain_value(
    builder : BytecodeBuilder,
    base : @engine.Expr,
    segments : Array[@engine.ChainSegment],
  ) -> Bool {
    if !bytecode_emit_optional_chain_core(builder, base, segments) {
      return false
    }
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    true
  }
  fn optional_chain_has_private(
    segments : Array[@engine.ChainSegment],
  ) -> Bool {
    for segment in segments {
      match segment {
        @engine.ChainSegment::Member(@engine.MemberKey::Private(_), _, _) =>
          return true
        _ => ()
      }
    }
    false
  }
  fn bytecode_emit_delete_optional_chain(
    builder : BytecodeBuilder,
    base : @engine.Expr,
    segments : Array[@engine.ChainSegment],
  ) -> Bool {
    if segments.is_empty() {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushTrue))
      return true
    }
    if optional_chain_has_private(segments) {
      bytecode_emit_throw(
        builder,
        BytecodeOp::ThrowSyntaxError,
        "cannot delete a private class field",
      )
      return true
    }
    if !bytecode_emit_expr(builder, base) {
      return false
    }
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
    let nullish_jumps : Array[Int] = []
    let last_index = segments.length() - 1
    for index, segment in segments {
      let is_last = index == last_index
      match segment {
        @engine.ChainSegment::Member(key, optional, _) => {
          if optional {
            nullish_jumps.push(
              bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish),
            )
          }
          if is_last {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            match key {
              @engine.MemberKey::Ident(name) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::DeleteProp, idx),
                )
              }
              @engine.MemberKey::Computed(expr) => {
                if !bytecode_emit_expr(builder, expr) {
                  return false
                }
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::DeleteElem))
              }
              @engine.MemberKey::Private(_) => {
                bytecode_emit_throw(
                  builder,
                  BytecodeOp::ThrowSyntaxError,
                  "cannot delete a private class field",
                )
                return true
              }
            }
          } else {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            match key {
              @engine.MemberKey::Ident(name) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                )
              }
              @engine.MemberKey::Computed(expr) => {
                if !bytecode_emit_expr(builder, expr) {
                  return false
                }
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
              }
              @engine.MemberKey::Private(name) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                )
              }
            }
          }
        }
        @engine.ChainSegment::Call(args, optional, _) => {
          if optional {
            nullish_jumps.push(
              bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish),
            )
          }
          if args_have_spread(args) {
            if !bytecode_emit_args_array(builder, args) {
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::CallWithThisArgs))
          } else {
            if !bytecode_emit_args(builder, args) {
              return false
            }
            bytecode_emit(
              builder,
              BytecodeInstr::I32(BytecodeOp::CallWithThis, args.length()),
            )
          }
          if is_last {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushTrue))
          } else {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
          }
        }
      }
    }
    if !nullish_jumps.is_empty() {
      let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
      let nullish_pc = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushTrue))
      let end_pc = bytecode_current_pc(builder)
      for idx in nullish_jumps {
        bytecode_patch_jump(builder, idx, nullish_pc)
      }
      bytecode_patch_jump(builder, end_jump, end_pc)
    }
    true
  }
  fn bytecode_emit_tagged_template_callee(
    builder : BytecodeBuilder,
    callee : @engine.Expr,
  ) -> Bool {
    match unwrap_paren(callee) {
      @engine.Expr::Member(obj_expr, key, _) =>
        match unwrap_paren(obj_expr) {
          Super => {
            match key {
              @engine.MemberKey::Computed(expr) =>
                if !bytecode_emit_expr(builder, expr) {
                  return false
                } else {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SuperGetElem))
                }
              _ => {
                let idx = bytecode_add_super_member(builder, key)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SuperMember, idx),
                )
              }
            }
            let this_idx = bytecode_add_const(builder, String("this"))
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::GetName, this_idx),
            )
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            true
          }
          _ =>
            match key {
              @engine.MemberKey::Ident(name) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                  )
                  true
                } else {
                  false
                }
              @engine.MemberKey::Computed(expr) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
                  true
                } else {
                  false
                }
              @engine.MemberKey::Private(name) =>
                if bytecode_emit_expr(builder, obj_expr) {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                  )
                  true
                } else {
                  false
                }
            }
        }
      _ =>
        if bytecode_emit_expr(builder, callee) {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
          true
        } else {
          false
        }
    }
  }
  fn bytecode_update_name_op(
    op : @engine.UpdateOp,
    kind : @engine.UpdateKind,
  ) -> BytecodeOp {
    match (op, kind) {
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdateNamePrefixInc
      (@engine.UpdateOp::Dec, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdateNamePrefixDec
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Postfix) =>
        BytecodeOp::UpdateNamePostfixInc
      _ => BytecodeOp::UpdateNamePostfixDec
    }
  }
  fn bytecode_update_prop_op(
    op : @engine.UpdateOp,
    kind : @engine.UpdateKind,
  ) -> BytecodeOp {
    match (op, kind) {
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdatePropPrefixInc
      (@engine.UpdateOp::Dec, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdatePropPrefixDec
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Postfix) =>
        BytecodeOp::UpdatePropPostfixInc
      _ => BytecodeOp::UpdatePropPostfixDec
    }
  }
  fn bytecode_update_private_op(
    op : @engine.UpdateOp,
    kind : @engine.UpdateKind,
  ) -> BytecodeOp {
    match (op, kind) {
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdatePrivatePrefixInc
      (@engine.UpdateOp::Dec, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdatePrivatePrefixDec
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Postfix) =>
        BytecodeOp::UpdatePrivatePostfixInc
      _ => BytecodeOp::UpdatePrivatePostfixDec
    }
  }
  fn bytecode_update_elem_op(
    op : @engine.UpdateOp,
    kind : @engine.UpdateKind,
  ) -> BytecodeOp {
    match (op, kind) {
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdateElemPrefixInc
      (@engine.UpdateOp::Dec, @engine.UpdateKind::Prefix) =>
        BytecodeOp::UpdateElemPrefixDec
      (@engine.UpdateOp::Inc, @engine.UpdateKind::Postfix) =>
        BytecodeOp::UpdateElemPostfixInc
      _ => BytecodeOp::UpdateElemPostfixDec
    }
  }
  match expr {
    Number(value, _, _) => {
      let idx = bytecode_add_const(builder, Number(value))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::PushConst, idx))
      true
    }
    Bool(value) => {
      let op = if value { BytecodeOp::PushTrue } else { BytecodeOp::PushFalse }
      bytecode_emit(builder, BytecodeInstr::Op(op))
      true
    }
    Null => {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushNull))
      true
    }
    String(value, _, _) => {
      let idx = bytecode_add_const(builder, String(value))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::PushConst, idx))
      true
    }
    TemplateLiteral(parts, exprs) => {
      let cooked = parts.cooked
      if cooked.is_empty() {
        let idx = bytecode_add_const(builder, String(""))
        bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::PushConst, idx))
        return true
      }
      match cooked[0] {
        Some(text) => {
          let idx = bytecode_add_const(builder, String(text))
          bytecode_emit(
            builder,
            BytecodeInstr::Const(BytecodeOp::PushConst, idx),
          )
        }
        None => {
          bytecode_emit_throw(
            builder,
            BytecodeOp::ThrowSyntaxError,
            "invalid escape sequence in template literal",
          )
          return true
        }
      }
      let add_code = 0 // BinaryOp::Add
      let mut i = 0
      while i < exprs.length() {
        if !bytecode_emit_expr(builder, exprs[i]) {
          return false
        }
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ToStringStrict))
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, add_code))
        if i + 1 < cooked.length() {
          match cooked[i + 1] {
            Some(text) => {
              let idx = bytecode_add_const(builder, String(text))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::PushConst, idx),
              )
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::Binary, add_code),
              )
            }
            None => {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "invalid escape sequence in template literal",
              )
              return true
            }
          }
        }
        i = i + 1
      }
      true
    }
    TaggedTemplate(callee, parts, exprs) => {
      if !bytecode_emit_tagged_template_callee(builder, callee) {
        return false
      }
      let idx = bytecode_add_tagged_template(builder, callee, parts, exprs)
      bytecode_emit(
        builder,
        BytecodeInstr::Const(BytecodeOp::TaggedTemplate, idx),
      )
      for expr in exprs {
        if !bytecode_emit_expr(builder, expr) {
          return false
        }
      }
      bytecode_emit(
        builder,
        BytecodeInstr::I32(
          BytecodeOp::CallWithThis,
          exprs.length() + 1,
        ),
      )
      true
    }
    Regexp(pattern, flags, offset) => {
      let pattern_idx = bytecode_add_const(builder, String(pattern))
      let flags_idx = bytecode_add_const(builder, String(flags))
      bytecode_emit(
        builder,
        BytecodeInstr::Const(BytecodeOp::PushConst, pattern_idx),
      )
      bytecode_emit(
        builder,
        BytecodeInstr::Const(BytecodeOp::PushConst, flags_idx),
      )
      bytecode_emit(
        builder,
        BytecodeInstr::I32(BytecodeOp::NewRegexp, offset),
      )
      true
    }
    BigInt(text) => {
      let mut parsed = @bigint.BigInt::from_int(0)
      let mut ok = true
      try {
        parsed = parse_bigint_literal(text)
      } catch {
        _ => ok = false
      }
      if !ok {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "invalid bigint literal",
        )
        return true
      }
      let idx = bytecode_add_const(builder, BigInt(parsed))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::PushConst, idx))
      true
    }
    ObjectLiteral(props) => {
      let func_def_no_name = fn(func : @engine.FunctionDef) -> @engine.FunctionDef {
        match func.name {
          Some(_) =>
            @engine.FunctionDef::new(
              None,
              func.params,
              func.param_inits,
              func.body,
              func.is_strict,
              func.is_generator,
              func.is_async,
              func.is_arrow,
              func.offset,
              func.end_offset,
            )
          None => func
        }
      }
      for prop in props {
        match prop {
          @engine.ObjectProp::KeyValue(key, _) =>
            match key {
              @engine.ObjKey::Ident(_)
              | @engine.ObjKey::String(_)
              | @engine.ObjKey::Number(_)
              | @engine.ObjKey::Computed(_) => ()
              _ => return false
            }
          @engine.ObjectProp::Method(key, _)
          | @engine.ObjectProp::Getter(key, _)
          | @engine.ObjectProp::Setter(key, _) =>
            match key {
              @engine.ObjKey::Ident(_)
              | @engine.ObjKey::String(_)
              | @engine.ObjKey::Number(_)
              | @engine.ObjKey::Computed(_) => ()
              _ => return false
            }
          @engine.ObjectProp::Shorthand(_) => ()
          @engine.ObjectProp::Spread(_) => ()
        }
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::NewObject))
      for prop in props {
        match prop {
          @engine.ObjectProp::KeyValue(key, expr) =>
            if obj_key_is_proto(key) {
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
              if !bytecode_emit_expr(builder, expr) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetProto))
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            } else {
              match key {
                @engine.ObjKey::Computed(key_expr) => {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  if !bytecode_emit_expr(builder, key_expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ToPropertyKey))
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  let op = if is_anonymous_function_definition(expr) {
                    BytecodeOp::InitPropComputedNamed
                  } else {
                    BytecodeOp::InitPropComputed
                  }
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Op(op),
                  )
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                }
                _ => {
                  let name = obj_key_simple_name(key)
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  if is_anonymous_function_definition(expr) {
                    if !bytecode_emit_named_expr(builder, expr, name) {
                      return false
                    }
                  } else if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  let op = if is_anonymous_function_definition(expr) {
                    BytecodeOp::InitPropNamed
                  } else {
                    BytecodeOp::InitProp
                  }
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(op, idx),
                  )
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                }
              }
            }
          @engine.ObjectProp::Method(key, func) => {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            match key {
              @engine.ObjKey::Computed(key_expr) => {
                if !bytecode_emit_expr(builder, key_expr) {
                  return false
                }
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ToPropertyKey))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Op(BytecodeOp::InitMethodComputed),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
              _ => {
                let name = obj_key_simple_name(key)
                let name_idx = bytecode_add_const(builder, String(name))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitMethod, name_idx),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
            }
          }
          @engine.ObjectProp::Shorthand(name) => {
            let idx = bytecode_add_const(builder, String(name))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::GetName, idx),
            )
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::InitProp, idx),
            )
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          @engine.ObjectProp::Getter(key, func) => {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            match key {
              @engine.ObjKey::Computed(key_expr) => {
                if !bytecode_emit_expr(builder, key_expr) {
                  return false
                }
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ToPropertyKey))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Op(BytecodeOp::InitGetterComputed),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
              _ => {
                let name = obj_key_simple_name(key)
                let name_idx = bytecode_add_const(builder, String(name))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitGetter, name_idx),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
            }
          }
          @engine.ObjectProp::Setter(key, func) => {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            match key {
              @engine.ObjKey::Computed(key_expr) => {
                if !bytecode_emit_expr(builder, key_expr) {
                  return false
                }
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ToPropertyKey))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Op(BytecodeOp::InitSetterComputed),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
              _ => {
                let name = obj_key_simple_name(key)
                let name_idx = bytecode_add_const(builder, String(name))
                let func_idx = bytecode_add_func(builder, func_def_no_name(func))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::NewFunction, func_idx),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitSetter, name_idx),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
            }
          }
          @engine.ObjectProp::Spread(expr) => {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            if !bytecode_emit_expr(builder, expr) {
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ObjectSpread))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
        }
      }
      true
    }
    ArrayLiteral(elems) => {
      let mut has_special = false
      for elem in elems {
        match elem {
          Some(@engine.Expr::Spread(_)) => has_special = true
          None => has_special = true
          _ => ()
        }
      }
      if !has_special {
        let mut count = 0
        for elem in elems {
          match elem {
            Some(_) => count = count + 1
            None => ()
          }
        }
        for elem in elems {
          match elem {
            Some(expr) =>
              if !bytecode_emit_expr(builder, expr) {
                return false
              }
            None => ()
          }
        }
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::NewArray, count))
        return true
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::NewArray, 0))
      let mut hole_count = 0
      for elem in elems {
        match elem {
          None => hole_count = hole_count + 1
          Some(@engine.Expr::Spread(expr)) => {
            if hole_count > 0 {
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::ArrayPushHole, hole_count),
              )
              hole_count = 0
            }
            if !bytecode_emit_expr(builder, expr) {
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArraySpread))
          }
          Some(expr) => {
            if hole_count > 0 {
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::ArrayPushHole, hole_count),
              )
              hole_count = 0
            }
            if !bytecode_emit_expr(builder, expr) {
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ArrayPush))
          }
        }
      }
      if hole_count > 0 {
        bytecode_emit(
          builder,
          BytecodeInstr::I32(BytecodeOp::ArrayPushHole, hole_count),
        )
      }
      true
    }
    Ident(name, _) => {
      let idx = bytecode_add_const(builder, String(name))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
      true
    }
    This => {
      let idx = bytecode_add_const(builder, String("this"))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
      true
    }
    NewTarget(_) => {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::NewTarget))
      true
    }
    ImportMeta(_) => {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ImportMeta))
      true
    }
    Super => {
      bytecode_emit_throw(
        builder,
        BytecodeOp::ThrowReferenceError,
        "super is not defined",
      )
      true
    }
    Yield(expr_opt, delegate) => {
      if !builder.allow_yield {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowTypeError,
          "yield not supported",
        )
        return true
      }
      if delegate {
        match expr_opt {
          Some(inner) => {
            let idx = bytecode_add_yield_star(builder, inner)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::YieldStar, idx),
            )
            return true
          }
          None => {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "missing yield* operand",
            )
            return true
          }
        }
      }
      match expr_opt {
        Some(expr) =>
          if !bytecode_emit_expr(builder, expr) {
            return false
          }
        None =>
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Yield))
      true
    }
    Await(inner, _) =>
      if bytecode_emit_expr(builder, inner) {
        if builder.allow_await {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Await))
        }
        true
      } else {
        false
      }
    Paren(inner) => bytecode_emit_expr(builder, inner)
    Sequence(exprs) => {
      if exprs.is_empty() {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
        return true
      }
      let last_index = exprs.length() - 1
      let mut i = 0
      while i < exprs.length() {
        if !bytecode_emit_expr(builder, exprs[i]) {
          return false
        }
        if i != last_index {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
        }
        i = i + 1
      }
      true
    }
    Unary(op, inner, _) =>
      match op {
        @engine.UnaryOp::Delete =>
          match unwrap_paren(inner) {
            Ident(name, _) => {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::DeleteName, idx),
              )
              true
            }
            Member(obj_expr, key, _) =>
              match unwrap_paren(obj_expr) {
                Super => {
                  let idx = bytecode_add_super_delete(builder, key)
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::DeleteSuper, idx),
                  )
                  true
                }
                _ =>
                  match key {
                    @engine.MemberKey::Ident(name) =>
                      if bytecode_emit_expr(builder, obj_expr) {
                        let idx = bytecode_add_const(builder, String(name))
                        bytecode_emit(
                          builder,
                          BytecodeInstr::Const(BytecodeOp::DeleteProp, idx),
                        )
                        true
                      } else {
                        false
                      }
                    @engine.MemberKey::Computed(expr) =>
                      if bytecode_emit_expr(builder, obj_expr) {
                        if !bytecode_emit_expr(builder, expr) {
                          false
                        } else {
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Op(BytecodeOp::DeleteElem),
                          )
                          true
                        }
                      } else {
                        false
                      }
                    @engine.MemberKey::Private(_) => {
                      bytecode_emit_throw(
                        builder,
                        BytecodeOp::ThrowSyntaxError,
                        "cannot delete a private class field",
                      )
                      true
                    }
                  }
              }
            OptionalChain(base, segments) => {
              bytecode_emit_delete_optional_chain(builder, base, segments)
            }
            _ =>
              if bytecode_emit_expr(builder, inner) {
                let code = bytecode_unary_op_code(op)
                bytecode_emit(
                  builder,
                  BytecodeInstr::I32(BytecodeOp::Unary, code),
                )
                true
              } else {
                false
              }
          }
        @engine.UnaryOp::Typeof =>
          match unwrap_paren(inner) {
            @engine.Expr::Ident(name, _) => {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::TypeofName, idx),
              )
              true
            }
            _ =>
              if bytecode_emit_expr(builder, inner) {
                let code = bytecode_unary_op_code(op)
                bytecode_emit(
                  builder,
                  BytecodeInstr::I32(BytecodeOp::Unary, code),
                )
                true
              } else {
                false
              }
          }
        _ =>
          if bytecode_emit_expr(builder, inner) {
            let code = bytecode_unary_op_code(op)
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Unary, code))
            true
          } else {
            false
          }
      }
    Update(op, inner, kind, _) =>
      match unwrap_paren(inner) {
        Ident(name, _) => {
          let idx = bytecode_add_const(builder, String(name))
          let update_op = bytecode_update_name_op(op, kind)
          bytecode_emit(builder, BytecodeInstr::Const(update_op, idx))
          true
        }
        Member(obj_expr, key, _) =>
          match unwrap_paren(obj_expr) {
            Super => {
              let idx = bytecode_add_super_update(builder, op, kind, key)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::SuperUpdate, idx),
              )
              true
            }
            _ =>
              match key {
                @engine.MemberKey::Ident(name) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    let idx = bytecode_add_const(builder, String(name))
                    let update_op = bytecode_update_prop_op(op, kind)
                    bytecode_emit(builder, BytecodeInstr::Const(update_op, idx))
                    true
                  } else {
                    false
                  }
                @engine.MemberKey::Computed(expr) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    if !bytecode_emit_expr(builder, expr) {
                      false
                    } else {
                      let update_op = bytecode_update_elem_op(op, kind)
                      bytecode_emit(builder, BytecodeInstr::Op(update_op))
                      true
                    }
                  } else {
                    false
                  }
                @engine.MemberKey::Private(name) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    let idx = bytecode_add_const(builder, String(name))
                    let update_op = bytecode_update_private_op(op, kind)
                    bytecode_emit(builder, BytecodeInstr::Const(update_op, idx))
                    true
                  } else {
                    false
                  }
              }
          }
        Call(_, _, _) =>
          if bytecode_emit_expr(builder, inner) {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowReferenceError,
              "invalid update target",
            )
            true
          } else {
            false
          }
        _ => {
          bytecode_emit_throw(
            builder,
            BytecodeOp::ThrowReferenceError,
            "invalid update target",
          )
          true
        }
      }
    Binary(op, left, right, _) =>
      match op {
        @engine.BinaryOp::LogicalAnd =>
          if !bytecode_emit_expr(builder, left) {
            false
          } else {
            let jump_index = bytecode_emit_jump(
              builder,
              BytecodeOp::JmpIfFalse,
            )
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if !bytecode_emit_expr(builder, right) {
              false
            } else {
              bytecode_patch_jump(
                builder,
                jump_index,
                bytecode_current_pc(builder),
              )
              true
            }
          }
        @engine.BinaryOp::LogicalOr =>
          if !bytecode_emit_expr(builder, left) {
            false
          } else {
            let jump_index = bytecode_emit_jump(
              builder,
              BytecodeOp::JmpIfTrue,
            )
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if !bytecode_emit_expr(builder, right) {
              false
            } else {
              bytecode_patch_jump(
                builder,
                jump_index,
                bytecode_current_pc(builder),
              )
              true
            }
          }
        @engine.BinaryOp::Coalesce =>
          if !bytecode_emit_expr(builder, left) {
            false
          } else {
            let nullish_jump =
              bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
            let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
            let rhs_pc = bytecode_current_pc(builder)
            bytecode_patch_jump(builder, nullish_jump, rhs_pc)
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if !bytecode_emit_expr(builder, right) {
              false
            } else {
              bytecode_patch_jump(
                builder,
                end_jump,
                bytecode_current_pc(builder),
              )
              true
            }
          }
        _ =>
          if bytecode_emit_expr(builder, left) {
            if bytecode_emit_expr(builder, right) {
              let code = bytecode_binary_op_code(op)
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, code))
              true
            } else {
              false
            }
          } else {
            false
          }
      }
    PrivateIn(name, right, _) =>
      if !bytecode_emit_expr(builder, right) {
        false
      } else {
        let idx = bytecode_add_const(builder, String(name))
        bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::PrivateIn, idx))
        true
      }
    Conditional(test_expr, conseq, alt) =>
      if !bytecode_emit_expr(builder, test_expr) {
        false
      } else {
        let else_jump = bytecode_emit_jump(
          builder,
          BytecodeOp::JmpIfFalse,
        )
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
        if !bytecode_emit_expr(builder, conseq) {
          false
        } else {
          let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
          let else_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, else_jump, else_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          if !bytecode_emit_expr(builder, alt) {
            false
          } else {
            bytecode_patch_jump(
              builder,
              end_jump,
              bytecode_current_pc(builder),
            )
            true
          }
        }
      }
    AssignInvalid(lhs, _, offset) => {
      if !bytecode_emit_expr(builder, lhs) {
        false
      } else {
        let idx = bytecode_add_assign_invalid(builder, lhs, offset)
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::AssignInvalid, idx),
        )
        true
      }
    }
    AssignArray(pattern, rhs) => {
      if !bytecode_emit_expr(builder, rhs) {
        false
      } else {
        let idx = bytecode_add_assign_array(builder, pattern, rhs)
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::AssignArray, idx),
        )
        true
      }
    }
    AssignObject(pattern, rhs) => {
      if !bytecode_emit_expr(builder, rhs) {
        false
      } else {
        let idx = bytecode_add_assign_object(builder, pattern, rhs)
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::AssignObject, idx),
        )
        true
      }
    }
    Assign(name, rhs, _, is_cover) => {
      let idx = bytecode_add_const(builder, String(name))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::BindName, idx))
      if !is_cover && is_anonymous_function_definition(rhs) {
        if !bytecode_emit_named_expr(builder, rhs, name) {
          return false
        }
      } else if !bytecode_emit_expr(builder, rhs) {
        return false
      }
      let op = if !is_cover && is_anonymous_function_definition(rhs) {
        BytecodeOp::AssignName
      } else {
        BytecodeOp::AssignNameNoName
      }
      bytecode_emit(builder, BytecodeInstr::Const(op, idx))
      true
    }
    AssignOp(op, lhs, rhs, _) => {
      let (target, is_cover) = unwrap_paren_with_cover(lhs)
      match target {
        @engine.Expr::Ident(name, _) => {
          let idx = bytecode_add_const(builder, String(name))
          bytecode_emit(
            builder,
            BytecodeInstr::Const(BytecodeOp::BindNameTarget, idx),
          )
          bytecode_emit(
            builder,
            BytecodeInstr::Const(BytecodeOp::GetNameFromTarget, idx),
          )
          let need_name = !is_cover && is_anonymous_function_definition(rhs)
          match op {
            @engine.BinaryOp::LogicalAnd => {
              let skip_jump =
                bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              if need_name {
                if !bytecode_emit_named_expr(builder, rhs, name) {
                  return false
                }
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
                )
              } else if !bytecode_emit_expr(builder, rhs) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignFromTarget, idx),
              )
              let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
              let skip_pc = bytecode_current_pc(builder)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PopTarget))
              bytecode_patch_jump(builder, skip_jump, skip_pc)
              bytecode_patch_jump(
                builder,
                end_jump,
                bytecode_current_pc(builder),
              )
              true
            }
            @engine.BinaryOp::LogicalOr => {
              let skip_jump =
                bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              if need_name {
                if !bytecode_emit_named_expr(builder, rhs, name) {
                  return false
                }
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
                )
              } else if !bytecode_emit_expr(builder, rhs) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignFromTarget, idx),
              )
              let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
              let skip_pc = bytecode_current_pc(builder)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PopTarget))
              bytecode_patch_jump(builder, skip_jump, skip_pc)
              bytecode_patch_jump(
                builder,
                end_jump,
                bytecode_current_pc(builder),
              )
              true
            }
            @engine.BinaryOp::Coalesce => {
              let assign_jump =
                bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PopTarget))
              let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
              let assign_pc = bytecode_current_pc(builder)
              bytecode_patch_jump(builder, assign_jump, assign_pc)
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              if need_name {
                if !bytecode_emit_named_expr(builder, rhs, name) {
                  return false
                }
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
                )
              } else if !bytecode_emit_expr(builder, rhs) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignFromTarget, idx),
              )
              bytecode_patch_jump(
                builder,
                end_jump,
                bytecode_current_pc(builder),
              )
              true
            }
            _ =>
              if bytecode_emit_expr(builder, rhs) {
                let code = bytecode_binary_op_code(op)
                bytecode_emit(
                  builder,
                  BytecodeInstr::I32(BytecodeOp::Binary, code),
                )
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::AssignFromTarget, idx),
                )
                true
              } else {
                false
              }
          }
        }
        @engine.Expr::Member(obj_expr, key, _) =>
          match unwrap_paren(obj_expr) {
            Super => {
              match key {
                @engine.MemberKey::Private(_) => {
                  bytecode_emit_throw(
                    builder,
                    BytecodeOp::ThrowSyntaxError,
                    "private class field forbidden after super",
                  )
                  true
                }
                @engine.MemberKey::Computed(expr) => {
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Op(BytecodeOp::SuperGetElem),
                  )
                  match op {
                    @engine.BinaryOp::LogicalAnd => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::SuperSetElem),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::LogicalOr => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::SuperSetElem),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::Coalesce => {
                      let assign_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let assign_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, assign_jump, assign_pc)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::SuperSetElem),
                      )
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    _ =>
                      if bytecode_emit_expr(builder, rhs) {
                        let code = bytecode_binary_op_code(op)
                        bytecode_emit(
                          builder,
                          BytecodeInstr::I32(BytecodeOp::Binary, code),
                        )
                        bytecode_emit(
                          builder,
                          BytecodeInstr::Op(BytecodeOp::SuperSetElem),
                        )
                        true
                      } else {
                        false
                      }
                  }
                }
                @engine.MemberKey::Ident(name) => {
                  let idx = bytecode_add_super_member(builder, key)
                  let name_idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::SuperMember, idx),
                  )
                  match op {
                    @engine.BinaryOp::LogicalAnd => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SuperSetProp, name_idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::LogicalOr => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SuperSetProp, name_idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::Coalesce => {
                      let assign_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let assign_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, assign_jump, assign_pc)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SuperSetProp, name_idx),
                      )
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    _ =>
                      if bytecode_emit_expr(builder, rhs) {
                        let code = bytecode_binary_op_code(op)
                        bytecode_emit(
                          builder,
                          BytecodeInstr::I32(BytecodeOp::Binary, code),
                        )
                        bytecode_emit(
                          builder,
                          BytecodeInstr::Const(BytecodeOp::SuperSetProp, name_idx),
                        )
                        true
                      } else {
                        false
                      }
                  }
                }
              }
            }
            _ =>
              match key {
                @engine.MemberKey::Ident(name) => {
                  if !bytecode_emit_expr(builder, obj_expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                  )
                  match op {
                    @engine.BinaryOp::LogicalAnd => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetProp, idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::LogicalOr => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetProp, idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::Coalesce => {
                      let assign_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let assign_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, assign_jump, assign_pc)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetProp, idx),
                      )
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    _ =>
                      if bytecode_emit_expr(builder, rhs) {
                        let code = bytecode_binary_op_code(op)
                        bytecode_emit(
                          builder,
                          BytecodeInstr::I32(BytecodeOp::Binary, code),
                        )
                        bytecode_emit(
                          builder,
                          BytecodeInstr::Const(BytecodeOp::SetProp, idx),
                        )
                        true
                      } else {
                        false
                      }
                  }
                }
                @engine.MemberKey::Private(name) => {
                  if !bytecode_emit_expr(builder, obj_expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                  )
                  match op {
                    @engine.BinaryOp::LogicalAnd => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetPrivate, idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::LogicalOr => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetPrivate, idx),
                      )
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::Coalesce => {
                      let assign_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let assign_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, assign_jump, assign_pc)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::SetPrivate, idx),
                      )
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    _ =>
                      if bytecode_emit_expr(builder, rhs) {
                        let code = bytecode_binary_op_code(op)
                        bytecode_emit(
                          builder,
                          BytecodeInstr::I32(BytecodeOp::Binary, code),
                        )
                        bytecode_emit(
                          builder,
                          BytecodeInstr::Const(BytecodeOp::SetPrivate, idx),
                        )
                        true
                      } else {
                        false
                      }
                  }
                }
                @engine.MemberKey::Computed(expr) => {
                  if !bytecode_emit_expr(builder, obj_expr) {
                    return false
                  }
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElemRef))
                  match op {
                    @engine.BinaryOp::LogicalAnd => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetElem))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::LogicalOr => {
                      let skip_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetElem))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let skip_pc = bytecode_current_pc(builder)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_patch_jump(builder, skip_jump, skip_pc)
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    @engine.BinaryOp::Coalesce => {
                      let assign_jump =
                        bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      let end_jump =
                        bytecode_emit_jump(builder, BytecodeOp::Jmp)
                      let assign_pc = bytecode_current_pc(builder)
                      bytecode_patch_jump(builder, assign_jump, assign_pc)
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                      if !bytecode_emit_expr(builder, rhs) {
                        return false
                      }
                      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetElem))
                      bytecode_patch_jump(
                        builder,
                        end_jump,
                        bytecode_current_pc(builder),
                      )
                      true
                    }
                    _ =>
                      if bytecode_emit_expr(builder, rhs) {
                        let code = bytecode_binary_op_code(op)
                        bytecode_emit(
                          builder,
                          BytecodeInstr::I32(BytecodeOp::Binary, code),
                        )
                        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetElem))
                        true
                      } else {
                        false
                      }
                  }
                }
              }
          }
        _ => false
      }
    }
    AssignMember(obj_expr, key, rhs, _) =>
      match unwrap_paren(obj_expr) {
        Super =>
          match key {
            @engine.MemberKey::Ident(name) => {
              bytecode_emit(
                builder,
                BytecodeInstr::Op(BytecodeOp::CheckSuperThis),
              )
              if !bytecode_emit_expr(builder, rhs) {
                false
              } else {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SuperSetProp, idx),
                )
                true
              }
            }
            @engine.MemberKey::Computed(expr) => {
              bytecode_emit(
                builder,
                BytecodeInstr::Op(BytecodeOp::CheckSuperThis),
              )
              if !bytecode_emit_expr(builder, expr) {
                false
              } else if !bytecode_emit_expr(builder, rhs) {
                false
              } else {
                bytecode_emit(
                  builder,
                  BytecodeInstr::Op(BytecodeOp::SuperSetElem),
                )
                true
              }
            }
            @engine.MemberKey::Private(_) => {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "private class field forbidden after super",
              )
              true
            }
          }
        _ =>
          match key {
            @engine.MemberKey::Ident(name) =>
              if !bytecode_emit_expr(builder, obj_expr) {
                false
              } else if !bytecode_emit_expr(builder, rhs) {
                false
              } else {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SetProp, idx),
                )
                true
              }
            @engine.MemberKey::Computed(expr) =>
              if !bytecode_emit_expr(builder, obj_expr) {
                false
              } else if !bytecode_emit_expr(builder, expr) {
                false
              } else if !bytecode_emit_expr(builder, rhs) {
                false
              } else {
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetElem))
                true
              }
            @engine.MemberKey::Private(name) =>
              if !bytecode_emit_expr(builder, obj_expr) {
                false
              } else if !bytecode_emit_expr(builder, rhs) {
                false
              } else {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::SetPrivate, idx),
                )
                true
              }
          }
      }
    New(callee, args) =>
      match unwrap_paren(callee) {
        @engine.Expr::Super => {
          bytecode_emit_throw(
            builder,
            BytecodeOp::ThrowReferenceError,
            "super is not defined",
          )
          true
        }
        _ =>
          if bytecode_emit_expr(builder, callee) {
            if args_have_spread(args) {
              if !bytecode_emit_args_array(builder, args) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::NewArgs))
            } else {
              if !bytecode_emit_args(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::New, args.length()),
              )
            }
            true
          } else {
            false
          }
      }
    Member(obj_expr, key, _) =>
      match unwrap_paren(obj_expr) {
        Super => {
          match key {
            @engine.MemberKey::Computed(expr) =>
              if !bytecode_emit_expr(builder, expr) {
                false
              } else {
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SuperGetElem))
                true
              }
            _ => {
              let idx = bytecode_add_super_member(builder, key)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::SuperMember, idx),
              )
              true
            }
          }
        }
        _ =>
          match key {
            @engine.MemberKey::Ident(name) =>
              if bytecode_emit_expr(builder, obj_expr) {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                )
                true
              } else {
                false
              }
            @engine.MemberKey::Computed(expr) =>
              if bytecode_emit_expr(builder, obj_expr) {
                if !bytecode_emit_expr(builder, expr) {
                  false
                } else {
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
                  true
                }
              } else {
                false
              }
            @engine.MemberKey::Private(name) =>
              if bytecode_emit_expr(builder, obj_expr) {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                )
                true
              } else {
                false
              }
          }
      }
    Call(callee, args, _) =>
      match unwrap_paren(callee) {
        @engine.Expr::Super => {
          if !bytecode_emit_args_array(builder, args) {
            return false
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SuperCallArgs))
          true
        }
        @engine.Expr::Ident(name, _) =>
          if name == "eval" {
            let idx = bytecode_add_const(builder, String(name))
            bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetCallRef, idx))
            if args_have_spread(args) {
              if !bytecode_emit_args_array(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::Op(BytecodeOp::CallWithThisEvalArgs),
              )
            } else {
              if !bytecode_emit_args(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::I32(
                  BytecodeOp::CallWithThisEval,
                  args.length(),
                ),
              )
            }
            true
          } else if name == "__export" {
            let idx = bytecode_add_const(builder, String(name))
            bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetCallRef, idx))
            if args_have_spread(args) {
              if !bytecode_emit_export_args_array(builder, args) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::CallWithThisArgs))
            } else {
              if !bytecode_emit_export_args(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::CallWithThis, args.length()),
              )
            }
            true
          } else {
            let idx = bytecode_add_const(builder, String(name))
            bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetCallRef, idx))
            if args_have_spread(args) {
              if !bytecode_emit_args_array(builder, args) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::CallWithThisArgs))
            } else {
              if !bytecode_emit_args(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::CallWithThis, args.length()),
              )
            }
            true
          }
        @engine.Expr::Member(obj_expr, key, _) =>
          match unwrap_paren(obj_expr) {
            Super => {
              let idx = bytecode_add_super_member_call(builder, key, args)
              match key {
                @engine.MemberKey::Computed(expr) =>
                  if !bytecode_emit_expr(builder, expr) {
                    return false
                  }
                _ => ()
              }
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::SuperMemberCall, idx),
              )
              if args_have_spread(args) {
                if !bytecode_emit_args_array(builder, args) {
                  return false
                }
                bytecode_emit(
                  builder,
                  BytecodeInstr::Op(BytecodeOp::CallWithThisArgs),
                )
              } else {
                if !bytecode_emit_args(builder, args) {
                  return false
                }
                bytecode_emit(
                  builder,
                  BytecodeInstr::I32(
                    BytecodeOp::CallWithThis,
                    args.length(),
                  ),
                )
              }
              true
            }
            _ =>
              match key {
                @engine.MemberKey::Ident(name) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::GetProp, idx),
                    )
                    if args_have_spread(args) {
                      if !bytecode_emit_args_array(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::CallWithThisArgs),
                      )
                    } else {
                      if !bytecode_emit_args(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::I32(
                          BytecodeOp::CallWithThis,
                          args.length(),
                        ),
                      )
                    }
                    true
                  } else {
                    false
                  }
                @engine.MemberKey::Computed(expr) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                    if !bytecode_emit_expr(builder, expr) {
                      return false
                    }
                    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetElem))
                    if args_have_spread(args) {
                      if !bytecode_emit_args_array(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::CallWithThisArgs),
                      )
                    } else {
                      if !bytecode_emit_args(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::I32(
                          BytecodeOp::CallWithThis,
                          args.length(),
                        ),
                      )
                    }
                    true
                  } else {
                    false
                  }
                @engine.MemberKey::Private(name) =>
                  if bytecode_emit_expr(builder, obj_expr) {
                    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::GetPrivate, idx),
                    )
                    if args_have_spread(args) {
                      if !bytecode_emit_args_array(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Op(BytecodeOp::CallWithThisArgs),
                      )
                    } else {
                      if !bytecode_emit_args(builder, args) {
                        return false
                      }
                      bytecode_emit(
                        builder,
                        BytecodeInstr::I32(
                          BytecodeOp::CallWithThis,
                          args.length(),
                        ),
                      )
                    }
                    true
                  } else {
                    false
                  }
              }
          }
        @engine.Expr::OptionalChain(base, segments) => {
          if !bytecode_emit_optional_chain_core(builder, base, segments) {
            return false
          }
          if args_have_spread(args) {
            if !bytecode_emit_args_array(builder, args) {
              return false
            }
            bytecode_emit(
              builder,
              BytecodeInstr::Op(BytecodeOp::CallWithThisArgs),
            )
          } else {
            if !bytecode_emit_args(builder, args) {
              return false
            }
            bytecode_emit(
              builder,
              BytecodeInstr::I32(BytecodeOp::CallWithThis, args.length()),
            )
          }
          true
        }
        _ =>
          if bytecode_emit_expr(builder, callee) {
            if args_have_spread(args) {
              if !bytecode_emit_args_array(builder, args) {
                return false
              }
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::CallArgs))
            } else {
              if !bytecode_emit_args(builder, args) {
                return false
              }
              bytecode_emit(
                builder,
                BytecodeInstr::I32(BytecodeOp::Call, args.length()),
              )
            }
            true
          } else {
            false
          }
      }
    @engine.Expr::FunctionExpr(func) => {
      let idx = bytecode_add_func(builder, func)
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::NewFunction, idx))
      true
    }
    ClassExpr(class_def) => {
      let idx = bytecode_add_class(builder, class_def)
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::NewClass, idx))
      true
    }
    OptionalChain(base, segments) => {
      bytecode_emit_optional_chain_value(builder, base, segments)
    }
    Spread(_) => {
      bytecode_emit_throw(
        builder,
        BytecodeOp::ThrowSyntaxError,
        "invalid spread expression",
      )
      true
    }
  }
}

///|
fn bytecode_emit_named_expr(
  builder : BytecodeBuilder,
  expr : @engine.Expr,
  name : String,
) -> Bool {
  match unwrap_paren(expr) {
    @engine.Expr::ClassExpr(class_def) =>
      if class_def.name is None {
        let idx = bytecode_add_class(builder, class_def, name_hint=Some(name))
        bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::NewClass, idx))
        true
      } else {
        bytecode_emit_expr(builder, expr)
      }
    _ => bytecode_emit_expr(builder, expr)
  }
}

///|
fn bytecode_emit_var_decl(
  builder : BytecodeBuilder,
  kind : @engine.VarKind,
  decls : Array[(@engine.VarBinding, @engine.Expr?)],
) -> Bool {
  for decl in decls {
    let (binding, init) = decl
    match binding {
      @engine.VarBinding::Name(name) => {
        match init {
          Some(expr) => {
            let idx = bytecode_add_const(builder, String(name))
            if kind is @engine.VarKind::Var {
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::BindNameTarget, idx),
              )
            }
            if is_anonymous_function_definition(expr) {
              if !bytecode_emit_named_expr(builder, expr, name) {
                return false
              }
            } else if !bytecode_emit_expr(builder, expr) {
              return false
            }
            if is_anonymous_function_definition(expr) {
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::SetAnonName, idx),
              )
            }
            match kind {
              @engine.VarKind::Var => {
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::AssignFromTarget, idx),
                )
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              }
              @engine.VarKind::ConstDecl =>
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                )
              _ =>
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitName, idx),
                )
            }
          }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "missing initializer in const declaration",
              )
              return true
            } else if kind is @engine.VarKind::Let {
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
            } else {
              continue
            }
        }
        match init {
          Some(_) => ()
          None =>
            if kind is @engine.VarKind::Let {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::InitName, idx),
              )
            } else {
              ()
            }
        }
      }
      @engine.VarBinding::ArrayPattern(pattern) =>
        match init {
          Some(expr) =>
            if !bytecode_emit_expr(builder, expr) {
              return false
            } else {
              let idx = bytecode_add_var_decl_array(builder, kind, pattern)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
              )
            }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "missing initializer in const declaration",
              )
              return true
            } else if kind is @engine.VarKind::Var {
              continue
            } else {
              let idx = bytecode_add_declare_binding(builder, binding)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::DeclareBinding, idx),
              )
            }
        }
      @engine.VarBinding::ObjectPattern(pattern) =>
        match init {
          Some(expr) =>
            if !bytecode_emit_expr(builder, expr) {
              return false
            } else {
              let idx = bytecode_add_var_decl_object(builder, kind, pattern)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
              )
            }
          None =>
            if kind is @engine.VarKind::ConstDecl {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "missing initializer in const declaration",
              )
              return true
            } else if kind is @engine.VarKind::Var {
              continue
            } else {
              let idx = bytecode_add_declare_binding(builder, binding)
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::DeclareBinding, idx),
              )
            }
        }
      @engine.VarBinding::Target(_) => {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "invalid binding",
        )
        return true
      }
    }
  }
  true
}

///|
type LoopForOfEntry = (Int, Bool, Bool, Int)

///|
type LabelEntry = (String, Bool, Int, Bool, Int, Int, Array[Int], Array[Int])

///|
fn label_targets_for_of(stmt : @engine.Stmt) -> Bool {
  match stmt {
    @engine.Stmt::ForOf(_, _, _) => true
    @engine.Stmt::ForAwaitOf(_, _, _) => true
    @engine.Stmt::Label(_, inner) => label_targets_for_of(inner)
    _ => false
  }
}

///|
fn bytecode_for_of_depth_get(depth_ref : Ref[Int]) -> Int {
  let mut value = 0
  depth_ref.update(fn(current) {
    value = current
    current
  })
  value
}

///|
fn bytecode_for_of_depth_set(depth_ref : Ref[Int], value : Int) -> Unit {
  depth_ref.update(fn(_) { value })
}

///|
fn bytecode_emit_for_of_close(
  builder : BytecodeBuilder,
  count : Int,
) -> Unit {
  if count > 0 {
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::ForOfClose, count))
  }
}

///|
fn bytecode_emit_enter_try(builder : BytecodeBuilder) -> Int {
  builder.try_stack_depth = builder.try_stack_depth + 1
  bytecode_emit_jump(builder, BytecodeOp::EnterTry)
}

///|
fn bytecode_emit_leave_try(builder : BytecodeBuilder) -> Unit {
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveTry))
  if builder.try_stack_depth > 0 {
    builder.try_stack_depth = builder.try_stack_depth - 1
  }
}

///|
fn bytecode_try_body_push(builder : BytecodeBuilder, base_depth : Int) -> Unit {
  builder.try_body_base_depths.push(base_depth)
}

///|
fn bytecode_try_body_pop(builder : BytecodeBuilder) -> Unit {
  let _ = builder.try_body_base_depths.pop()
}

///|
fn bytecode_emit_leave_try_to_base(
  builder : BytecodeBuilder,
  base_depth : Int,
) -> Unit {
  let saved_depth = builder.try_stack_depth
  let mut depth = builder.try_stack_depth
  while depth > base_depth {
    bytecode_emit_leave_try(builder)
    depth = builder.try_stack_depth
  }
  builder.try_stack_depth = saved_depth
}

///|
fn bytecode_emit_pop_env(
  builder : BytecodeBuilder,
  count : Int,
) -> Unit {
  if count > 0 {
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, count))
  }
}

///|
fn bytecode_finally_push(
  builder : BytecodeBuilder,
  normal_restore : Bool,
) -> Unit {
  let ctx = FinallyContext::{
    jump_indices: [],
    dispatch_jump_indices: [],
    resume_entries: [],
    next_code: 4,
    normal_restore,
  }
  builder.finally_stack.push(ctx)
}

///|
fn bytecode_finally_pop(
  builder : BytecodeBuilder,
) -> FinallyContext? {
  builder.finally_stack.pop()
}

///|
fn bytecode_finally_add_resume_at(
  builder : BytecodeBuilder,
  index : Int,
  targets : Array[Int],
  close_for_of : Int,
  pop_env : Int,
  restore_value : Bool,
) -> Int {
  let code = builder.finally_stack[index].next_code
  builder.finally_stack[index].next_code = code + 1
  builder.finally_stack[index].resume_entries.push(
    FinallyResumeEntry::{
      code,
      targets,
      close_for_of,
      pop_env,
      restore_value,
    },
  )
  code
}

///|
fn bytecode_finally_emit_jump(
  builder : BytecodeBuilder,
  code : Int,
  store_value : Bool,
) -> Unit {
  let top_index = builder.finally_stack.length() - 1
  bytecode_finally_emit_jump_at(builder, top_index, code, store_value)
}

///|
fn bytecode_finally_emit_jump_at(
  builder : BytecodeBuilder,
  index : Int,
  code : Int,
  store_value : Bool,
) -> Unit {
  if store_value {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetFinallyValue))
  }
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::SetFinallyCode, code))
  let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
  builder.finally_stack[index].jump_indices.push(jump_index)
}

///|
fn bytecode_finally_emit_dispatch_jump_at(
  builder : BytecodeBuilder,
  index : Int,
  code : Int,
  store_value : Bool,
) -> Unit {
  if store_value {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetFinallyValue))
  }
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::SetFinallyCode, code))
  let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
  builder.finally_stack[index].dispatch_jump_indices.push(jump_index)
}

///|
fn bytecode_finally_patch_jumps(
  builder : BytecodeBuilder,
  ctx : FinallyContext,
  target : Int,
) -> Unit {
  for idx in ctx.jump_indices {
    bytecode_patch_jump(builder, idx, target)
  }
}

///|
fn bytecode_finally_patch_dispatch_jumps(
  builder : BytecodeBuilder,
  ctx : FinallyContext,
  target : Int,
) -> Unit {
  for idx in ctx.dispatch_jump_indices {
    bytecode_patch_jump(builder, idx, target)
  }
}

///|
fn bytecode_finally_body_enter(
  builder : BytecodeBuilder,
  loop_for_of_depths : Array[LoopForOfEntry],
  label_stack : Array[LabelEntry],
) -> Unit {
  builder.finally_body_scopes.push(
    (loop_for_of_depths.length(), label_stack.length()),
  )
}

///|
fn bytecode_finally_body_exit(builder : BytecodeBuilder) -> Unit {
  let _ = builder.finally_body_scopes.pop()
}

///|
///|
fn bytecode_finally_emit_dispatch(
  builder : BytecodeBuilder,
  ctx : FinallyContext,
) -> Unit {
  let strict_eq_code = 17
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::GetFinallyCode))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PushI32, 1))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, strict_eq_code))
  let throw_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PushI32, 2))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, strict_eq_code))
  let return_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PushI32, 3))
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, strict_eq_code))
  let return_no_await_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  let resume_jumps : Array[(Int, FinallyResumeEntry)] = []
  let mut i = 0
  while i < ctx.resume_entries.length() {
    let entry = ctx.resume_entries[i]
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PushI32, entry.code))
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Binary, strict_eq_code))
    let jump_index = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    resume_jumps.push((jump_index, entry))
    i = i + 1
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  if ctx.normal_restore {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushFinallyValue))
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
  let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)

  let throw_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, throw_jump, throw_pc)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushFinallyValue))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Throw))

  let return_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, return_jump, return_pc)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushFinallyValue))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Return))

  let return_no_await_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, return_no_await_jump, return_no_await_pc)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushFinallyValue))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ReturnNoAwait))

  for entry_jump in resume_jumps {
    let (jump_index, entry) = entry_jump
    let label_pc = bytecode_current_pc(builder)
    bytecode_patch_jump(builder, jump_index, label_pc)
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    if entry.restore_value {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushFinallyValue))
    }
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
    bytecode_emit_for_of_close(builder, entry.close_for_of)
    bytecode_emit_pop_env(builder, entry.pop_env)
    let resume_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
    let targets = entry.targets
    targets.push(resume_jump)
  }

  let end_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, end_jump, end_pc)
}

///|
fn bytecode_emit_try_body(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  try_body : @engine.Stmt,
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Bool {
  let try_updates = stmt_updates_completion(try_body)
  match try_body {
    @engine.Stmt::Block(stmts) =>
      bytecode_emit_block(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        stmts,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      )
    _ =>
      if bytecode_emit_stmt(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        try_body,
        try_updates,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        if try_updates && builder.try_depth > 0 {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::UpdateTryLast))
        }
        if result_needed {
          if !try_updates {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
        } else if try_updates {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
        }
        true
      } else {
        false
      }
  }
}

///|
fn bytecode_emit_try_catch_inner(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  try_body : @engine.Stmt,
  catch_clause : @engine.CatchClause,
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
  try_body_base_depth_override : Int?,
) -> Bool {
  let catch_updates = stmt_updates_completion(catch_clause.body)
  let base_depth = match try_body_base_depth_override {
    Some(value) => value
    None => builder.try_stack_depth
  }
  bytecode_try_body_push(builder, base_depth)
  let enter_index = bytecode_emit_enter_try(builder)
  if result_needed {
    builder.try_depth = builder.try_depth + 1
  }
  let try_ok = bytecode_emit_try_body(
    builder,
    loop_breaks,
    loop_continues,
    label_stack,
    try_body,
    result_needed,
    loop_for_of_depths,
    for_of_depth_ref,
  )
  bytecode_try_body_pop(builder)
  if result_needed {
    builder.try_depth = builder.try_depth - 1
  }
  if !try_ok {
    return false
  }
  bytecode_emit_leave_try(builder)
  let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
  let catch_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, enter_index, catch_pc)
  let lex_bindings : Array[@engine.VarBinding] = []
  match catch_clause.name {
    Some(name) =>
      lex_bindings.push(@engine.var_binding_name(name))
    None => ()
  }
  let def = BlockEnvDef::{
    func_indices: [],
    lex_bindings,
    class_names: [],
  }
  let env_idx = bytecode_add_block_env(builder, def)
  bytecode_emit(
    builder,
    BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
  )
  builder.block_env_depth = builder.block_env_depth + 1
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::MarkCatchEnv))
  match catch_clause.name {
    Some(name) => {
      let name_idx = bytecode_add_const(builder, String(name))
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, name_idx))
    }
    None => {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    }
  }
  builder.try_stack_depth = builder.try_stack_depth + 1
  bytecode_try_body_push(builder, base_depth)
  let mut catch_ok = true
  match catch_clause.body {
    @engine.Stmt::Block(stmts) =>
      match catch_clause.name {
        Some(name) => {
          let mut handled = false
          if stmts.length() > 0 {
            match stmts[0] {
              @engine.Stmt::VarDecl(kind, decls) =>
                if kind is @engine.VarKind::Let && decls.length() == 1 {
                  let (_, init_expr_opt) = decls[0]
                  match init_expr_opt {
                    Some(@engine.Expr::Ident(init_name, _)) =>
                      if init_name == name {
                        handled = true
                        if !bytecode_emit_var_decl(builder, kind, decls) {
                          catch_ok = false
                        } else {
                          let rest : Array[@engine.Stmt] = []
                          let mut idx = 1
                          while idx < stmts.length() {
                            rest.push(stmts[idx])
                            idx = idx + 1
                          }
                          catch_ok = bytecode_emit_block(
                            builder,
                            loop_breaks,
                            loop_continues,
                            label_stack,
                            rest,
                            result_needed,
                            loop_for_of_depths,
                            for_of_depth_ref,
                          )
                        }
                      }
                    _ => ()
                  }
                }
              _ => ()
            }
          }
          if !handled {
            catch_ok = bytecode_emit_block(
              builder,
              loop_breaks,
              loop_continues,
              label_stack,
              stmts,
              result_needed,
              loop_for_of_depths,
              for_of_depth_ref,
            )
          }
        }
        None =>
          catch_ok = bytecode_emit_block(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            stmts,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          )
      }
    _ =>
      catch_ok = bytecode_emit_stmt(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        catch_clause.body,
        catch_updates,
        loop_for_of_depths,
        for_of_depth_ref,
      )
  }
  bytecode_try_body_pop(builder)
  builder.try_stack_depth = builder.try_stack_depth - 1
  if !catch_ok {
    builder.block_env_depth = builder.block_env_depth - 1
    return false
  }
  if result_needed && !catch_updates {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushTryLast))
  }
  builder.block_env_depth = builder.block_env_depth - 1
  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
  bytecode_emit_leave_try(builder)
  bytecode_patch_jump(builder, end_jump, bytecode_current_pc(builder))
  true
}

///|
fn bytecode_emit_try_finally(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  try_body : @engine.Stmt,
  catch_clause_opt : @engine.CatchClause?,
  finally_body : @engine.Stmt,
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Bool {
  bytecode_finally_push(builder, result_needed)
  let enter_pc = bytecode_current_pc(builder)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::EnterFinally))
  let try_body_base_depth = builder.try_stack_depth
  let outer_enter = bytecode_emit_enter_try(builder)
  let ok = match catch_clause_opt {
    Some(catch_clause) =>
      bytecode_emit_try_catch_inner(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        try_body,
        catch_clause,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
        Some(try_body_base_depth),
      )
    None =>
      {
        bytecode_try_body_push(builder, try_body_base_depth)
        let ok = bytecode_emit_try_body(
          builder,
          loop_breaks,
          loop_continues,
          label_stack,
          try_body,
          result_needed,
          loop_for_of_depths,
          for_of_depth_ref,
        )
        bytecode_try_body_pop(builder)
        ok
      }
  }
  if !ok {
    let _ = bytecode_finally_pop(builder)
    return false
  }
  bytecode_emit_leave_try(builder)
  if result_needed {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetFinallyValue))
  }
  bytecode_finally_emit_jump(builder, 0, false)
  let handler_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, outer_enter, handler_pc)
  bytecode_emit_leave_try(builder)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::SetFinallyValue))
  bytecode_finally_emit_jump(builder, 1, false)
  let finally_start = bytecode_current_pc(builder)
  if builder.finally_stack.is_empty() {
    return false
  }
  let ctx_index = builder.finally_stack.length() - 1
  let ctx_snapshot = builder.finally_stack[ctx_index]
  bytecode_finally_patch_jumps(builder, ctx_snapshot, finally_start)

  let finally_enter = bytecode_emit_enter_try(builder)
  bytecode_finally_body_enter(builder, loop_for_of_depths, label_stack)
  let finally_ok = bytecode_emit_stmt(
    builder,
    loop_breaks,
    loop_continues,
    label_stack,
    finally_body,
    false,
    loop_for_of_depths,
    for_of_depth_ref,
  )
  bytecode_finally_body_exit(builder)
  if !finally_ok {
    let _ = bytecode_finally_pop(builder)
    return false
  }
  bytecode_emit_leave_try(builder)
  let finally_end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
  let finally_handler_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, finally_enter, finally_handler_pc)
  bytecode_emit_leave_try(builder)
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::LeaveFinally))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Throw))
  let dispatch_pc = bytecode_current_pc(builder)
  bytecode_patch_jump(builder, finally_end_jump, dispatch_pc)
  let ctx = match bytecode_finally_pop(builder) {
    Some(value) => value
    None => return false
  }
  bytecode_finally_patch_dispatch_jumps(builder, ctx, dispatch_pc)
  builder.finally_pc_pool.push((enter_pc, finally_start, dispatch_pc))
  bytecode_finally_emit_dispatch(builder, ctx)
  true
}

///|
fn bytecode_break_for_of_entry(
  loop_for_of_depths : Array[LoopForOfEntry],
) -> LoopForOfEntry? {
  if loop_for_of_depths.is_empty() {
    None
  } else {
    Some(loop_for_of_depths[loop_for_of_depths.length() - 1])
  }
}

///|
fn bytecode_continue_for_of_entry(
  loop_for_of_depths : Array[LoopForOfEntry],
) -> LoopForOfEntry? {
  let mut i = loop_for_of_depths.length()
  while i > 0 {
    i = i - 1
    let entry = loop_for_of_depths[i]
    let (_, _, is_loop, _) = entry
    if is_loop {
      return Some(entry)
    }
  }
  None
}

///|
fn bytecode_loop_push(
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
  is_for_of : Bool,
  is_loop : Bool,
  block_env_depth : Int,
) -> Unit {
  let entry_depth = bytecode_for_of_depth_get(for_of_depth_ref)
  loop_for_of_depths.push((entry_depth, is_for_of, is_loop, block_env_depth))
  if is_for_of {
    bytecode_for_of_depth_set(for_of_depth_ref, entry_depth + 1)
  }
}

///|
fn bytecode_loop_pop(
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Unit {
  match loop_for_of_depths.pop() {
    Some((entry_depth, is_for_of, _, _)) =>
      if is_for_of {
        bytecode_for_of_depth_set(for_of_depth_ref, entry_depth)
      }
    None => ()
  }
}

///|
fn bytecode_find_label(
  label_stack : Array[LabelEntry],
  name : String,
) -> (Bool, Int, Bool, Int, Int, Array[Int], Array[Int], Int)? {
  let mut i = label_stack.length()
  while i > 0 {
    i = i - 1
    let (
      label_name,
      is_iteration,
      for_of_depth,
      is_for_of,
      break_env_depth,
      continue_env_depth,
      breaks,
      continues,
    ) =
      label_stack[i]
    if label_name == name {
      return Some(
        (
          is_iteration,
          for_of_depth,
          is_for_of,
          break_env_depth,
          continue_env_depth,
          breaks,
          continues,
          i,
        ),
      )
    }
  }
  None
}

///|
fn bytecode_label_update_continue_env(
  label_stack : Array[LabelEntry],
  index : Int,
  continue_env_depth : Int,
) -> Unit {
  if index < 0 || index >= label_stack.length() {
    return
  }
  let (
    name,
    is_iteration,
    for_of_depth,
    is_for_of,
    break_env_depth,
    _,
    breaks,
    continues,
  ) =
    label_stack[index]
  label_stack[index] =
    (
      name,
      is_iteration,
      for_of_depth,
      is_for_of,
      break_env_depth,
      continue_env_depth,
      breaks,
      continues,
    )
}

///|
fn bytecode_collect_block_env(
  builder : BytecodeBuilder,
  stmts : Array[@engine.Stmt],
) -> Int {
  let func_indices : Array[Int] = []
  let lex_bindings : Array[@engine.VarBinding] = []
  let class_names : Array[String] = []
  fn collect_funcs(
    builder : BytecodeBuilder,
    stmt : @engine.Stmt,
    func_indices : Array[Int],
  ) -> Unit {
    match stmt {
      @engine.Stmt::FunctionDecl(func) => {
        let idx = bytecode_add_func(builder, func)
        func_indices.push(idx)
      }
      @engine.Stmt::Label(_, inner) =>
        collect_funcs(builder, inner, func_indices)
      _ => ()
    }
  }
  for stmt in stmts {
    collect_funcs(builder, stmt, func_indices)
    match stmt {
      @engine.Stmt::VarDecl(kind, decls) =>
        if !(kind is @engine.VarKind::Var) {
          for decl in decls {
            let (binding, _) = decl
            lex_bindings.push(binding)
          }
        }
      @engine.Stmt::ClassDecl(class_def) =>
        match class_def.name {
          Some(name) => class_names.push(name)
          None => ()
        }
      _ => ()
    }
  }
  let def = BlockEnvDef::{ func_indices, lex_bindings, class_names }
  bytecode_add_block_env(builder, def)
}

///|
fn bytecode_for_init_binding_names(
  init : @engine.ForInit,
) -> Array[String] {
  let names : @hashmap.HashMap[String, Bool] = @hashmap.new()
  match init {
    @engine.ForInit::Var(_, decls) =>
      for decl in decls {
        let (binding, _) = decl
        collect_binding_names(binding, names)
      }
    _ => ()
  }
  let result : Array[String] = []
  for name, _ in names {
    result.push(name)
  }
  result
}

///|
fn bytecode_emit_block(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  stmts : Array[@engine.Stmt],
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Bool {
  let mut needs_lex_env = block_needs_lex_env(stmts, false)
  if block_is_export_wrapper(stmts) {
    needs_lex_env = false
  }
  if needs_lex_env {
    let env_idx = bytecode_collect_block_env(builder, stmts)
    bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx))
    builder.block_env_depth = builder.block_env_depth + 1
  }
  let mut track_completion = false
  let mut scan = 0
  while scan < stmts.length() {
    if stmt_updates_completion(stmts[scan]) {
      track_completion = true
      break
    }
    scan = scan + 1
  }
  if track_completion {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
  }
  let mut i = 0
  while i < stmts.length() {
    let updates = stmt_updates_completion(stmts[i])
    if !bytecode_emit_stmt(
      builder,
      loop_breaks,
      loop_continues,
      label_stack,
      stmts[i],
      updates,
      loop_for_of_depths,
      for_of_depth_ref,
    ) {
      if needs_lex_env {
        builder.block_env_depth = builder.block_env_depth - 1
      }
      return false
    }
    if updates && builder.try_depth > 0 {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::UpdateTryLast))
    }
    if track_completion && updates {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    }
    i = i + 1
  }
  if track_completion {
    if !result_needed {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    }
  } else if result_needed {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
  }
  if needs_lex_env {
    builder.block_env_depth = builder.block_env_depth - 1
    bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
  }
  true
}

///|
fn bytecode_emit_stmt_with_lex_env(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  stmt : @engine.Stmt,
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Bool {
  if stmt_needs_lex_env(stmt, false) {
    match stmt {
      @engine.Stmt::Block(_) =>
        bytecode_emit_stmt(
          builder,
          loop_breaks,
          loop_continues,
          label_stack,
          stmt,
          result_needed,
          loop_for_of_depths,
          for_of_depth_ref,
        )
      _ => {
        let env_idx = bytecode_collect_block_env(builder, [stmt])
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
        )
        builder.block_env_depth = builder.block_env_depth + 1
        if !bytecode_emit_stmt(
          builder,
          loop_breaks,
          loop_continues,
          label_stack,
          stmt,
          result_needed,
          loop_for_of_depths,
          for_of_depth_ref,
        ) {
          builder.block_env_depth = builder.block_env_depth - 1
          return false
        }
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
        true
      }
    }
  } else {
    bytecode_emit_stmt(
      builder,
      loop_breaks,
      loop_continues,
      label_stack,
      stmt,
      result_needed,
      loop_for_of_depths,
      for_of_depth_ref,
    )
  }
}

///|
fn bytecode_emit_stmt(
  builder : BytecodeBuilder,
  loop_breaks : Array[Array[Int]],
  loop_continues : Array[Array[Int]],
  label_stack : Array[LabelEntry],
  stmt : @engine.Stmt,
  result_needed : Bool,
  loop_for_of_depths : Array[LoopForOfEntry],
  for_of_depth_ref : Ref[Int],
) -> Bool {
  match stmt {
    @engine.Stmt::Empty =>
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
        true
      } else {
        true
      }
    @engine.Stmt::ExprStmt(expr) =>
      if bytecode_emit_expr(builder, expr) {
        if !result_needed {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
        }
        true
      } else {
        false
      }
    @engine.Stmt::VarDecl(kind, decls) =>
      if bytecode_emit_var_decl(builder, kind, decls) {
        if result_needed {
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
        }
        true
      } else {
        false
      }
    @engine.Stmt::FunctionDecl(func) => {
      let idx = bytecode_add_func(builder, func)
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::FunctionDecl, idx))
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      true
    }
    @engine.Stmt::ClassDecl(class_def) => {
      let idx = bytecode_add_class(builder, class_def)
      bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::NewClass, idx))
      match class_def.name {
        Some(name) => {
          let name_idx = bytecode_add_const(builder, String(name))
          bytecode_emit(
            builder,
            BytecodeInstr::Const(BytecodeOp::InitName, name_idx),
          )
        }
        None => bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      true
    }
    @engine.Stmt::Block(stmts) =>
      bytecode_emit_block(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        stmts,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      )
    @engine.Stmt::If(cond_expr, conseq, alt) => {
      if !bytecode_emit_expr(builder, cond_expr) {
        return false
      }
      let else_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        conseq,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        return false
      }
      let end_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
      let else_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, else_jump, else_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      match alt {
        Some(stmt) =>
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            stmt,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            return false
          }
        None =>
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
      }
      bytecode_patch_jump(builder, end_jump, bytecode_current_pc(builder))
      true
    }
    @engine.Stmt::With(expr, body) => {
      if !bytecode_emit_expr(builder, expr) {
        return false
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::EnterWithEnv))
      builder.block_env_depth = builder.block_env_depth + 1
      if !bytecode_emit_stmt(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        builder.block_env_depth = builder.block_env_depth - 1
        return false
      }
      builder.block_env_depth = builder.block_env_depth - 1
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      true
    }
    @engine.Stmt::Return(expr_opt) => {
      if !builder.allow_return {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "illegal return",
        )
        return true
      }
      let is_async_gen = builder.allow_yield && builder.allow_await
      let return_no_await = is_async_gen && expr_opt is None
      match expr_opt {
        Some(expr) =>
          if !bytecode_emit_expr(builder, expr) {
            return false
          }
        None =>
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      let use_finally = !builder.finally_stack.is_empty()
      let mut jump_to_dispatch = false
      if !builder.try_body_base_depths.is_empty() {
        let index = builder.try_body_base_depths.length() - 1
        let base_depth = builder.try_body_base_depths[index]
        bytecode_emit_leave_try_to_base(builder, base_depth)
      }
      if use_finally && !builder.finally_body_scopes.is_empty() {
        if builder.finally_body_scopes.length() >= builder.finally_stack.length() {
          jump_to_dispatch = true
        }
      }
      if use_finally {
        let top_index = builder.finally_stack.length() - 1
        let return_code = if return_no_await { 3 } else { 2 }
        if jump_to_dispatch {
          bytecode_finally_emit_dispatch_jump_at(
            builder,
            top_index,
            return_code,
            true,
          )
        } else {
          bytecode_finally_emit_jump_at(builder, top_index, return_code, true)
        }
      } else {
        let op = if return_no_await {
          BytecodeOp::ReturnNoAwait
        } else {
          BytecodeOp::Return
        }
        bytecode_emit(builder, BytecodeInstr::Op(op))
      }
      true
    }
    @engine.Stmt::Throw(expr) =>
      if bytecode_emit_expr(builder, expr) {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Throw))
        true
      } else {
        false
      }
    @engine.Stmt::Try(try_body, catch_clause_opt, finally_body_opt) =>
      match finally_body_opt {
        Some(finally_body) =>
          bytecode_emit_try_finally(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            try_body,
            catch_clause_opt,
            finally_body,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          )
        None =>
          match catch_clause_opt {
            Some(catch_clause) =>
              bytecode_emit_try_catch_inner(
                builder,
                loop_breaks,
                loop_continues,
                label_stack,
                try_body,
                catch_clause,
                result_needed,
                loop_for_of_depths,
                for_of_depth_ref,
                None,
              )
            None => false
          }
      }
    @engine.Stmt::Switch(discriminant, cases) => {
      let case_stmts : Array[@engine.Stmt] = []
      for clause in cases {
        let body = match clause {
          @engine.SwitchCase::Case(_, stmts) => stmts
          @engine.SwitchCase::Default(stmts) => stmts
        }
        for stmt in body {
          case_stmts.push(stmt)
        }
      }
      if !bytecode_emit_expr(builder, discriminant) {
        return false
      }
      let env_idx = bytecode_collect_block_env(builder, case_stmts)
      bytecode_emit(
        builder,
        BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
      )
      builder.block_env_depth = builder.block_env_depth + 1
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      loop_breaks.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        false,
        false,
        builder.block_env_depth,
      )
      let case_body_jumps : Array[Int] = []
      let mut default_index = -1
      let strict_eq_code = 17
      for clause in cases {
        match clause {
          @engine.SwitchCase::Case(expr, _) => {
            if result_needed {
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Dup))
            if !bytecode_emit_expr(builder, expr) {
              bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
              return false
            }
            bytecode_emit(
              builder,
              BytecodeInstr::I32(
                BytecodeOp::Binary,
                strict_eq_code,
              ),
            )
            let false_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if result_needed {
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            }
            let body_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
            case_body_jumps.push(body_jump)
            let false_pc = bytecode_current_pc(builder)
            bytecode_patch_jump(builder, false_jump, false_pc)
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if result_needed {
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            }
          }
          @engine.SwitchCase::Default(_) => {
            default_index = case_body_jumps.length()
            case_body_jumps.push(-1)
          }
        }
      }
      let no_match_jump = bytecode_emit_jump(builder, BytecodeOp::Jmp)
      let mut default_label = -1
      let mut clause_index = 0
      while clause_index < cases.length() {
        let clause = cases[clause_index]
        let clause_start = bytecode_current_pc(builder)
        let jump_idx = case_body_jumps[clause_index]
        if jump_idx >= 0 {
          bytecode_patch_jump(builder, jump_idx, clause_start)
        }
        match clause {
          @engine.SwitchCase::Default(_) => default_label = clause_start
          _ => ()
        }
        let stmts = match clause {
          @engine.SwitchCase::Case(_, body) => body
          @engine.SwitchCase::Default(body) => body
        }
        for stmt in stmts {
          let updates = stmt_updates_completion(stmt)
          if result_needed && updates {
            if !bytecode_emit_stmt(
              builder,
              loop_breaks,
              loop_continues,
              label_stack,
              stmt,
              true,
              loop_for_of_depths,
              for_of_depth_ref,
            ) {
              bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
              return false
            }
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          } else if !bytecode_emit_stmt(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            stmt,
            false,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            return false
          }
        }
        clause_index = clause_index + 1
      }
      let exit_pc = bytecode_current_pc(builder)
      if default_index >= 0 && default_label >= 0 {
        bytecode_patch_jump(builder, no_match_jump, default_label)
      } else {
        bytecode_patch_jump(builder, no_match_jump, exit_pc)
      }
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      } else {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      builder.block_env_depth = builder.block_env_depth - 1
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      true
    }
    @engine.Stmt::While(cond_expr, body) => {
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      let loop_start = bytecode_current_pc(builder)
      if !bytecode_emit_expr(builder, cond_expr) {
        return false
      }
      let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        false,
        true,
        builder.block_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let continue_target = loop_start
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, continue_target)
          }
        None => ()
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_cond_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      let exit_pc = bytecode_current_pc(builder)
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      true
    }
    @engine.Stmt::For(init, test_expr, update_expr, body) => {
      let mut use_loop_env = false
      let mut loop_kind : @engine.VarKind? = None
      let loop_bindings : Array[@engine.VarBinding] = []
      let mut loop_names : Array[String] = []
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if kind is @engine.VarKind::Var {
            if !bytecode_emit_var_decl(builder, kind, decls) {
              return false
            }
          } else {
            use_loop_env = true
            loop_kind = Some(kind)
            for decl in decls {
              let (binding, _) = decl
              loop_bindings.push(binding)
            }
            loop_names = bytecode_for_init_binding_names(init)
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: loop_bindings,
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            if !bytecode_emit_var_decl(builder, kind, decls) {
              builder.block_env_depth = builder.block_env_depth - 1
              return false
            }
          }
        @engine.ForInit::Expr(expr) =>
          if bytecode_emit_expr(builder, expr) {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          } else {
            return false
          }
        @engine.ForInit::None => ()
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      let per_iter =
        use_loop_env &&
        for_loop_needs_per_iteration_env(init, test_expr, update_expr, body)
      let loop_entry_env_depth = builder.block_env_depth
      let init_op = match loop_kind {
        Some(@engine.VarKind::ConstDecl) => BytecodeOp::InitConstName
        _ => BytecodeOp::InitName
      }
      if per_iter {
        for name in loop_names {
          let idx = bytecode_add_const(builder, String(name))
          bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
        }
        let def = BlockEnvDef::{
          func_indices: [],
          lex_bindings: loop_bindings,
          class_names: [],
        }
        let env_idx = bytecode_add_block_env(builder, def)
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
        )
        builder.block_env_depth = builder.block_env_depth + 1
        let mut i = loop_names.length()
        while i > 0 {
          i = i - 1
          let idx = bytecode_add_const(builder, String(loop_names[i]))
          bytecode_emit(builder, BytecodeInstr::Const(init_op, idx))
        }
      }
      let loop_start = bytecode_current_pc(builder)
      let mut exit_jump = -1
      match test_expr {
        Some(expr) =>
          if bytecode_emit_expr(builder, expr) {
            exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          } else {
            return false
          }
        None => ()
      }
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        false,
        true,
        loop_entry_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        if per_iter {
          builder.block_env_depth = builder.block_env_depth - 1
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
        }
        if use_loop_env {
          builder.block_env_depth = builder.block_env_depth - 1
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
        }
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let update_start = bytecode_current_pc(builder)
      if per_iter {
        for name in loop_names {
          let idx = bytecode_add_const(builder, String(name))
          bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
        }
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
        let def = BlockEnvDef::{
          func_indices: [],
          lex_bindings: loop_bindings,
          class_names: [],
        }
        let env_idx = bytecode_add_block_env(builder, def)
        bytecode_emit(
          builder,
          BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
        )
        builder.block_env_depth = builder.block_env_depth + 1
        let mut i = loop_names.length()
        while i > 0 {
          i = i - 1
          let idx = bytecode_add_const(builder, String(loop_names[i]))
          bytecode_emit(builder, BytecodeInstr::Const(init_op, idx))
        }
      }
      match update_expr {
        Some(expr) =>
          if bytecode_emit_expr(builder, expr) {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          } else {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            if per_iter {
              builder.block_env_depth = builder.block_env_depth - 1
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            }
            if use_loop_env {
              builder.block_env_depth = builder.block_env_depth - 1
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            }
            return false
          }
        None => ()
      }
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, update_start)
          }
        None => ()
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_cond_pc = bytecode_current_pc(builder)
      if exit_jump >= 0 {
        bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
        if per_iter {
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
        }
      }
      let exit_pc = bytecode_current_pc(builder)
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      if use_loop_env {
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      }
      true
    }
    @engine.Stmt::ForIn(init, expr, body) => {
      let mut init_name : String? = None
      let mut init_kind : @engine.VarKind? = None
      let mut init_binding : @engine.VarBinding? = None
      let mut init_expr : @engine.Expr? = None
      let mut init_var_decls : Array[(@engine.VarBinding, @engine.Expr?)] = []
      let mut init_is_var = false
      let mut init_is_lex = false
      let mut init_has_init = false
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if decls.length() == 1 {
            let (binding, init_expr) = decls[0]
            init_has_init = init_expr is Some(_)
            init_kind = Some(kind)
            init_binding = Some(binding)
            if kind is @engine.VarKind::Var {
              init_is_var = true
              init_var_decls = decls
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                @engine.VarBinding::ArrayPattern(_) => ()
                @engine.VarBinding::ObjectPattern(_) => ()
                _ => return false
              }
            } else {
              init_is_lex = true
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                _ => ()
              }
            }
          } else {
            return false
          }
        @engine.ForInit::Expr(inner) =>
          match unwrap_paren(inner) {
            @engine.Expr::Ident(name, _) => init_name = Some(name)
            _ => init_expr = Some(inner)
          }
        @engine.ForInit::None => return false
      }
      if init_is_var && init_has_init {
        match init_kind {
          Some(kind) =>
            if !bytecode_emit_var_decl(builder, kind, init_var_decls) {
              return false
            }
          None => return false
        }
      }
      if init_is_lex && init_has_init {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "a declaration in the head of a for-in loop can't have an initializer",
        )
        return true
      }
      if !init_is_lex {
        if init_name is None && init_binding is None && init_expr is None {
          return false
        }
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            if !bytecode_emit_expr(builder, expr) {
              builder.block_env_depth = builder.block_env_depth - 1
              return false
            }
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          None => return false
        }
      } else {
        if !bytecode_emit_expr(builder, expr) {
          return false
        }
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInInit))
      let loop_start = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInNext))
      let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            match (init_kind, binding) {
              (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
              }
              (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                )
              }
              _ => return false
            }
          }
          None => return false
        }
      } else {
        match init_name {
          Some(name) =>
            if init_is_var {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
              )
            } else {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::BindName, idx))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
              )
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            }
          None =>
            match init_expr {
              Some(expr) => {
                let idx = bytecode_add_assign_for_in(builder, expr)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                )
              }
              None =>
                match init_binding {
                  Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                      )
                    } else {
                      return false
                    }
                  Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                      )
                    } else {
                      return false
                    }
                  _ => return false
                }
            }
        }
      }
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        false,
        true,
        builder.block_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let continue_target = loop_start
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, continue_target)
          }
        None => ()
      }
      if init_is_lex {
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, exit_jump, exit_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      let cleanup_pc = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInEnd))
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      true
    }
    @engine.Stmt::ForOf(init, expr, body) => {
      let mut init_name : String? = None
      let mut init_kind : @engine.VarKind? = None
      let mut init_binding : @engine.VarBinding? = None
      let mut init_expr : @engine.Expr? = None
      let mut init_is_var = false
      let mut init_is_lex = false
      let mut init_has_init = false
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if decls.length() == 1 {
            let (binding, init_expr) = decls[0]
            init_has_init = init_expr is Some(_)
            init_kind = Some(kind)
            init_binding = Some(binding)
            if kind is @engine.VarKind::Var {
              init_is_var = true
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                @engine.VarBinding::ArrayPattern(_) => ()
                @engine.VarBinding::ObjectPattern(_) => ()
                _ => return false
              }
            } else {
              init_is_lex = true
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                _ => ()
              }
            }
          } else {
            return false
          }
        @engine.ForInit::Expr(inner) =>
          match unwrap_paren(inner) {
            @engine.Expr::Ident(name, _) => init_name = Some(name)
            _ => init_expr = Some(inner)
          }
        @engine.ForInit::None => return false
      }
      if init_is_var && init_has_init {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "a declaration in the head of a for-of loop can't have an initializer",
        )
        return true
      }
      if init_is_lex && init_has_init {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "a declaration in the head of a for-of loop can't have an initializer",
        )
        return true
      }
      if !init_is_lex {
        if init_name is None && init_binding is None && init_expr is None {
          return false
        }
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            if !bytecode_emit_expr(builder, expr) {
              builder.block_env_depth = builder.block_env_depth - 1
              return false
            }
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          None => return false
        }
      } else {
        if !bytecode_emit_expr(builder, expr) {
          return false
        }
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfInit))
      let loop_start = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfNext))
      let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            match (init_kind, binding) {
              (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
              }
              (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                )
              }
              _ => return false
            }
          }
          None => return false
        }
      } else {
        match init_name {
          Some(name) =>
            if init_is_var {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
              )
            } else {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::BindName, idx))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
              )
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            }
          None =>
            match init_expr {
              Some(expr) => {
                let idx = bytecode_add_assign_for_in(builder, expr)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                )
              }
              None =>
                match init_binding {
                  Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                      )
                    } else {
                      return false
                    }
                  Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                      )
                    } else {
                      return false
                    }
                  _ => return false
                }
            }
        }
      }
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        true,
        true,
        builder.block_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let continue_target = loop_start
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, continue_target)
          }
        None => ()
      }
      if init_is_lex {
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, exit_jump, exit_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      let cleanup_pc = bytecode_current_pc(builder)
      bytecode_emit_for_of_close(builder, 1)
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      true
    }
    @engine.Stmt::ForAwaitOf(init, expr, body) => {
      let mut init_name : String? = None
      let mut init_kind : @engine.VarKind? = None
      let mut init_binding : @engine.VarBinding? = None
      let mut init_expr : @engine.Expr? = None
      let mut init_is_var = false
      let mut init_is_lex = false
      let mut init_has_init = false
      match init {
        @engine.ForInit::Var(kind, decls) =>
          if decls.length() == 1 {
            let (binding, init_expr) = decls[0]
            init_has_init = init_expr is Some(_)
            init_kind = Some(kind)
            init_binding = Some(binding)
            if kind is @engine.VarKind::Var {
              init_is_var = true
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                @engine.VarBinding::ArrayPattern(_) => ()
                @engine.VarBinding::ObjectPattern(_) => ()
                _ => return false
              }
            } else {
              init_is_lex = true
              match binding {
                @engine.VarBinding::Name(name) => init_name = Some(name)
                _ => ()
              }
            }
          } else {
            return false
          }
        @engine.ForInit::Expr(inner) =>
          match unwrap_paren(inner) {
            @engine.Expr::Ident(name, _) => init_name = Some(name)
            _ => init_expr = Some(inner)
          }
        @engine.ForInit::None => return false
      }
      if init_is_var && init_has_init {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "a declaration in the head of a for-of loop can't have an initializer",
        )
        return true
      }
      if init_is_lex && init_has_init {
        bytecode_emit_throw(
          builder,
          BytecodeOp::ThrowSyntaxError,
          "a declaration in the head of a for-of loop can't have an initializer",
        )
        return true
      }
      if !init_is_lex {
        if init_name is None && init_binding is None && init_expr is None {
          return false
        }
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            if !bytecode_emit_expr(builder, expr) {
              builder.block_env_depth = builder.block_env_depth - 1
              return false
            }
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          None => return false
        }
      } else {
        if !bytecode_emit_expr(builder, expr) {
          return false
        }
      }
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForAwaitInit))
      let loop_start = bytecode_current_pc(builder)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfNext))
      let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      if init_is_lex {
        match init_binding {
          Some(binding) => {
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: [binding],
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            match (init_kind, binding) {
              (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
              }
              (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                let idx = bytecode_add_const(builder, String(name))
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                )
              }
              (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                )
              }
              _ => return false
            }
          }
          None => return false
        }
      } else {
        match init_name {
          Some(name) =>
            if init_is_var {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
              )
            } else {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::BindName, idx))
              bytecode_emit(
                builder,
                BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
              )
              bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            }
          None =>
            match init_expr {
              Some(expr) => {
                let idx = bytecode_add_assign_for_in(builder, expr)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                )
              }
              None =>
                match init_binding {
                  Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                      )
                    } else {
                      return false
                    }
                  Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                    if init_kind is Some(kind) {
                      let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                      bytecode_emit(
                        builder,
                        BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                      )
                    } else {
                      return false
                    }
                  _ => return false
                }
            }
        }
      }
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        true,
        true,
        builder.block_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let continue_target = loop_start
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, continue_target)
          }
        None => ()
      }
      if init_is_lex {
        builder.block_env_depth = builder.block_env_depth - 1
        bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, exit_jump, exit_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      let cleanup_pc = bytecode_current_pc(builder)
      bytecode_emit_for_of_close(builder, 1)
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      true
    }
    @engine.Stmt::DoWhile(body, cond_expr) => {
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
      }
      let loop_start = bytecode_current_pc(builder)
      loop_breaks.push([])
      loop_continues.push([])
      bytecode_loop_push(
        loop_for_of_depths,
        for_of_depth_ref,
        false,
        true,
        builder.block_env_depth,
      )
      if !bytecode_emit_stmt_with_lex_env(
        builder,
        loop_breaks,
        loop_continues,
        label_stack,
        body,
        result_needed,
        loop_for_of_depths,
        for_of_depth_ref,
      ) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      if result_needed {
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
        bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      }
      let continue_target = bytecode_current_pc(builder)
      if !bytecode_emit_expr(builder, cond_expr) {
        bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
        return false
      }
      let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      match loop_continues.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, continue_target)
          }
        None => ()
      }
      bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
      let exit_cond_pc = bytecode_current_pc(builder)
      bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
      let exit_pc = bytecode_current_pc(builder)
      match loop_breaks.pop() {
        Some(jumps) =>
          for idx in jumps {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
        None => ()
      }
      bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
      true
    }
    @engine.Stmt::Break(label_opt) =>
      match label_opt {
        Some(name) =>
          match bytecode_find_label(label_stack, name) {
            Some(
              (
                _,
                label_depth,
                label_is_for_of,
                break_env_depth,
                _continue_env_depth,
                breaks,
                _,
                label_index,
              ),
            ) => {
              let current_depth = bytecode_for_of_depth_get(for_of_depth_ref)
              let mut close_count = current_depth - label_depth
              if label_is_for_of {
                close_count = close_count - 1
              }
              if close_count < 0 {
                close_count = 0
              }
              let mut env_close_count = builder.block_env_depth - break_env_depth
              if env_close_count < 0 {
                env_close_count = 0
              }
              let mut use_finally = !builder.finally_stack.is_empty()
              let mut jump_to_dispatch = false
              if !builder.finally_body_scopes.is_empty() {
                let scope_index = builder.finally_body_scopes.length() - 1
                let (_, scope_label_depth) = builder.finally_body_scopes[scope_index]
                if label_index >= scope_label_depth {
                  use_finally = false
                } else {
                  jump_to_dispatch = true
                }
              }
              if use_finally {
                let top_index = builder.finally_stack.length() - 1
                let code = bytecode_finally_add_resume_at(
                  builder,
                  top_index,
                  breaks,
                  close_count,
                  env_close_count,
                  result_needed,
                )
                if jump_to_dispatch {
                  bytecode_finally_emit_dispatch_jump_at(
                    builder,
                    top_index,
                    code,
                    result_needed,
                  )
                } else {
                  bytecode_finally_emit_jump_at(
                    builder,
                    top_index,
                    code,
                    result_needed,
                  )
                }
              } else {
                bytecode_emit_for_of_close(builder, close_count)
                bytecode_emit_pop_env(builder, env_close_count)
                let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
                breaks.push(jump_index)
              }
              true
            }
            None => {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "illegal break",
              )
              true
            }
          }
        None =>
          if loop_breaks.is_empty() {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "illegal break",
            )
            true
          } else {
            let current_depth = bytecode_for_of_depth_get(for_of_depth_ref)
            let mut close_count = 0
            let mut env_close_count = 0
            match bytecode_break_for_of_entry(loop_for_of_depths) {
              Some((entry_depth, is_for_of, is_loop, entry_env_depth)) => {
                close_count = current_depth - entry_depth
                if is_loop && is_for_of {
                  close_count = close_count - 1
                }
                env_close_count = builder.block_env_depth - entry_env_depth
              }
              None => ()
            }
            if close_count < 0 {
              close_count = 0
            }
            if env_close_count < 0 {
              env_close_count = 0
            }
            let mut use_finally = !builder.finally_stack.is_empty()
            let mut jump_to_dispatch = false
            if !builder.finally_body_scopes.is_empty() {
              let scope_index = builder.finally_body_scopes.length() - 1
              let (scope_loop_depth, _) = builder.finally_body_scopes[scope_index]
              let target_index = loop_for_of_depths.length() - 1
              if target_index >= scope_loop_depth {
                use_finally = false
              } else {
                jump_to_dispatch = true
              }
            }
            if use_finally {
              let top_index = builder.finally_stack.length() - 1
              let targets = loop_breaks[loop_breaks.length() - 1]
              let code = bytecode_finally_add_resume_at(
                builder,
                top_index,
                targets,
                close_count,
                env_close_count,
                result_needed,
              )
              if jump_to_dispatch {
                bytecode_finally_emit_dispatch_jump_at(
                  builder,
                  top_index,
                  code,
                  result_needed,
                )
              } else {
                bytecode_finally_emit_jump_at(
                  builder,
                  top_index,
                  code,
                  result_needed,
                )
              }
            } else {
              bytecode_emit_for_of_close(builder, close_count)
              bytecode_emit_pop_env(builder, env_close_count)
              let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
              loop_breaks[loop_breaks.length() - 1].push(jump_index)
            }
            true
          }
      }
    @engine.Stmt::Continue(label_opt) =>
      match label_opt {
        Some(name) =>
          match bytecode_find_label(label_stack, name) {
            Some(
              (
                is_iteration,
                label_depth,
                label_is_for_of,
                _break_env_depth,
                continue_env_depth,
                _,
                continues,
                label_index,
              ),
            ) =>
              if is_iteration {
                let current_depth = bytecode_for_of_depth_get(for_of_depth_ref)
                let mut close_count = current_depth - label_depth
                if label_is_for_of {
                  close_count = close_count - 1
                }
                if close_count < 0 {
                  close_count = 0
                }
                let mut env_close_count =
                  builder.block_env_depth - continue_env_depth
                if env_close_count < 0 {
                  env_close_count = 0
                }
                let mut use_finally = !builder.finally_stack.is_empty()
                let mut jump_to_dispatch = false
                if !builder.finally_body_scopes.is_empty() {
                  let scope_index = builder.finally_body_scopes.length() - 1
                  let (_, scope_label_depth) = builder.finally_body_scopes[scope_index]
                  if label_index >= scope_label_depth {
                    use_finally = false
                  } else {
                    jump_to_dispatch = true
                  }
                }
                if use_finally {
                  let top_index = builder.finally_stack.length() - 1
                  let code = bytecode_finally_add_resume_at(
                    builder,
                    top_index,
                    continues,
                    close_count,
                    env_close_count,
                    result_needed,
                  )
                  if jump_to_dispatch {
                    bytecode_finally_emit_dispatch_jump_at(
                      builder,
                      top_index,
                      code,
                      result_needed,
                    )
                  } else {
                    bytecode_finally_emit_jump_at(
                      builder,
                      top_index,
                      code,
                      result_needed,
                    )
                  }
                } else {
                  bytecode_emit_for_of_close(builder, close_count)
                  bytecode_emit_pop_env(builder, env_close_count)
                  let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
                  continues.push(jump_index)
                }
                true
              } else {
                bytecode_emit_throw(
                  builder,
                  BytecodeOp::ThrowSyntaxError,
                  "illegal continue",
                )
                true
              }
            None => {
              bytecode_emit_throw(
                builder,
                BytecodeOp::ThrowSyntaxError,
                "illegal continue",
              )
              true
            }
          }
        None =>
          if loop_continues.is_empty() {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "illegal continue",
            )
            true
          } else {
            let current_depth = bytecode_for_of_depth_get(for_of_depth_ref)
            let mut close_count = 0
            let mut env_close_count = 0
            match bytecode_continue_for_of_entry(loop_for_of_depths) {
              Some((entry_depth, is_for_of, _, entry_env_depth)) => {
                close_count = current_depth - entry_depth
                if is_for_of {
                  close_count = close_count - 1
                }
                env_close_count = builder.block_env_depth - entry_env_depth
              }
              None => ()
            }
            if close_count < 0 {
              close_count = 0
            }
            if env_close_count < 0 {
              env_close_count = 0
            }
            let mut use_finally = !builder.finally_stack.is_empty()
            let mut jump_to_dispatch = false
            if !builder.finally_body_scopes.is_empty() {
              let scope_index = builder.finally_body_scopes.length() - 1
              let (scope_loop_depth, _) = builder.finally_body_scopes[scope_index]
              let mut target_index = loop_for_of_depths.length() - 1
              if target_index < 0 {
                target_index = 0
              }
              if target_index >= scope_loop_depth {
                use_finally = false
              } else {
                jump_to_dispatch = true
              }
            }
            if use_finally {
              let top_index = builder.finally_stack.length() - 1
              let targets = loop_continues[loop_continues.length() - 1]
              let code = bytecode_finally_add_resume_at(
                builder,
                top_index,
                targets,
                close_count,
                env_close_count,
                result_needed,
              )
              if jump_to_dispatch {
                bytecode_finally_emit_dispatch_jump_at(
                  builder,
                  top_index,
                  code,
                  result_needed,
                )
              } else {
                bytecode_finally_emit_jump_at(
                  builder,
                  top_index,
                  code,
                  result_needed,
                )
              }
            } else {
              bytecode_emit_for_of_close(builder, close_count)
              bytecode_emit_pop_env(builder, env_close_count)
              let jump_index = bytecode_emit_jump(builder, BytecodeOp::Jmp)
              loop_continues[loop_continues.length() - 1].push(jump_index)
            }
            true
          }
      }
    @engine.Stmt::Label(name, body) => {
      let is_iteration = label_targets_iteration(body)
      let label_is_for_of = label_targets_for_of(body)
      let label_depth = bytecode_for_of_depth_get(for_of_depth_ref)
      let label_breaks : Array[Int] = []
      let label_continues : Array[Int] = []
      label_stack.push(
        (
          name,
          is_iteration,
          label_depth,
          label_is_for_of,
          builder.block_env_depth,
          builder.block_env_depth,
          label_breaks,
          label_continues,
        ),
      )
      let label_index = label_stack.length() - 1
      let ok = match body {
        @engine.Stmt::While(cond_expr, inner) => {
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          let loop_start = bytecode_current_pc(builder)
          if !bytecode_emit_expr(builder, cond_expr) {
            let _ = label_stack.pop()
            return false
          }
          let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          loop_breaks.push([])
          loop_continues.push([])
          let loop_entry_env_depth = builder.block_env_depth
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            false,
            true,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let continue_target = loop_start
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, continue_target)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, continue_target)
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_cond_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          let exit_pc = bytecode_current_pc(builder)
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, exit_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          let _ = label_stack.pop()
          true
        }
        @engine.Stmt::For(init, test_expr, update_expr, inner) => {
          let mut use_loop_env = false
          let mut loop_kind : @engine.VarKind? = None
          let loop_bindings : Array[@engine.VarBinding] = []
          let mut loop_names : Array[String] = []
          match init {
            @engine.ForInit::Var(kind, decls) =>
              if kind is @engine.VarKind::Var {
                if !bytecode_emit_var_decl(builder, kind, decls) {
                  let _ = label_stack.pop()
                  return false
                }
              } else {
                use_loop_env = true
                loop_kind = Some(kind)
                for decl in decls {
                  let (binding, _) = decl
                  loop_bindings.push(binding)
                }
                loop_names = bytecode_for_init_binding_names(init)
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: loop_bindings,
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                if !bytecode_emit_var_decl(builder, kind, decls) {
                  builder.block_env_depth = builder.block_env_depth - 1
                  let _ = label_stack.pop()
                  return false
                }
              }
            @engine.ForInit::Expr(expr) =>
              if bytecode_emit_expr(builder, expr) {
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              } else {
                let _ = label_stack.pop()
                return false
              }
            @engine.ForInit::None => ()
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          let per_iter =
            use_loop_env &&
            for_loop_needs_per_iteration_env(init, test_expr, update_expr, inner)
          let loop_entry_env_depth = builder.block_env_depth
          let init_op = match loop_kind {
            Some(@engine.VarKind::ConstDecl) => BytecodeOp::InitConstName
            _ => BytecodeOp::InitName
          }
          if per_iter {
            for name in loop_names {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
            }
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: loop_bindings,
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            let mut i = loop_names.length()
            while i > 0 {
              i = i - 1
              let idx = bytecode_add_const(builder, String(loop_names[i]))
              bytecode_emit(builder, BytecodeInstr::Const(init_op, idx))
            }
          }
          let loop_start = bytecode_current_pc(builder)
          let mut exit_jump = -1
          match test_expr {
            Some(expr) =>
              if bytecode_emit_expr(builder, expr) {
                exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              } else {
                let _ = label_stack.pop()
                return false
              }
            None => ()
          }
          loop_breaks.push([])
          loop_continues.push([])
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            false,
            true,
            loop_entry_env_depth,
          )
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            if per_iter {
              builder.block_env_depth = builder.block_env_depth - 1
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            }
            if use_loop_env {
              builder.block_env_depth = builder.block_env_depth - 1
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            }
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let update_start = bytecode_current_pc(builder)
          if per_iter {
            for name in loop_names {
              let idx = bytecode_add_const(builder, String(name))
              bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::GetName, idx))
            }
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            let def = BlockEnvDef::{
              func_indices: [],
              lex_bindings: loop_bindings,
              class_names: [],
            }
            let env_idx = bytecode_add_block_env(builder, def)
            bytecode_emit(
              builder,
              BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
            )
            builder.block_env_depth = builder.block_env_depth + 1
            let mut i = loop_names.length()
            while i > 0 {
              i = i - 1
              let idx = bytecode_add_const(builder, String(loop_names[i]))
              bytecode_emit(builder, BytecodeInstr::Const(init_op, idx))
            }
          }
          match update_expr {
            Some(expr) =>
              if bytecode_emit_expr(builder, expr) {
                bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
              } else {
                bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
                if per_iter {
                  builder.block_env_depth = builder.block_env_depth - 1
                  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
                }
                if use_loop_env {
                  builder.block_env_depth = builder.block_env_depth - 1
                  bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
                }
                let _ = label_stack.pop()
                return false
              }
            None => ()
          }
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, update_start)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, update_start)
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_cond_pc = bytecode_current_pc(builder)
          if exit_jump >= 0 {
            bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
            if per_iter {
              bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
            }
          }
          let exit_pc = bytecode_current_pc(builder)
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, exit_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          if use_loop_env {
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          let _ = label_stack.pop()
          true
        }
        @engine.Stmt::ForIn(init, expr, inner) => {
          let mut init_name : String? = None
          let mut init_kind : @engine.VarKind? = None
          let mut init_binding : @engine.VarBinding? = None
          let mut init_expr : @engine.Expr? = None
          let mut init_var_decls : Array[(@engine.VarBinding, @engine.Expr?)] = []
          let mut init_is_var = false
          let mut init_is_lex = false
          let mut init_has_init = false
          match init {
            @engine.ForInit::Var(kind, decls) =>
              if decls.length() == 1 {
                let (binding, init_expr) = decls[0]
                init_has_init = init_expr is Some(_)
                init_kind = Some(kind)
                init_binding = Some(binding)
                if kind is @engine.VarKind::Var {
                  init_is_var = true
                  init_var_decls = decls
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    @engine.VarBinding::ArrayPattern(_) => ()
                    @engine.VarBinding::ObjectPattern(_) => ()
                    _ => {
                      let _ = label_stack.pop()
                      return false
                    }
                  }
                } else {
                  init_is_lex = true
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    _ => ()
                  }
                }
              } else {
                let _ = label_stack.pop()
                return false
              }
            @engine.ForInit::Expr(inner_expr) =>
              match unwrap_paren(inner_expr) {
                @engine.Expr::Ident(name, _) => init_name = Some(name)
                _ => init_expr = Some(inner_expr)
              }
            @engine.ForInit::None => {
              let _ = label_stack.pop()
              return false
            }
          }
          if init_is_var && init_has_init {
            match init_kind {
              Some(kind) =>
                if !bytecode_emit_var_decl(builder, kind, init_var_decls) {
                  let _ = label_stack.pop()
                  return false
                }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          }
          if init_is_lex && init_has_init {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "a declaration in the head of a for-in loop can't have an initializer",
            )
            let _ = label_stack.pop()
            return true
          }
          if !init_is_lex {
            if init_name is None &&
              init_binding is None &&
              init_expr is None {
              let _ = label_stack.pop()
              return false
            }
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                if !bytecode_emit_expr(builder, expr) {
                  builder.block_env_depth = builder.block_env_depth - 1
                  let _ = label_stack.pop()
                  return false
                }
                builder.block_env_depth = builder.block_env_depth - 1
                bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            if !bytecode_emit_expr(builder, expr) {
              let _ = label_stack.pop()
              return false
            }
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInInit))
          let loop_start = bytecode_current_pc(builder)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInNext))
          let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfNullish)
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                match (init_kind, binding) {
                  (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
                  }
                  (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                    )
                  }
                  _ => {
                    let _ = label_stack.pop()
                    return false
                  }
                }
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            match init_name {
              Some(name) =>
                if init_is_var {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
                  )
                } else {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::BindName, idx),
                  )
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
                  )
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                }
              None =>
                match init_expr {
                  Some(expr) => {
                    let idx = bytecode_add_assign_for_in(builder, expr)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                    )
                  }
                  None =>
                    match init_binding {
                      Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      _ => {
                        let _ = label_stack.pop()
                        return false
                      }
                    }
                }
            }
          }
          loop_breaks.push([])
          loop_continues.push([])
          let loop_entry_env_depth = builder.block_env_depth
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            false,
            true,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let continue_target = loop_start
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, continue_target)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, continue_target)
          }
          if init_is_lex {
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, exit_jump, exit_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          let cleanup_pc = bytecode_current_pc(builder)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForInEnd))
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, cleanup_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          let _ = label_stack.pop()
          true
        }
        @engine.Stmt::ForOf(init, expr, inner) => {
          let mut init_name : String? = None
          let mut init_kind : @engine.VarKind? = None
          let mut init_binding : @engine.VarBinding? = None
          let mut init_expr : @engine.Expr? = None
          let mut init_is_var = false
          let mut init_is_lex = false
          let mut init_has_init = false
          match init {
            @engine.ForInit::Var(kind, decls) =>
              if decls.length() == 1 {
                let (binding, init_expr) = decls[0]
                init_has_init = init_expr is Some(_)
                init_kind = Some(kind)
                init_binding = Some(binding)
                if kind is @engine.VarKind::Var {
                  init_is_var = true
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    @engine.VarBinding::ArrayPattern(_) => ()
                    @engine.VarBinding::ObjectPattern(_) => ()
                    _ => {
                      let _ = label_stack.pop()
                      return false
                    }
                  }
                } else {
                  init_is_lex = true
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    _ => ()
                  }
                }
              } else {
                let _ = label_stack.pop()
                return false
              }
            @engine.ForInit::Expr(inner_expr) =>
              match unwrap_paren(inner_expr) {
                @engine.Expr::Ident(name, _) => init_name = Some(name)
                _ => init_expr = Some(inner_expr)
              }
            @engine.ForInit::None => {
              let _ = label_stack.pop()
              return false
            }
          }
          if init_is_var && init_has_init {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "a declaration in the head of a for-of loop can't have an initializer",
            )
            let _ = label_stack.pop()
            return true
          }
          if init_is_lex && init_has_init {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "a declaration in the head of a for-of loop can't have an initializer",
            )
            let _ = label_stack.pop()
            return true
          }
          if !init_is_lex {
            if init_name is None &&
              init_binding is None &&
              init_expr is None {
              let _ = label_stack.pop()
              return false
            }
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                if !bytecode_emit_expr(builder, expr) {
                  builder.block_env_depth = builder.block_env_depth - 1
                  let _ = label_stack.pop()
                  return false
                }
                builder.block_env_depth = builder.block_env_depth - 1
                bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            if !bytecode_emit_expr(builder, expr) {
              let _ = label_stack.pop()
              return false
            }
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfInit))
          let loop_start = bytecode_current_pc(builder)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfNext))
          let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                match (init_kind, binding) {
                  (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
                  }
                  (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                    )
                  }
                  _ => {
                    let _ = label_stack.pop()
                    return false
                  }
                }
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            match init_name {
              Some(name) =>
                if init_is_var {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
                  )
                } else {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::BindName, idx),
                  )
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
                  )
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                }
              None =>
                match init_expr {
                  Some(expr) => {
                    let idx = bytecode_add_assign_for_in(builder, expr)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                    )
                  }
                  None =>
                    match init_binding {
                      Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      _ => {
                        let _ = label_stack.pop()
                        return false
                      }
                    }
                }
            }
          }
          loop_breaks.push([])
          loop_continues.push([])
          let loop_entry_env_depth = builder.block_env_depth
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            true,
            true,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let continue_target = loop_start
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, continue_target)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, continue_target)
          }
          if init_is_lex {
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, exit_jump, exit_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          let cleanup_pc = bytecode_current_pc(builder)
          bytecode_emit_for_of_close(builder, 1)
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, cleanup_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          let _ = label_stack.pop()
          true
        }
        @engine.Stmt::ForAwaitOf(init, expr, inner) => {
          let mut init_name : String? = None
          let mut init_kind : @engine.VarKind? = None
          let mut init_binding : @engine.VarBinding? = None
          let mut init_expr : @engine.Expr? = None
          let mut init_is_var = false
          let mut init_is_lex = false
          let mut init_has_init = false
          match init {
            @engine.ForInit::Var(kind, decls) =>
              if decls.length() == 1 {
                let (binding, init_expr) = decls[0]
                init_has_init = init_expr is Some(_)
                init_kind = Some(kind)
                init_binding = Some(binding)
                if kind is @engine.VarKind::Var {
                  init_is_var = true
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    @engine.VarBinding::ArrayPattern(_) => ()
                    @engine.VarBinding::ObjectPattern(_) => ()
                    _ => {
                      let _ = label_stack.pop()
                      return false
                    }
                  }
                } else {
                  init_is_lex = true
                  match binding {
                    @engine.VarBinding::Name(name) => init_name = Some(name)
                    _ => ()
                  }
                }
              } else {
                let _ = label_stack.pop()
                return false
              }
            @engine.ForInit::Expr(inner_expr) =>
              match unwrap_paren(inner_expr) {
                @engine.Expr::Ident(name, _) => init_name = Some(name)
                _ => init_expr = Some(inner_expr)
              }
            @engine.ForInit::None => {
              let _ = label_stack.pop()
              return false
            }
          }
          if init_is_var && init_has_init {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "a declaration in the head of a for-of loop can't have an initializer",
            )
            let _ = label_stack.pop()
            return true
          }
          if init_is_lex && init_has_init {
            bytecode_emit_throw(
              builder,
              BytecodeOp::ThrowSyntaxError,
              "a declaration in the head of a for-of loop can't have an initializer",
            )
            let _ = label_stack.pop()
            return true
          }
          if !init_is_lex {
            if init_name is None &&
              init_binding is None &&
              init_expr is None {
              let _ = label_stack.pop()
              return false
            }
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                if !bytecode_emit_expr(builder, expr) {
                  builder.block_env_depth = builder.block_env_depth - 1
                  let _ = label_stack.pop()
                  return false
                }
                builder.block_env_depth = builder.block_env_depth - 1
                bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            if !bytecode_emit_expr(builder, expr) {
              let _ = label_stack.pop()
              return false
            }
          }
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForAwaitInit))
          let loop_start = bytecode_current_pc(builder)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::ForOfNext))
          let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfTrue)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          if init_is_lex {
            match init_binding {
              Some(binding) => {
                let def = BlockEnvDef::{
                  func_indices: [],
                  lex_bindings: [binding],
                  class_names: [],
                }
                let env_idx = bytecode_add_block_env(builder, def)
                bytecode_emit(
                  builder,
                  BytecodeInstr::Const(BytecodeOp::EnterBlockEnv, env_idx),
                )
                builder.block_env_depth = builder.block_env_depth + 1
                match (init_kind, binding) {
                  (Some(@engine.VarKind::Let), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(builder, BytecodeInstr::Const(BytecodeOp::InitName, idx))
                  }
                  (Some(@engine.VarKind::ConstDecl), @engine.VarBinding::Name(name)) => {
                    let idx = bytecode_add_const(builder, String(name))
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::InitConstName, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ArrayPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                    )
                  }
                  (Some(kind), @engine.VarBinding::ObjectPattern(pattern)) => {
                    let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                    )
                  }
                  _ => {
                    let _ = label_stack.pop()
                    return false
                  }
                }
              }
              None => {
                let _ = label_stack.pop()
                return false
              }
            }
          } else {
            match init_name {
              Some(name) =>
                if init_is_var {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::VarInitName, idx),
                  )
                } else {
                  let idx = bytecode_add_const(builder, String(name))
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::BindName, idx),
                  )
                  bytecode_emit(
                    builder,
                    BytecodeInstr::Const(BytecodeOp::AssignNameNoName, idx),
                  )
                  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
                }
              None =>
                match init_expr {
                  Some(expr) => {
                    let idx = bytecode_add_assign_for_in(builder, expr)
                    bytecode_emit(
                      builder,
                      BytecodeInstr::Const(BytecodeOp::AssignForInExpr, idx),
                    )
                  }
                  None =>
                    match init_binding {
                      Some(@engine.VarBinding::ArrayPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_array(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclArray, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      Some(@engine.VarBinding::ObjectPattern(pattern)) =>
                        if init_kind is Some(kind) {
                          let idx = bytecode_add_var_decl_object(builder, kind, pattern)
                          bytecode_emit(
                            builder,
                            BytecodeInstr::Const(BytecodeOp::VarDeclObject, idx),
                          )
                        } else {
                          let _ = label_stack.pop()
                          return false
                        }
                      _ => {
                        let _ = label_stack.pop()
                        return false
                      }
                    }
                }
            }
          }
          loop_breaks.push([])
          loop_continues.push([])
          let loop_entry_env_depth = builder.block_env_depth
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            true,
            true,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let continue_target = loop_start
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, continue_target)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, continue_target)
          }
          if init_is_lex {
            builder.block_env_depth = builder.block_env_depth - 1
            bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::PopEnv, 1))
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, exit_jump, exit_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          let cleanup_pc = bytecode_current_pc(builder)
          bytecode_emit_for_of_close(builder, 1)
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, cleanup_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, cleanup_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          let _ = label_stack.pop()
          true
        }
        @engine.Stmt::DoWhile(inner, cond_expr) => {
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          let loop_start = bytecode_current_pc(builder)
          loop_breaks.push([])
          loop_continues.push([])
          let loop_entry_env_depth = builder.block_env_depth
          bytecode_label_update_continue_env(
            label_stack,
            label_index,
            loop_entry_env_depth,
          )
          bytecode_loop_push(
            loop_for_of_depths,
            for_of_depth_ref,
            false,
            true,
            loop_entry_env_depth,
          )
          if !bytecode_emit_stmt_with_lex_env(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            inner,
            result_needed,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let continue_target = bytecode_current_pc(builder)
          if !bytecode_emit_expr(builder, cond_expr) {
            bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
            let _ = label_stack.pop()
            return false
          }
          let exit_jump = bytecode_emit_jump(builder, BytecodeOp::JmpIfFalse)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          match loop_continues.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, continue_target)
              }
            None => ()
          }
          for idx in label_continues {
            bytecode_patch_jump(builder, idx, continue_target)
          }
          bytecode_emit(builder, BytecodeInstr::I32(BytecodeOp::Jmp, loop_start))
          let exit_cond_pc = bytecode_current_pc(builder)
          bytecode_patch_jump(builder, exit_jump, exit_cond_pc)
          bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          let exit_pc = bytecode_current_pc(builder)
          match loop_breaks.pop() {
            Some(jumps) =>
              for idx in jumps {
                bytecode_patch_jump(builder, idx, exit_pc)
              }
            None => ()
          }
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
          bytecode_loop_pop(loop_for_of_depths, for_of_depth_ref)
          let _ = label_stack.pop()
          true
        }
        _ => {
          let updates = stmt_updates_completion(body)
          if result_needed {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
          }
          if !bytecode_emit_stmt(
            builder,
            loop_breaks,
            loop_continues,
            label_stack,
            body,
            result_needed && updates,
            loop_for_of_depths,
            for_of_depth_ref,
          ) {
            let _ = label_stack.pop()
            return false
          }
          if result_needed && updates {
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Swap))
            bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
          }
          let exit_pc = bytecode_current_pc(builder)
          for idx in label_breaks {
            bytecode_patch_jump(builder, idx, exit_pc)
          }
          if !label_continues.is_empty() {
            let _ = label_stack.pop()
            return false
          }
          let _ = label_stack.pop()
          true
        }
      }
      ok
    }
  }
}

///|
fn bytecode_compile_script(
  script : @engine.Script,
) -> BytecodeFunction? {
  if script.is_module {
    return None
  }
  let stmts = script.body
  let builder = bytecode_builder_new()
  let loop_breaks : Array[Array[Int]] = []
  let loop_continues : Array[Array[Int]] = []
  let loop_for_of_depths : Array[LoopForOfEntry] = []
  let for_of_depth_ref : Ref[Int] = Ref::new(0)
  let label_stack : Array[LabelEntry] = []
  let mut last_completion_index = -1
  let mut scan = 0
  while scan < stmts.length() {
    if stmt_updates_completion(stmts[scan]) {
      last_completion_index = scan
    }
    scan = scan + 1
  }
  let mut i = 0
  while i < stmts.length() {
    let updates = stmt_updates_completion(stmts[i])
    if !bytecode_emit_stmt(
      builder,
      loop_breaks,
      loop_continues,
      label_stack,
      stmts[i],
      updates,
      loop_for_of_depths,
      for_of_depth_ref,
    ) {
      return None
    }
    if updates && i != last_completion_index {
      bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Pop))
    }
    i = i + 1
  }
  if last_completion_index < 0 {
    bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Return))
  Some(bytecode_finish(
    builder,
    0,
    0,
    BytecodeDebug::{ filename: None, source_len: script.source.length(), pc2line: [] },
  ))
}

///|
fn bytecode_compile_function_body(
  func : FunctionValue,
) -> BytecodeFunction? {
  match bytecode_cache_get(func.id) {
    Some(code) => return Some(code)
    None => ()
  }
  let builder = bytecode_builder_new()
  builder.allow_return = true
  builder.allow_yield = func.is_generator
  builder.allow_await = func.is_async
  let loop_breaks : Array[Array[Int]] = []
  let loop_continues : Array[Array[Int]] = []
  let loop_for_of_depths : Array[LoopForOfEntry] = []
  let for_of_depth_ref : Ref[Int] = Ref::new(0)
  let label_stack : Array[LabelEntry] = []
  let mut i = 0
  while i < func.body.length() {
    if !bytecode_emit_stmt(
      builder,
      loop_breaks,
      loop_continues,
      label_stack,
      func.body[i],
      false,
      loop_for_of_depths,
      for_of_depth_ref,
    ) {
      return None
    }
    i = i + 1
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
  let return_op = if func.is_async && func.is_generator {
    BytecodeOp::ReturnNoAwait
  } else {
    BytecodeOp::Return
  }
  bytecode_emit(builder, BytecodeInstr::Op(return_op))
  let source_len = match func.source {
    Some(source) => source.length()
    None => 0
  }
  let compiled = bytecode_finish(
    builder,
    func.params.length(),
    0,
    BytecodeDebug::{ filename: func.source_path, source_len, pc2line: [] },
  )
  bytecode_cache_set(func.id, compiled)
  Some(compiled)
}

///|
fn bytecode_compile_expr(
  expr : @engine.Expr,
  allow_yield : Bool,
  allow_await : Bool,
) -> BytecodeFunction? {
  let builder = bytecode_builder_new()
  builder.allow_return = true
  builder.allow_yield = allow_yield
  builder.allow_await = allow_await
  if !bytecode_emit_expr(builder, expr) {
    return None
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Return))
  Some(bytecode_finish(
    builder,
    0,
    0,
    BytecodeDebug::{ filename: None, source_len: 0, pc2line: [] },
  ))
}

///|
fn bytecode_compile_block(
  stmts : Array[@engine.Stmt],
  allow_yield : Bool,
  allow_await : Bool,
) -> BytecodeFunction? {
  let builder = bytecode_builder_new()
  builder.allow_return = false
  builder.allow_yield = allow_yield
  builder.allow_await = allow_await
  let loop_breaks : Array[Array[Int]] = []
  let loop_continues : Array[Array[Int]] = []
  let loop_for_of_depths : Array[LoopForOfEntry] = []
  let for_of_depth_ref : Ref[Int] = Ref::new(0)
  let label_stack : Array[LabelEntry] = []
  let mut i = 0
  while i < stmts.length() {
    if !bytecode_emit_stmt(
      builder,
      loop_breaks,
      loop_continues,
      label_stack,
      stmts[i],
      false,
      loop_for_of_depths,
      for_of_depth_ref,
    ) {
      return None
    }
    i = i + 1
  }
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::PushUndefined))
  bytecode_emit(builder, BytecodeInstr::Op(BytecodeOp::Return))
  Some(bytecode_finish(
    builder,
    0,
    0,
    BytecodeDebug::{ filename: None, source_len: 0, pc2line: [] },
  ))
}
