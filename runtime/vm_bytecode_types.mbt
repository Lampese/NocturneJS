///|
#warnings("-unused_constructor")
enum BytecodeOp {
  Nop
  Pop
  Dup
  Dup2
  Swap
  PushI32
  PushConst
  PushTrue
  PushFalse
  PushNull
  PushUndefined
  GetName
  TypeofName
  BindName
  AssignName
  AssignNameNoName
  BindNameTarget
  GetNameFromTarget
  AssignFromTarget
  PopTarget
  SetAnonName
  VarInitName
  InitName
  InitConstName
  GetProp
  SetProp
  GetElem
  GetElemRef
  SetElem
  GetPrivate
  SetPrivate
  DeleteName
  DeleteProp
  DeleteElem
  NewObject
  InitProp
  InitPropNamed
  InitPropComputed
  InitPropComputedNamed
  InitMethod
  InitMethodComputed
  InitGetter
  InitGetterComputed
  InitSetter
  InitSetterComputed
  ObjectSpread
  SetProto
  NewArray
  ArrayPush
  ArrayPushHole
  ArraySpread
  UpdateNamePrefixInc
  UpdateNamePrefixDec
  UpdateNamePostfixInc
  UpdateNamePostfixDec
  UpdatePropPrefixInc
  UpdatePropPrefixDec
  UpdatePropPostfixInc
  UpdatePropPostfixDec
  UpdatePrivatePrefixInc
  UpdatePrivatePrefixDec
  UpdatePrivatePostfixInc
  UpdatePrivatePostfixDec
  UpdateElemPrefixInc
  UpdateElemPrefixDec
  UpdateElemPostfixInc
  UpdateElemPostfixDec
  ForInInit
  ForInNext
  ForInEnd
  ForOfInit
  ForAwaitInit
  ForOfNext
  ForOfClose
  GetCallRef
  NewFunction
  NewClass
  OptionalChain
  OptionalChainCallRef
  DeleteOptionalChain
  TaggedTemplate
  SuperMember
  SuperMemberCall
  SuperCall
  SuperCallArgs
  CheckSuperThis
  SuperSetProp
  SuperSetElem
  SuperGetElem
  SuperUpdate
  AssignOpEval
  DeleteSuper
  PrivateIn
  AssignArray
  AssignObject
  AssignInvalid
  AssignForInExpr
  VarDeclArray
  VarDeclObject
  DeclareBinding
  ThrowSyntaxError
  ThrowReferenceError
  ThrowTypeError
  NewTarget
  ImportMeta
  NewRegexp
  Unary
  Binary
  ToStringStrict
  ToPropertyKey
  Call
  CallArgs
  New
  NewArgs
  CallWithThis
  CallWithThisArgs
  CallWithThisEval
  CallWithThisEvalArgs
  EnterWithEnv
  EnterBlockEnv
  EnterTry
  LeaveTry
  UpdateTryLast
  PushTryLast
  PopTryLast
  MarkCatchEnv
  EnterFinally
  LeaveFinally
  SetFinallyCode
  SetFinallyValue
  GetFinallyCode
  PushFinallyValue
  PopEnv
  FunctionDecl
  Await
  Yield
  YieldStar
  Throw
  Jmp
  JmpIfTrue
  JmpIfFalse
  JmpIfNullish
  ReturnNoAwait
  Return
} derive(Show, Eq, ToJson)

///|
#warnings("-unused_constructor")
enum BytecodeInstr {
  Op(BytecodeOp)
  I32(BytecodeOp, Int)
  Const(BytecodeOp, Int)
} derive(Show, Eq, ToJson)

struct BytecodeDebug {
  filename : String?
  source_len : Int
  pc2line : Array[(Int, Int)]
} derive(Show, Eq, ToJson)

struct OptionalChainDef {
  base : @engine.Expr
  segments : Array[@engine.ChainSegment]
} derive(Show, Eq, ToJson)

struct TaggedTemplateDef {
  callee : @engine.Expr
  parts : @engine.TemplateParts
  exprs : Array[@engine.Expr]
} derive(Show, Eq, ToJson)

struct BytecodeClassDef {
  class_def : @engine.ClassDef
  name_hint : String?
} derive(Show, Eq, ToJson)

struct SuperMemberDef {
  key : @engine.MemberKey
} derive(Show, Eq, ToJson)

struct SuperMemberCallDef {
  key : @engine.MemberKey
  args : Array[@engine.Expr]
} derive(Show, Eq, ToJson)

struct SuperCallDef {
  args : Array[@engine.Expr]
} derive(Show, Eq, ToJson)

struct SuperUpdateDef {
  op : @engine.UpdateOp
  kind : @engine.UpdateKind
  key : @engine.MemberKey
} derive(Show, Eq, ToJson)

struct AssignOpDef {
  op : @engine.BinaryOp
  lhs : @engine.Expr
  rhs : @engine.Expr
} derive(Show, Eq, ToJson)

struct SuperDeleteDef {
  key : @engine.MemberKey
} derive(Show, Eq, ToJson)

struct AssignArrayDef {
  pattern : Array[@engine.ArrayPatternElem]
  rhs : @engine.Expr
} derive(Show, Eq, ToJson)

struct AssignObjectDef {
  pattern : Array[@engine.ObjectPatternElem]
  rhs : @engine.Expr
} derive(Show, Eq, ToJson)

struct AssignInvalidDef {
  lhs : @engine.Expr
  offset : Int
} derive(Show, Eq, ToJson)

struct AssignForInDef {
  expr : @engine.Expr
} derive(Show, Eq, ToJson)

struct YieldStarDef {
  expr : @engine.Expr
} derive(Show, Eq, ToJson)

struct VarDeclArrayDef {
  kind : @engine.VarKind
  pattern : Array[@engine.ArrayPatternElem]
} derive(Show, Eq, ToJson)

struct VarDeclObjectDef {
  kind : @engine.VarKind
  pattern : Array[@engine.ObjectPatternElem]
} derive(Show, Eq, ToJson)

struct DeclareBindingDef {
  binding : @engine.VarBinding
} derive(Show, Eq, ToJson)

struct BlockEnvDef {
  func_indices : Array[Int]
  lex_bindings : Array[@engine.VarBinding]
  class_names : Array[String]
} derive(Show, Eq, ToJson)

///|
priv struct FinallyResumeEntry {
  code : Int
  targets : Array[Int]
  close_for_of : Int
  pop_env : Int
  restore_value : Bool
}

///|
priv struct FinallyContext {
  jump_indices : Array[Int]
  dispatch_jump_indices : Array[Int]
  resume_entries : Array[FinallyResumeEntry]
  mut next_code : Int
  normal_restore : Bool
}

///|
struct BytecodeFunction {
  code : Array[BytecodeInstr]
  cpool : Array[Value]
  func_pool : Array[@engine.FunctionDef]
  class_pool : Array[BytecodeClassDef]
  optional_chain_pool : Array[OptionalChainDef]
  tagged_template_pool : Array[TaggedTemplateDef]
  super_member_pool : Array[SuperMemberDef]
  super_member_call_pool : Array[SuperMemberCallDef]
  super_call_pool : Array[SuperCallDef]
  super_update_pool : Array[SuperUpdateDef]
  assign_op_pool : Array[AssignOpDef]
  super_delete_pool : Array[SuperDeleteDef]
  assign_array_pool : Array[AssignArrayDef]
  assign_object_pool : Array[AssignObjectDef]
  assign_invalid_pool : Array[AssignInvalidDef]
  assign_for_in_pool : Array[AssignForInDef]
  yield_star_pool : Array[YieldStarDef]
  var_decl_array_pool : Array[VarDeclArrayDef]
  var_decl_object_pool : Array[VarDeclObjectDef]
  declare_binding_pool : Array[DeclareBindingDef]
  block_env_pool : Array[BlockEnvDef]
  finally_pc_pool : Array[(Int, Int, Int)]
  arg_count : Int
  var_count : Int
  stack_size : Int
  debug : BytecodeDebug
} derive(Show, ToJson)

///|
priv struct BytecodeBuilder {
  code : Array[BytecodeInstr]
  cpool : Array[Value]
  func_pool : Array[@engine.FunctionDef]
  class_pool : Array[BytecodeClassDef]
  optional_chain_pool : Array[OptionalChainDef]
  tagged_template_pool : Array[TaggedTemplateDef]
  super_member_pool : Array[SuperMemberDef]
  super_member_call_pool : Array[SuperMemberCallDef]
  super_call_pool : Array[SuperCallDef]
  super_update_pool : Array[SuperUpdateDef]
  assign_op_pool : Array[AssignOpDef]
  super_delete_pool : Array[SuperDeleteDef]
  assign_array_pool : Array[AssignArrayDef]
  assign_object_pool : Array[AssignObjectDef]
  assign_invalid_pool : Array[AssignInvalidDef]
  assign_for_in_pool : Array[AssignForInDef]
  yield_star_pool : Array[YieldStarDef]
  var_decl_array_pool : Array[VarDeclArrayDef]
  var_decl_object_pool : Array[VarDeclObjectDef]
  declare_binding_pool : Array[DeclareBindingDef]
  block_env_pool : Array[BlockEnvDef]
  finally_pc_pool : Array[(Int, Int, Int)]
  mut allow_return : Bool
  mut allow_yield : Bool
  mut allow_await : Bool
  mut max_stack : Int
  mut stack_size : Int
  mut block_env_depth : Int
  mut try_depth : Int
  mut try_stack_depth : Int
  try_body_base_depths : Array[Int]
  finally_stack : Array[FinallyContext]
  finally_body_scopes : Array[(Int, Int)]
}

///|
fn bytecode_builder_new() -> BytecodeBuilder {
  BytecodeBuilder::{
    code: [],
    cpool: [],
    func_pool: [],
    class_pool: [],
    optional_chain_pool: [],
    tagged_template_pool: [],
    super_member_pool: [],
    super_member_call_pool: [],
    super_call_pool: [],
    super_update_pool: [],
    assign_op_pool: [],
    super_delete_pool: [],
    assign_array_pool: [],
    assign_object_pool: [],
    assign_invalid_pool: [],
    assign_for_in_pool: [],
    yield_star_pool: [],
    var_decl_array_pool: [],
    var_decl_object_pool: [],
    declare_binding_pool: [],
    block_env_pool: [],
    finally_pc_pool: [],
    allow_return: false,
    allow_yield: false,
    allow_await: false,
    max_stack: 0,
    stack_size: 0,
    block_env_depth: 0,
    try_depth: 0,
    try_stack_depth: 0,
    try_body_base_depths: [],
    finally_stack: [],
    finally_body_scopes: [],
  }
}

///|
fn bytecode_add_const(builder : BytecodeBuilder, value : Value) -> Int {
  let index = builder.cpool.length()
  builder.cpool.push(value)
  index
}

///|
fn bytecode_add_func(
  builder : BytecodeBuilder,
  func_def : @engine.FunctionDef,
) -> Int {
  let index = builder.func_pool.length()
  builder.func_pool.push(func_def)
  index
}

///|
fn bytecode_add_class(
  builder : BytecodeBuilder,
  class_def : @engine.ClassDef,
  name_hint? : String? = None,
) -> Int {
  let index = builder.class_pool.length()
  builder.class_pool.push(BytecodeClassDef::{ class_def, name_hint })
  index
}

///|
fn bytecode_add_optional_chain(
  builder : BytecodeBuilder,
  base : @engine.Expr,
  segments : Array[@engine.ChainSegment],
) -> Int {
  let index = builder.optional_chain_pool.length()
  builder.optional_chain_pool.push(OptionalChainDef::{ base, segments })
  index
}

///|
///|
fn bytecode_add_tagged_template(
  builder : BytecodeBuilder,
  callee : @engine.Expr,
  parts : @engine.TemplateParts,
  exprs : Array[@engine.Expr],
) -> Int {
  let index = builder.tagged_template_pool.length()
  builder.tagged_template_pool.push(
    TaggedTemplateDef::{ callee, parts, exprs },
  )
  index
}

///|
fn bytecode_add_super_member(
  builder : BytecodeBuilder,
  key : @engine.MemberKey,
) -> Int {
  let index = builder.super_member_pool.length()
  builder.super_member_pool.push(SuperMemberDef::{ key })
  index
}

///|
fn bytecode_add_super_member_call(
  builder : BytecodeBuilder,
  key : @engine.MemberKey,
  args : Array[@engine.Expr],
) -> Int {
  let index = builder.super_member_call_pool.length()
  builder.super_member_call_pool.push(SuperMemberCallDef::{ key, args })
  index
}

///|
fn bytecode_add_super_call(
  builder : BytecodeBuilder,
  args : Array[@engine.Expr],
) -> Int {
  let index = builder.super_call_pool.length()
  builder.super_call_pool.push(SuperCallDef::{ args })
  index
}

///|
fn bytecode_add_super_update(
  builder : BytecodeBuilder,
  op : @engine.UpdateOp,
  kind : @engine.UpdateKind,
  key : @engine.MemberKey,
) -> Int {
  let index = builder.super_update_pool.length()
  builder.super_update_pool.push(SuperUpdateDef::{ op, kind, key })
  index
}

///|
fn bytecode_add_assign_op(
  builder : BytecodeBuilder,
  op : @engine.BinaryOp,
  lhs : @engine.Expr,
  rhs : @engine.Expr,
) -> Int {
  let index = builder.assign_op_pool.length()
  builder.assign_op_pool.push(AssignOpDef::{ op, lhs, rhs })
  index
}

///|
fn bytecode_add_super_delete(
  builder : BytecodeBuilder,
  key : @engine.MemberKey,
) -> Int {
  let index = builder.super_delete_pool.length()
  builder.super_delete_pool.push(SuperDeleteDef::{ key })
  index
}

///|
fn bytecode_add_assign_array(
  builder : BytecodeBuilder,
  pattern : Array[@engine.ArrayPatternElem],
  rhs : @engine.Expr,
) -> Int {
  let index = builder.assign_array_pool.length()
  builder.assign_array_pool.push(AssignArrayDef::{ pattern, rhs })
  index
}

///|
fn bytecode_add_assign_object(
  builder : BytecodeBuilder,
  pattern : Array[@engine.ObjectPatternElem],
  rhs : @engine.Expr,
) -> Int {
  let index = builder.assign_object_pool.length()
  builder.assign_object_pool.push(AssignObjectDef::{ pattern, rhs })
  index
}

///|
fn bytecode_add_assign_invalid(
  builder : BytecodeBuilder,
  lhs : @engine.Expr,
  offset : Int,
) -> Int {
  let index = builder.assign_invalid_pool.length()
  builder.assign_invalid_pool.push(AssignInvalidDef::{ lhs, offset })
  index
}

///|
fn bytecode_add_assign_for_in(
  builder : BytecodeBuilder,
  expr : @engine.Expr,
) -> Int {
  let index = builder.assign_for_in_pool.length()
  builder.assign_for_in_pool.push(AssignForInDef::{ expr })
  index
}

///|
fn bytecode_add_yield_star(
  builder : BytecodeBuilder,
  expr : @engine.Expr,
) -> Int {
  let index = builder.yield_star_pool.length()
  builder.yield_star_pool.push(YieldStarDef::{ expr })
  index
}

///|
fn bytecode_add_var_decl_array(
  builder : BytecodeBuilder,
  kind : @engine.VarKind,
  pattern : Array[@engine.ArrayPatternElem],
) -> Int {
  let index = builder.var_decl_array_pool.length()
  builder.var_decl_array_pool.push(VarDeclArrayDef::{ kind, pattern })
  index
}

///|
fn bytecode_add_var_decl_object(
  builder : BytecodeBuilder,
  kind : @engine.VarKind,
  pattern : Array[@engine.ObjectPatternElem],
) -> Int {
  let index = builder.var_decl_object_pool.length()
  builder.var_decl_object_pool.push(VarDeclObjectDef::{ kind, pattern })
  index
}

///|
fn bytecode_add_declare_binding(
  builder : BytecodeBuilder,
  binding : @engine.VarBinding,
) -> Int {
  let index = builder.declare_binding_pool.length()
  builder.declare_binding_pool.push(DeclareBindingDef::{ binding })
  index
}

///|
fn bytecode_add_block_env(
  builder : BytecodeBuilder,
  def : BlockEnvDef,
) -> Int {
  let index = builder.block_env_pool.length()
  builder.block_env_pool.push(def)
  index
}

///|
fn bytecode_adjust_stack(builder : BytecodeBuilder, delta : Int) -> Unit {
  let next = builder.stack_size + delta
  builder.stack_size = if next < 0 { 0 } else { next }
  if builder.stack_size > builder.max_stack {
    builder.max_stack = builder.stack_size
  }
}

///|
fn bytecode_emit(builder : BytecodeBuilder, instr : BytecodeInstr) -> Unit {
  match instr {
    BytecodeInstr::Op(op) => {
      match op {
        BytecodeOp::PushTrue
        | BytecodeOp::PushFalse
        | BytecodeOp::PushNull
        | BytecodeOp::PushUndefined
        | BytecodeOp::NewTarget
        | BytecodeOp::ImportMeta
        | BytecodeOp::NewObject
        | BytecodeOp::PushTryLast
        | BytecodeOp::GetFinallyCode
        | BytecodeOp::PushFinallyValue =>
          bytecode_adjust_stack(builder, 1)
        BytecodeOp::Pop => bytecode_adjust_stack(builder, -1)
        BytecodeOp::Dup => bytecode_adjust_stack(builder, 1)
        BytecodeOp::Dup2 => bytecode_adjust_stack(builder, 2)
        BytecodeOp::SetFinallyValue => bytecode_adjust_stack(builder, -1)
        BytecodeOp::GetElem => bytecode_adjust_stack(builder, -1)
        BytecodeOp::GetElemRef => bytecode_adjust_stack(builder, 1)
        BytecodeOp::SetElem => bytecode_adjust_stack(builder, -2)
        BytecodeOp::DeleteElem => bytecode_adjust_stack(builder, -1)
        BytecodeOp::ArrayPush
        | BytecodeOp::ArraySpread
        | BytecodeOp::ObjectSpread
        | BytecodeOp::SetProto =>
          bytecode_adjust_stack(builder, -1)
        BytecodeOp::SuperSetElem => bytecode_adjust_stack(builder, -1)
        BytecodeOp::SuperGetElem => ()
        BytecodeOp::InitPropComputed
        | BytecodeOp::InitPropComputedNamed
        | BytecodeOp::InitMethodComputed
        | BytecodeOp::InitGetterComputed
        | BytecodeOp::InitSetterComputed =>
          bytecode_adjust_stack(builder, -2)
        BytecodeOp::CallArgs => bytecode_adjust_stack(builder, -1)
        BytecodeOp::NewArgs => bytecode_adjust_stack(builder, -1)
        BytecodeOp::CallWithThisArgs
        | BytecodeOp::CallWithThisEvalArgs =>
          bytecode_adjust_stack(builder, -2)
        BytecodeOp::UpdateElemPrefixInc
        | BytecodeOp::UpdateElemPrefixDec
        | BytecodeOp::UpdateElemPostfixInc
        | BytecodeOp::UpdateElemPostfixDec =>
          bytecode_adjust_stack(builder, -1)
        BytecodeOp::EnterWithEnv => bytecode_adjust_stack(builder, -1)
        BytecodeOp::ForInInit => bytecode_adjust_stack(builder, -1)
        BytecodeOp::ForAwaitInit => bytecode_adjust_stack(builder, -1)
        BytecodeOp::ForInNext => bytecode_adjust_stack(builder, 1)
        BytecodeOp::ForOfInit => bytecode_adjust_stack(builder, -1)
        BytecodeOp::ForOfNext => bytecode_adjust_stack(builder, 2)
        BytecodeOp::Return
        | BytecodeOp::ReturnNoAwait =>
          bytecode_adjust_stack(builder, -1)
        BytecodeOp::Throw => bytecode_adjust_stack(builder, -1)
        _ => ()
      }
    }
    BytecodeInstr::I32(op, value) =>
      match op {
        BytecodeOp::PushI32 => bytecode_adjust_stack(builder, 1)
        BytecodeOp::Unary => ()
        BytecodeOp::Binary => bytecode_adjust_stack(builder, -1)
        BytecodeOp::Call => bytecode_adjust_stack(builder, -value)
        BytecodeOp::New => bytecode_adjust_stack(builder, -value)
        BytecodeOp::CallWithThis
        | BytecodeOp::CallWithThisEval =>
          bytecode_adjust_stack(builder, -value - 1)
        BytecodeOp::PopEnv => ()
        BytecodeOp::NewRegexp => bytecode_adjust_stack(builder, -1)
        BytecodeOp::NewArray => bytecode_adjust_stack(builder, -value + 1)
        BytecodeOp::ArrayPushHole => ()
        BytecodeOp::Jmp
        | BytecodeOp::JmpIfTrue
        | BytecodeOp::JmpIfFalse
        | BytecodeOp::JmpIfNullish =>
          ()
        _ => ()
      }
    BytecodeInstr::Const(op, _) =>
      match op {
        BytecodeOp::PushConst
        | BytecodeOp::GetName
        | BytecodeOp::TypeofName
        | BytecodeOp::GetNameFromTarget
        | BytecodeOp::UpdateNamePrefixInc
        | BytecodeOp::UpdateNamePrefixDec
        | BytecodeOp::UpdateNamePostfixInc
        | BytecodeOp::UpdateNamePostfixDec
        | BytecodeOp::DeleteName
        | BytecodeOp::DeleteSuper
        | BytecodeOp::NewFunction
        | BytecodeOp::NewClass
        | BytecodeOp::OptionalChain
        | BytecodeOp::DeleteOptionalChain
        | BytecodeOp::TaggedTemplate
        | BytecodeOp::SuperMember
        | BytecodeOp::SuperCall
        | BytecodeOp::SuperUpdate
        | BytecodeOp::AssignArray
        | BytecodeOp::AssignObject
        | BytecodeOp::AssignInvalid
        | BytecodeOp::YieldStar =>
          bytecode_adjust_stack(builder, 1)
        BytecodeOp::SuperMemberCall => bytecode_adjust_stack(builder, 2)
        BytecodeOp::AssignOpEval => bytecode_adjust_stack(builder, 1)
        BytecodeOp::EnterBlockEnv => ()
        BytecodeOp::VarInitName
        | BytecodeOp::InitName
        | BytecodeOp::InitConstName
        | BytecodeOp::AssignForInExpr
        | BytecodeOp::VarDeclArray
        | BytecodeOp::VarDeclObject =>
          bytecode_adjust_stack(builder, -1)
        BytecodeOp::SetProp
        | BytecodeOp::InitProp
        | BytecodeOp::InitPropNamed
        | BytecodeOp::InitMethod
        | BytecodeOp::InitGetter
        | BytecodeOp::InitSetter
        | BytecodeOp::SetPrivate =>
          bytecode_adjust_stack(builder, -1)
        BytecodeOp::GetProp => ()
        BytecodeOp::GetCallRef => bytecode_adjust_stack(builder, 2)
        BytecodeOp::OptionalChainCallRef => bytecode_adjust_stack(builder, 2)
        _ => ()
      }
  }
  builder.code.push(instr)
}

///|
fn bytecode_finish(
  builder : BytecodeBuilder,
  arg_count : Int,
  var_count : Int,
  debug : BytecodeDebug,
) -> BytecodeFunction {
  BytecodeFunction::{
    code: builder.code,
    cpool: builder.cpool,
    func_pool: builder.func_pool,
    class_pool: builder.class_pool,
    optional_chain_pool: builder.optional_chain_pool,
    tagged_template_pool: builder.tagged_template_pool,
    super_member_pool: builder.super_member_pool,
    super_member_call_pool: builder.super_member_call_pool,
    super_call_pool: builder.super_call_pool,
    super_update_pool: builder.super_update_pool,
    assign_op_pool: builder.assign_op_pool,
    super_delete_pool: builder.super_delete_pool,
    assign_array_pool: builder.assign_array_pool,
    assign_object_pool: builder.assign_object_pool,
    assign_invalid_pool: builder.assign_invalid_pool,
    assign_for_in_pool: builder.assign_for_in_pool,
    yield_star_pool: builder.yield_star_pool,
    var_decl_array_pool: builder.var_decl_array_pool,
    var_decl_object_pool: builder.var_decl_object_pool,
    declare_binding_pool: builder.declare_binding_pool,
    block_env_pool: builder.block_env_pool,
    finally_pc_pool: builder.finally_pc_pool,
    arg_count,
    var_count,
    stack_size: builder.max_stack,
    debug,
  }
}

///|
fn bytecode_current_pc(builder : BytecodeBuilder) -> Int {
  builder.code.length()
}

///|
fn bytecode_emit_jump(builder : BytecodeBuilder, op : BytecodeOp) -> Int {
  let index = builder.code.length()
  bytecode_emit(builder, BytecodeInstr::I32(op, -1))
  index
}

///|
fn bytecode_patch_jump(
  builder : BytecodeBuilder,
  instr_index : Int,
  target : Int,
) -> Unit {
  if instr_index < 0 || instr_index >= builder.code.length() {
    return
  }
  match builder.code[instr_index] {
    BytecodeInstr::I32(op, _) =>
      builder.code[instr_index] = BytecodeInstr::I32(op, target)
    _ => ()
  }
}

///|
fn bytecode_unary_op_code(op : @engine.UnaryOp) -> Int {
  match op {
    @engine.UnaryOp::Plus => 0
    @engine.UnaryOp::Minus => 1
    @engine.UnaryOp::BitNot => 2
    @engine.UnaryOp::Not => 3
    @engine.UnaryOp::Typeof => 4
    @engine.UnaryOp::Void => 5
    @engine.UnaryOp::Delete => 6
  }
}

///|
fn bytecode_binary_op_code(op : @engine.BinaryOp) -> Int {
  match op {
    @engine.BinaryOp::Add => 0
    @engine.BinaryOp::Sub => 1
    @engine.BinaryOp::Mul => 2
    @engine.BinaryOp::Div => 3
    @engine.BinaryOp::Mod => 4
    @engine.BinaryOp::Pow => 5
    @engine.BinaryOp::Shl => 6
    @engine.BinaryOp::Sar => 7
    @engine.BinaryOp::Shr => 8
    @engine.BinaryOp::Lt => 9
    @engine.BinaryOp::Lte => 10
    @engine.BinaryOp::Gt => 11
    @engine.BinaryOp::Gte => 12
    @engine.BinaryOp::In => 13
    @engine.BinaryOp::Instanceof => 14
    @engine.BinaryOp::Eq => 15
    @engine.BinaryOp::Neq => 16
    @engine.BinaryOp::StrictEq => 17
    @engine.BinaryOp::StrictNeq => 18
    @engine.BinaryOp::BitAnd => 19
    @engine.BinaryOp::BitXor => 20
    @engine.BinaryOp::BitOr => 21
    @engine.BinaryOp::LogicalAnd => 22
    @engine.BinaryOp::LogicalOr => 23
    @engine.BinaryOp::Coalesce => 24
  }
}

///|
