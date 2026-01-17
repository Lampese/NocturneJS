///|
#warnings("-unused_constructor")
pub struct TemplateParts {
  cooked : Array[String?]
  raw : Array[String]
  site_id : Int
} derive(Show, Eq, ToJson)

///|
#warnings("-unused_constructor")
pub enum Expr {
  Number(Double, Bool, Int)
  Bool(Bool)
  Null
  String(String, Bool, Bool)
  TemplateLiteral(TemplateParts, Array[Expr])
  TaggedTemplate(Expr, TemplateParts, Array[Expr])
  Regexp(String, String, Int)
  BigInt(String)
  Ident(String, Int)
  This
  Super
  Yield(Expr?, Bool)
  Await(Expr, Int)
  Unary(UnaryOp, Expr, Int)
  Update(UpdateOp, Expr, UpdateKind, Int)
  NewTarget(Int)
  ImportMeta(Int)
  New(Expr, Array[Expr])
  Binary(BinaryOp, Expr, Expr, Int)
  PrivateIn(String, Expr, Int)
  Conditional(Expr, Expr, Expr)
  Sequence(Array[Expr])
  Paren(Expr)
  Assign(String, Expr, Int, Bool)
  AssignInvalid(Expr, Expr, Int)
  AssignArray(Array[ArrayPatternElem], Expr)
  AssignObject(Array[ObjectPatternElem], Expr)
  AssignOp(BinaryOp, Expr, Expr, Int)
  AssignMember(Expr, MemberKey, Expr, Int)
  Call(Expr, Array[Expr], Int)
  Member(Expr, MemberKey, Int)
  OptionalChain(Expr, Array[ChainSegment])
  FunctionExpr(FunctionDef)
  ClassExpr(ClassDef)
  ObjectLiteral(Array[ObjectProp])
  ArrayLiteral(Array[Expr?])
  Spread(Expr)
} derive(Show, Eq, ToJson)

///|
pub enum BinaryOp {
  Add
  Sub
  Mul
  Div
  Mod
  Pow
  Shl
  Sar
  Shr
  Lt
  Lte
  Gt
  Gte
  In
  Instanceof
  Eq
  Neq
  StrictEq
  StrictNeq
  BitAnd
  BitXor
  BitOr
  LogicalAnd
  LogicalOr
  Coalesce
} derive(Show, Eq, ToJson)

///|
pub enum UnaryOp {
  Plus
  Minus
  BitNot
  Not
  Typeof
  Void
  Delete
} derive(Show, Eq, ToJson)

///|
pub enum UpdateOp {
  Inc
  Dec
} derive(Show, Eq, ToJson)

///|
pub enum UpdateKind {
  Prefix
  Postfix
} derive(Show, Eq, ToJson)

///|
pub enum MemberKey {
  Ident(String)
  Private(String)
  Computed(Expr)
} derive(Show, Eq, ToJson)

///|
pub enum ChainSegment {
  Member(MemberKey, Bool, Int)
  Call(Array[Expr], Bool, Int)
} derive(Show, Eq, ToJson)

///|
pub enum ObjectProp {
  KeyValue(ObjKey, Expr)
  Method(ObjKey, FunctionDef)
  Shorthand(String)
  Getter(ObjKey, FunctionDef)
  Setter(ObjKey, FunctionDef)
  Spread(Expr)
} derive(Show, Eq, ToJson)

///|
pub enum ObjKey {
  Ident(String)
  Private(String)
  String(String)
  Number(Double)
  Computed(Expr)
} derive(Show, Eq, ToJson)

///|
pub enum ArrayPatternElem {
  Bind(VarBinding, Expr?)
  Rest(VarBinding)
  Hole
} derive(Show, Eq, ToJson)

///|
pub enum ObjectPatternElem {
  Bind(ObjKey, VarBinding, Expr?)
  Rest(VarBinding)
} derive(Show, Eq, ToJson)

///|
pub enum VarBinding {
  Name(String)
  ArrayPattern(Array[ArrayPatternElem])
  ObjectPattern(Array[ObjectPatternElem])
  Target(Expr)
} derive(Show, Eq, ToJson)

///|
pub struct ParamBinding {
  binding : VarBinding
  is_rest : Bool
} derive(Show, Eq, ToJson)

///|
pub enum ClassMember {
  Method(ObjKey, FunctionDef, Bool)
  Getter(ObjKey, FunctionDef, Bool)
  Setter(ObjKey, FunctionDef, Bool)
  Field(ObjKey, Expr?, Bool)
  StaticBlock(Array[Stmt])
} derive(Show, Eq, ToJson)

///|
pub struct ClassDef {
  name : String?
  super_class : Expr?
  body : Array[ClassMember]
  offset : Int
  end_offset : Int
} derive(Show, Eq, ToJson)

///|
pub enum VarKind {
  Var
  Let
  ConstDecl
} derive(Show, Eq, ToJson)

///|
pub enum Stmt {
  Empty
  ExprStmt(Expr)
  VarDecl(VarKind, Array[(VarBinding, Expr?)])
  Block(Array[Stmt])
  Label(String, Stmt)
  FunctionDecl(FunctionDef)
  ClassDecl(ClassDef)
  Return(Expr?)
  If(Expr, Stmt, Stmt?)
  With(Expr, Stmt)
  While(Expr, Stmt)
  For(ForInit, Expr?, Expr?, Stmt)
  ForIn(ForInit, Expr, Stmt)
  ForOf(ForInit, Expr, Stmt)
  ForAwaitOf(ForInit, Expr, Stmt)
  DoWhile(Stmt, Expr)
  Switch(Expr, Array[SwitchCase])
  Throw(Expr)
  Try(Stmt, CatchClause?, Stmt?)
  Break(String?)
  Continue(String?)
} derive(Show, Eq, ToJson)

///|
pub enum ForInit {
  None
  Var(VarKind, Array[(VarBinding, Expr?)])
  Expr(Expr)
} derive(Show, Eq, ToJson)

///|
pub enum SwitchCase {
  Case(Expr, Array[Stmt])
  Default(Array[Stmt])
} derive(Show, Eq, ToJson)

///|
pub struct CatchClause {
  name : String?
  body : Stmt
} derive(Show, Eq, ToJson)

///|
pub struct FunctionDef {
  name : String?
  params : Array[ParamBinding]
  param_inits : Array[Expr?]
  body : Array[Stmt]
  is_strict : Bool
  is_generator : Bool
  is_async : Bool
  is_arrow : Bool
  offset : Int
  end_offset : Int
} derive(Show, Eq, ToJson)

///|
pub fn FunctionDef::new(
  name : String?,
  params : Array[ParamBinding],
  param_inits : Array[Expr?],
  body : Array[Stmt],
  is_strict : Bool,
  is_generator : Bool,
  is_async : Bool,
  is_arrow : Bool,
  offset : Int,
  end_offset : Int,
) -> FunctionDef {
  FunctionDef::{
    name,
    params,
    param_inits,
    body,
    is_strict,
    is_generator,
    is_async,
    is_arrow,
    offset,
    end_offset,
  }
}

///|
pub struct Script {
  body : Array[Stmt]
  is_strict : Bool
  is_module : Bool
  source : String
} derive(Show, Eq, ToJson)

///|
pub fn Script::empty() -> Script {
  Script::{ body: [], is_strict: false, is_module: false, source: "" }
}
