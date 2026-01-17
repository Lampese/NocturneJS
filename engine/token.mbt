///|
pub enum TokenKind {
  Number
  String
  Template
  TemplateHead
  TemplateMiddle
  TemplateTail
  Ident
  Regexp
  MulAssign
  DivAssign
  ModAssign
  PlusAssign
  MinusAssign
  ShlAssign
  SarAssign
  ShrAssign
  AndAssign
  XorAssign
  OrAssign
  PowAssign
  LandAssign
  LorAssign
  DoubleQuestionMarkAssign
  Dec
  Inc
  Shl
  Sar
  Shr
  Lt
  Lte
  Gt
  Gte
  Eq
  StrictEq
  Neq
  StrictNeq
  Land
  Lor
  Pow
  Arrow
  Ellipsis
  DoubleQuestionMark
  QuestionMarkDot
  Error
  PrivateName
  Eof
  KeywordNull
  KeywordFalse
  KeywordTrue
  KeywordIf
  KeywordElse
  KeywordReturn
  KeywordVar
  KeywordThis
  KeywordDelete
  KeywordVoid
  KeywordTypeof
  KeywordNew
  KeywordIn
  KeywordInstanceof
  KeywordDo
  KeywordWhile
  KeywordFor
  KeywordBreak
  KeywordContinue
  KeywordSwitch
  KeywordCase
  KeywordDefault
  KeywordThrow
  KeywordTry
  KeywordCatch
  KeywordFinally
  KeywordFunction
  KeywordDebugger
  KeywordWith
  KeywordClass
  KeywordConst
  KeywordEnum
  KeywordExport
  KeywordExtends
  KeywordImport
  KeywordSuper
  KeywordImplements
  KeywordInterface
  KeywordLet
  KeywordPackage
  KeywordPrivate
  KeywordProtected
  KeywordPublic
  KeywordStatic
  KeywordYield
  KeywordAwait
  KeywordOf
  Punct(Char)
} derive(Show, Eq, ToJson)

///|
pub struct Token {
  kind : TokenKind
  lexeme : String
  raw_lexeme : String?
  offset : Int
  line_break_before : Bool
} derive(Show, Eq, ToJson)

///|
pub fn Token::eof(offset : Int, line_break_before : Bool) -> Token {
  Token::{ kind: Eof, lexeme: "", raw_lexeme: None, offset, line_break_before }
}
