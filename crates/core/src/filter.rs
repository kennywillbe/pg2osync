//! A restricted SQL predicate over one row's own columns.
//!
//! A row filter has to be true in two places at once. The initial load hands
//! it to the database inside the load query, and afterwards the engine has to
//! apply it again to every streamed row, where there is no query to put it
//! in. Whatever the operator writes must therefore mean exactly the same
//! thing to the source's SQL parser and to the evaluator here, on a JSON
//! document that has already lost the column types. The grammar is the part
//! of SQL both can say exactly: a column against a literal, null tests, IN
//! lists and the three connectives under SQL's three-valued logic. Functions,
//! pattern matching or column-to-column comparisons would either need each
//! source's own semantics re-implemented here or would quietly diverge
//! between the two evaluations, and a filter that disagrees with itself keeps
//! or drops a row depending on whether it arrived by load or by stream.

use serde_json::{Map, Value};
use std::cmp::Ordering;

/// Appended to every parse failure a config reports, so the operator does not
/// have to find the list in the documentation.
pub const SUPPORTED: &str = "supported: column = <> != < <= > >= literal, column IS [NOT] NULL, column [NOT] IN (literal, ...), AND, OR, NOT, parentheses; literals are 'text' (with '' for a quote), 12, 3.5, true, false";

/// How one source spells identifiers and string literals. They differ:
/// MySQL treats a backslash inside a string as an escape, PostgreSQL does
/// not, and the loader building the statement is the only place that knows
/// which it is talking to.
pub struct SqlDialect<'a> {
    pub quote_ident: &'a dyn Fn(&str) -> String,
    pub quote_str: &'a dyn Fn(&str) -> String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Literal {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Filter {
    expr: Expr,
    columns: Vec<String>,
}

impl Filter {
    pub fn parse(src: &str) -> Result<Self, String> {
        if src.trim().is_empty() {
            return Err("the predicate is empty".into());
        }
        let mut parser = Parser {
            src,
            tokens: tokenize(src)?,
            pos: 0,
            columns: Vec::new(),
        };
        let expr = parser.or_expr()?;
        if let Some(extra) = parser.peek() {
            return Err(format!(
                "unexpected {} at offset {} after the end of the predicate",
                extra.describe(),
                extra.offset
            ));
        }
        let mut columns = parser.columns;
        columns.sort();
        columns.dedup();
        Ok(Self { expr, columns })
    }

    /// Every column the predicate names, sorted and de-duplicated, so a
    /// startup check can prove they all exist.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Whether this row belongs in the index. Three-valued internally; only
    /// TRUE matches, which is what WHERE means.
    pub fn matches(&self, doc: &Value) -> bool {
        doc.as_object()
            .is_some_and(|fields| eval(&self.expr, fields) == Some(true))
    }

    pub fn to_sql(&self, dialect: &SqlDialect<'_>) -> String {
        render(&self.expr, dialect)
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Expr {
    Cmp {
        column: String,
        op: CmpOp,
        literal: Literal,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    In {
        column: String,
        list: Vec<Literal>,
        negated: bool,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CmpOp {
    fn holds(self, ordering: Ordering) -> bool {
        match self {
            CmpOp::Eq => ordering == Ordering::Equal,
            CmpOp::Ne => ordering != Ordering::Equal,
            CmpOp::Lt => ordering == Ordering::Less,
            CmpOp::Le => ordering != Ordering::Greater,
            CmpOp::Gt => ordering == Ordering::Greater,
            CmpOp::Ge => ordering != Ordering::Less,
        }
    }

    fn sql(self) -> &'static str {
        match self {
            CmpOp::Eq => "=",
            CmpOp::Ne => "<>",
            CmpOp::Lt => "<",
            CmpOp::Le => "<=",
            CmpOp::Gt => ">",
            CmpOp::Ge => ">=",
        }
    }
}

// ---------------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------------

/// SQL truth: `None` is UNKNOWN, which is what any comparison against NULL
/// yields and what AND/OR/NOT propagate the way the standard says.
fn eval(expr: &Expr, doc: &Map<String, Value>) -> Option<bool> {
    match expr {
        Expr::Cmp {
            column,
            op,
            literal,
        } => compare(doc.get(column), literal).map(|ordering| op.holds(ordering)),
        // Absent means the source did not send the column; NULL is the only
        // thing the database would have called it.
        Expr::IsNull { column, negated } => {
            let is_null = doc.get(column).is_none_or(Value::is_null);
            Some(is_null != *negated)
        }
        Expr::In {
            column,
            list,
            negated,
        } => {
            let value = doc.get(column);
            let mut unknown = false;
            for literal in list {
                match compare(value, literal) {
                    Some(Ordering::Equal) => return Some(!negated),
                    None => unknown = true,
                    Some(_) => {}
                }
            }
            if unknown { None } else { Some(*negated) }
        }
        Expr::And(a, b) => match (eval(a, doc), eval(b, doc)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (None, _) | (_, None) => None,
            _ => Some(true),
        },
        Expr::Or(a, b) => match (eval(a, doc), eval(b, doc)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (None, _) | (_, None) => None,
            _ => Some(false),
        },
        Expr::Not(a) => eval(a, doc).map(|b| !b),
    }
}

/// A number as either side of a comparison sees it. Integers stay exact so a
/// key beyond 2^53 still compares equal only to itself.
#[derive(Clone, Copy)]
enum Num {
    Int(i128),
    Float(f64),
}

impl Num {
    fn from_json(n: &serde_json::Number) -> Option<Self> {
        if let Some(i) = n.as_i64() {
            Some(Num::Int(i128::from(i)))
        } else if let Some(u) = n.as_u64() {
            Some(Num::Int(i128::from(u)))
        } else {
            n.as_f64().map(Num::Float)
        }
    }

    fn from_str(s: &str) -> Option<Self> {
        if let Ok(i) = s.parse::<i128>() {
            Some(Num::Int(i))
        } else {
            s.parse::<f64>().ok().map(Num::Float)
        }
    }

    fn from_literal(literal: &Literal) -> Option<Self> {
        match literal {
            Literal::Int(i) => Some(Num::Int(i128::from(*i))),
            Literal::Float(f) => Some(Num::Float(*f)),
            Literal::Str(_) | Literal::Bool(_) => None,
        }
    }

    fn cmp(self, other: Num) -> Option<Ordering> {
        match (self, other) {
            (Num::Int(a), Num::Int(b)) => Some(a.cmp(&b)),
            (a, b) => a.as_f64().partial_cmp(&b.as_f64()),
        }
    }

    fn as_f64(self) -> f64 {
        match self {
            Num::Int(i) => i as f64,
            Num::Float(f) => f,
        }
    }
}

/// Orders a document value against a literal, or `None` when SQL would have
/// said UNKNOWN: a NULL on either side, or types that do not compare.
fn compare(value: Option<&Value>, literal: &Literal) -> Option<Ordering> {
    match (value?, literal) {
        (Value::Null, _) => None,
        // Byte-wise ordering is what makes `created_at >= '2024-01-01'` work
        // against a timestamp that reached the engine as text.
        (Value::String(s), Literal::Str(l)) => Some(s.as_bytes().cmp(l.as_bytes())),
        // PostgreSQL `numeric` and MySQL `DECIMAL` reach the engine as JSON
        // strings on purpose, and SQL would have compared them numerically.
        (Value::String(s), Literal::Int(_) | Literal::Float(_)) => {
            Num::from_str(s)?.cmp(Num::from_literal(literal)?)
        }
        (Value::Number(n), Literal::Int(_) | Literal::Float(_)) => {
            Num::from_json(n)?.cmp(Num::from_literal(literal)?)
        }
        (Value::Bool(b), Literal::Bool(l)) => Some(b.cmp(l)),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Every connective is parenthesised so precedence never depends on the
/// target's parser.
fn render(expr: &Expr, dialect: &SqlDialect<'_>) -> String {
    match expr {
        Expr::Cmp {
            column,
            op,
            literal,
        } => format!(
            "{} {} {}",
            (dialect.quote_ident)(column),
            op.sql(),
            render_literal(literal, dialect)
        ),
        Expr::IsNull { column, negated } => format!(
            "{} IS {}NULL",
            (dialect.quote_ident)(column),
            if *negated { "NOT " } else { "" }
        ),
        Expr::In {
            column,
            list,
            negated,
        } => {
            let items: Vec<String> = list
                .iter()
                .map(|literal| render_literal(literal, dialect))
                .collect();
            format!(
                "{} {}IN ({})",
                (dialect.quote_ident)(column),
                if *negated { "NOT " } else { "" },
                items.join(", ")
            )
        }
        Expr::And(a, b) => format!("({} AND {})", render(a, dialect), render(b, dialect)),
        Expr::Or(a, b) => format!("({} OR {})", render(a, dialect), render(b, dialect)),
        Expr::Not(a) => format!("(NOT {})", render(a, dialect)),
    }
}

fn render_literal(literal: &Literal, dialect: &SqlDialect<'_>) -> String {
    match literal {
        Literal::Str(s) => (dialect.quote_str)(s),
        Literal::Int(i) => i.to_string(),
        // 10.50 renders as 10.5: the same value to the database, so the
        // source spelling is not worth carrying around.
        Literal::Float(f) => f.to_string(),
        Literal::Bool(true) => "TRUE".into(),
        Literal::Bool(false) => "FALSE".into(),
    }
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Ident(String),
    Str(String),
    Int(i64),
    Float(f64),
    Op(CmpOp),
    LParen,
    RParen,
    Comma,
}

#[derive(Debug, Clone)]
struct Token<'a> {
    tok: Tok,
    offset: usize,
    text: &'a str,
}

impl Token<'_> {
    /// How an error names this token: literals as written, so `'active'`
    /// keeps its quotes and `42` has none; everything else double-quoted.
    fn describe(&self) -> String {
        match self.tok {
            Tok::Str(_) | Tok::Int(_) | Tok::Float(_) => self.text.to_string(),
            _ => format!("{:?}", self.text),
        }
    }

    fn is_keyword(&self, keyword: &str) -> bool {
        matches!(&self.tok, Tok::Ident(word) if word.eq_ignore_ascii_case(keyword))
    }

    fn is_word_operator(&self) -> bool {
        matches!(&self.tok, Tok::Ident(word) if is_word_operator(word))
    }
}

const KEYWORDS: [&str; 8] = ["AND", "OR", "NOT", "IS", "NULL", "IN", "TRUE", "FALSE"];

/// SQL operators spelled as words. They are recognised only to name them in
/// the error, which reads better than "expected an operator".
const WORD_OPERATORS: [&str; 7] = [
    "LIKE", "ILIKE", "BETWEEN", "SIMILAR", "ANY", "ALL", "EXISTS",
];

fn is_keyword(word: &str) -> bool {
    KEYWORDS.iter().any(|k| word.eq_ignore_ascii_case(k))
}

fn is_word_operator(word: &str) -> bool {
    WORD_OPERATORS.iter().any(|k| word.eq_ignore_ascii_case(k))
}

fn tokenize(src: &str) -> Result<Vec<Token<'_>>, String> {
    let bytes = src.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let single = |tok: Tok| Token {
            tok,
            offset: start,
            text: &src[start..start + 1],
        };
        match bytes[i] {
            b' ' | b'\t' | b'\r' | b'\n' => i += 1,
            b'(' => {
                tokens.push(single(Tok::LParen));
                i += 1;
            }
            b')' => {
                tokens.push(single(Tok::RParen));
                i += 1;
            }
            b',' => {
                tokens.push(single(Tok::Comma));
                i += 1;
            }
            b'\'' => {
                let mut end = i + 1;
                loop {
                    match bytes.get(end) {
                        None => {
                            return Err(format!(
                                "unterminated string literal starting at offset {start}"
                            ));
                        }
                        Some(b'\'') if bytes.get(end + 1) == Some(&b'\'') => end += 2,
                        Some(b'\'') => break,
                        Some(_) => end += 1,
                    }
                }
                // The quote is ASCII, so both bounds sit on char boundaries.
                let value = src[start + 1..end].replace("''", "'");
                tokens.push(Token {
                    tok: Tok::Str(value),
                    offset: start,
                    text: &src[start..=end],
                });
                i = end + 1;
            }
            b'-' if bytes.get(i + 1).is_some_and(u8::is_ascii_digit) => {
                i = scan_number(src, start, i + 1, &mut tokens)?;
            }
            b'0'..=b'9' => {
                i = scan_number(src, start, i, &mut tokens)?;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'_' => {
                let mut end = i + 1;
                while bytes
                    .get(end)
                    .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'$')
                {
                    end += 1;
                }
                tokens.push(Token {
                    tok: Tok::Ident(src[start..end].to_string()),
                    offset: start,
                    text: &src[start..end],
                });
                i = end;
            }
            b'<' | b'>' | b'=' | b'!' => {
                let two = bytes.get(i + 1).map(|next| (bytes[i], *next));
                let (op, len) = match two {
                    Some((b'<', b'>')) | Some((b'!', b'=')) => (CmpOp::Ne, 2),
                    Some((b'<', b'=')) => (CmpOp::Le, 2),
                    Some((b'>', b'=')) => (CmpOp::Ge, 2),
                    _ => match bytes[i] {
                        b'<' => (CmpOp::Lt, 1),
                        b'>' => (CmpOp::Gt, 1),
                        b'=' => (CmpOp::Eq, 1),
                        _ => return Err(unexpected_character(src, start)),
                    },
                };
                tokens.push(Token {
                    tok: Tok::Op(op),
                    offset: start,
                    text: &src[start..start + len],
                });
                i += len;
            }
            b'~' | b'|' | b'&' | b'%' | b'*' | b'+' | b'/' => {
                return Err(format!(
                    "unsupported operator {:?} at offset {start}",
                    &src[start..start + 1]
                ));
            }
            _ => return Err(unexpected_character(src, start)),
        }
    }
    Ok(tokens)
}

fn unexpected_character(src: &str, offset: usize) -> String {
    // The scanner only ever stops on a byte it has not consumed part of, so
    // `offset` is a char boundary and the slice cannot panic.
    let c = src[offset..].chars().next().unwrap_or('\0');
    format!("unexpected character {c:?} at offset {offset}")
}

/// Scans digits (and one fraction) from `digits_at`, pushes the token that
/// starts at `start`, and returns where the next token begins.
fn scan_number<'a>(
    src: &'a str,
    start: usize,
    digits_at: usize,
    tokens: &mut Vec<Token<'a>>,
) -> Result<usize, String> {
    let bytes = src.as_bytes();
    let mut end = digits_at;
    while bytes.get(end).is_some_and(u8::is_ascii_digit) {
        end += 1;
    }
    let mut is_float = false;
    if bytes.get(end) == Some(&b'.') && bytes.get(end + 1).is_some_and(u8::is_ascii_digit) {
        is_float = true;
        end += 1;
        while bytes.get(end).is_some_and(u8::is_ascii_digit) {
            end += 1;
        }
    }
    let text = &src[start..end];
    let out_of_range = || format!("the number {text:?} at offset {start} is out of range");
    let tok = if is_float {
        let f: f64 = text.parse().map_err(|_| out_of_range())?;
        if !f.is_finite() {
            return Err(out_of_range());
        }
        Tok::Float(f)
    } else {
        Tok::Int(text.parse().map_err(|_| out_of_range())?)
    };
    tokens.push(Token {
        tok,
        offset: start,
        text,
    });
    Ok(end)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    src: &'a str,
    tokens: Vec<Token<'a>>,
    pos: usize,
    columns: Vec<String>,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<&Token<'a>> {
        self.tokens.get(self.pos)
    }

    fn advance(&mut self) -> Option<Token<'a>> {
        let token = self.tokens.get(self.pos).cloned();
        self.pos += 1;
        token
    }

    fn accept_keyword(&mut self, keyword: &str) -> bool {
        if self.peek().is_some_and(|t| t.is_keyword(keyword)) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// Where the next token starts, or the end of the input.
    fn offset(&self) -> usize {
        self.peek().map_or(self.src.len(), |t| t.offset)
    }

    fn found(&self) -> String {
        self.peek()
            .map_or_else(|| "the end of the predicate".to_string(), Token::describe)
    }

    fn or_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.and_expr()?;
        while self.accept_keyword("OR") {
            let right = self.and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn and_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.not_expr()?;
        while self.accept_keyword("AND") {
            let right = self.not_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    fn not_expr(&mut self) -> Result<Expr, String> {
        if self.accept_keyword("NOT") {
            return Ok(Expr::Not(Box::new(self.not_expr()?)));
        }
        self.primary()
    }

    fn primary(&mut self) -> Result<Expr, String> {
        if self.peek().is_some_and(|t| t.tok == Tok::LParen) {
            self.pos += 1;
            let inner = self.or_expr()?;
            self.expect_rparen()?;
            return Ok(inner);
        }
        self.comparison()
    }

    fn expect_rparen(&mut self) -> Result<(), String> {
        match self.peek() {
            Some(t) if t.tok == Tok::RParen => {
                self.pos += 1;
                Ok(())
            }
            Some(t) => Err(format!("expected ')' at offset {}", t.offset)),
            None => Err("expected ')' before the end of the predicate".into()),
        }
    }

    fn comparison(&mut self) -> Result<Expr, String> {
        let column = match self.advance() {
            Some(Token {
                tok: Tok::Ident(name),
                offset,
                ..
            }) if !is_keyword(&name) => {
                if self.peek().is_some_and(|t| t.tok == Tok::LParen) {
                    return Err(format!(
                        "function calls are not supported: {name}(...) at offset {offset}"
                    ));
                }
                name
            }
            Some(t) => {
                return Err(format!(
                    "expected a column name at offset {}, found {}",
                    t.offset,
                    t.describe()
                ));
            }
            None => {
                return Err(format!(
                    "expected a column name at offset {}, found the end of the predicate",
                    self.src.len()
                ));
            }
        };
        self.columns.push(column.clone());

        let Some(operator) = self.advance() else {
            return Err(format!(
                "expected an operator after column {column:?} at offset {}, found the end of the predicate",
                self.src.len()
            ));
        };
        match &operator.tok {
            Tok::Op(op) => self.comparison_rhs(column, *op, &operator),
            Tok::Ident(_) if operator.is_keyword("IS") => {
                let negated = self.accept_keyword("NOT");
                if !self.accept_keyword("NULL") {
                    return Err(format!(
                        "IS must be followed by NULL or NOT NULL, at offset {}",
                        self.offset()
                    ));
                }
                Ok(Expr::IsNull { column, negated })
            }
            Tok::Ident(_) if operator.is_keyword("IN") => self.in_list(column, false),
            Tok::Ident(_) if operator.is_keyword("NOT") => {
                if self.accept_keyword("IN") {
                    return self.in_list(column, true);
                }
                match self.peek() {
                    Some(t) if t.is_word_operator() => Err(format!(
                        "unsupported operator \"NOT {}\" at offset {}",
                        t.text, operator.offset
                    )),
                    _ => Err(format!(
                        "expected IN after NOT at offset {}, found {}",
                        self.offset(),
                        self.found()
                    )),
                }
            }
            Tok::Ident(_) if operator.is_word_operator() => Err(format!(
                "unsupported operator {} at offset {}",
                operator.describe(),
                operator.offset
            )),
            _ => Err(format!(
                "expected an operator after column {column:?} at offset {}, found {}",
                operator.offset,
                operator.describe()
            )),
        }
    }

    fn comparison_rhs(
        &mut self,
        column: String,
        op: CmpOp,
        operator: &Token<'a>,
    ) -> Result<Expr, String> {
        let spelled = operator.text;
        if matches!(op, CmpOp::Eq | CmpOp::Ne) && self.peek().is_some_and(|t| t.is_keyword("NULL"))
        {
            return Err(format!(
                "{spelled} NULL is always unknown; write IS NULL or IS NOT NULL (offset {})",
                operator.offset
            ));
        }
        let Some(literal) = self.peek().and_then(literal_of) else {
            return Err(format!(
                "expected a literal after {spelled} at offset {}, found {}; a comparison is written column {spelled} literal",
                self.offset(),
                self.found()
            ));
        };
        self.pos += 1;
        Ok(Expr::Cmp {
            column,
            op,
            literal,
        })
    }

    fn in_list(&mut self, column: String, negated: bool) -> Result<Expr, String> {
        if !self.peek().is_some_and(|t| t.tok == Tok::LParen) {
            return Err(format!(
                "IN must be followed by a parenthesised list, at offset {}",
                self.offset()
            ));
        }
        self.pos += 1;
        if self.peek().is_some_and(|t| t.tok == Tok::RParen) {
            return Err(format!(
                "IN (...) needs at least one value, at offset {}",
                self.offset()
            ));
        }
        let mut list = Vec::new();
        loop {
            match self.peek() {
                None => return Err("expected ')' before the end of the predicate".into()),
                Some(t) => match literal_of(t) {
                    Some(literal) => list.push(literal),
                    None => {
                        return Err(format!(
                            "IN (...) takes literals only, at offset {}",
                            t.offset
                        ));
                    }
                },
            }
            self.pos += 1;
            if self.peek().is_some_and(|t| t.tok == Tok::Comma) {
                self.pos += 1;
                continue;
            }
            self.expect_rparen()?;
            return Ok(Expr::In {
                column,
                list,
                negated,
            });
        }
    }
}

fn literal_of(token: &Token<'_>) -> Option<Literal> {
    match &token.tok {
        Tok::Str(s) => Some(Literal::Str(s.clone())),
        Tok::Int(i) => Some(Literal::Int(*i)),
        Tok::Float(f) => Some(Literal::Float(*f)),
        Tok::Ident(_) if token.is_keyword("TRUE") => Some(Literal::Bool(true)),
        Tok::Ident(_) if token.is_keyword("FALSE") => Some(Literal::Bool(false)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(src: &str) -> Filter {
        Filter::parse(src).unwrap_or_else(|e| panic!("{src:?} should parse: {e}"))
    }

    fn refuse(src: &str, phrase: &str) {
        let err = Filter::parse(src).expect_err(src);
        assert!(err.contains(phrase), "{src:?} -> {err:?} lacks {phrase:?}");
    }

    fn truth(src: &str, doc: &Value) -> Option<bool> {
        let fields = doc.as_object().expect("object document");
        eval(&parse(src).expr, fields)
    }

    #[test]
    fn the_supported_grammar_parses_and_anything_else_is_refused() {
        for src in [
            "status = 'active' AND tenant IN ('eu','us') AND deleted_at IS NULL",
            "a IS NOT NULL",
            "a NOT IN (1, 2)",
            "NOT (a = 1 OR (b <> 2))",
            "n = -5",
            "name = 'o''brien'",
            "x >= 3.5",
            "ok = TRUE",
            "a Is nOt NULL and b iN (1) Or not c = false",
        ] {
            parse(src);
        }

        refuse("status LIKE 'a%'", "unsupported operator");
        refuse(
            "lower(x) = 'a'",
            "function calls are not supported: lower(...)",
        );
        refuse("a = b", "expected a literal after =");
        refuse("x = NULL", "IS NULL");
        refuse("a IN ()", "at least one value");
        refuse("a IS 3", "IS must be followed by NULL or NOT NULL");
        refuse("(a = 1", "expected ')' before the end of the predicate");
        refuse("a = 1 b", "after the end of the predicate");
        refuse("", "the predicate is empty");
        refuse("a = 'unterminated", "unterminated string literal");
        refuse("a = 99999999999999999999", "out of range");
    }

    #[test]
    fn rendered_sql_quotes_identifiers_and_strings_for_the_dialect_it_is_given() {
        let pg_ident = |c: &str| format!("p.\"{c}\"");
        let pg_str = |s: &str| format!("'{}'", s.replace('\'', "''"));
        let pg = SqlDialect {
            quote_ident: &pg_ident,
            quote_str: &pg_str,
        };
        let my_ident = |c: &str| format!("`{c}`");
        let my_str = |s: &str| format!("'{}'", s.replace('\\', "\\\\").replace('\'', "''"));
        let my = SqlDialect {
            quote_ident: &my_ident,
            quote_str: &my_str,
        };

        let filter = parse("dec = 'o''brien' AND dec IS NOT NULL");
        assert_eq!(
            filter.to_sql(&pg),
            r#"(p."dec" = 'o''brien' AND p."dec" IS NOT NULL)"#
        );
        assert_eq!(
            filter.to_sql(&my),
            "(`dec` = 'o''brien' AND `dec` IS NOT NULL)"
        );

        let filter = parse("a = 1 AND b = 2 OR c = 3");
        assert_eq!(
            filter.to_sql(&pg),
            r#"((p."a" = 1 AND p."b" = 2) OR p."c" = 3)"#
        );

        let filter = parse("NOT x IN (1.50, TRUE, 'y') AND x < -2");
        assert_eq!(
            filter.to_sql(&my),
            "((NOT `x` IN (1.5, TRUE, 'y')) AND `x` < -2)"
        );
    }

    #[test]
    fn null_makes_a_comparison_unknown_and_unknown_never_matches() {
        let filter = parse("NOT (x = 1)");
        assert!(!filter.matches(&json!({ "x": null })));
        assert!(!filter.matches(&json!({})));
        assert!(filter.matches(&json!({ "x": 2 })));

        assert!(parse("x IS NULL").matches(&json!({})));
        assert!(parse("x IS NULL").matches(&json!({ "x": null })));
        assert!(!parse("x IS NOT NULL").matches(&json!({})));
        assert!(!parse("x = 1").matches(&json!([1])));
    }

    #[test]
    fn a_numeric_string_compares_as_a_number_and_a_text_one_byte_wise() {
        assert!(parse("d = 10").matches(&json!({ "d": "10.00" })));
        assert!(parse("d < 10").matches(&json!({ "d": "9" })));
        assert!(!parse("d > 5").matches(&json!({ "d": "abc" })));
        assert!(!parse("d <= 5").matches(&json!({ "d": "abc" })));
        assert!(parse("t >= '2024-01-01'").matches(&json!({ "t": "2024-01-02" })));
        assert!(parse("d = '10'").matches(&json!({ "d": "10" })));
        assert!(!parse("d = '10'").matches(&json!({ "d": 10 })));
        assert!(parse("f > 2.5").matches(&json!({ "f": 3 })));
    }

    #[test]
    fn in_with_a_null_column_is_unknown_and_not_in_inherits_it() {
        let inside = parse("t IN ('a', 'b')");
        let outside = parse("t NOT IN ('a', 'b')");
        assert!(!inside.matches(&json!({ "t": null })));
        assert!(!outside.matches(&json!({ "t": null })));
        assert!(!inside.matches(&json!({})));
        assert!(!outside.matches(&json!({})));
        assert!(inside.matches(&json!({ "t": "a" })));
        assert!(!outside.matches(&json!({ "t": "a" })));
        assert!(!inside.matches(&json!({ "t": "c" })));
        assert!(outside.matches(&json!({ "t": "c" })));

        // A list element the column cannot be compared with leaves the
        // answer unknown unless another element matches outright.
        assert!(parse("n IN ('x', 2)").matches(&json!({ "n": 2 })));
        assert!(!parse("n NOT IN ('x', 3)").matches(&json!({ "n": 2 })));
    }

    #[test]
    fn three_valued_and_or_agree_with_sql() {
        let doc = json!({ "t": 1, "f": 2, "n": null });
        let cases = [
            ("t = 1", Some(true)),
            ("f = 1", Some(false)),
            ("n = 1", None),
        ];
        for (a, va) in cases {
            assert_eq!(truth(a, &doc), va, "{a}");
            assert_eq!(truth(&format!("NOT {a}"), &doc), va.map(|b| !b));
            for (b, vb) in cases {
                let and = match (va, vb) {
                    (Some(false), _) | (_, Some(false)) => Some(false),
                    (None, _) | (_, None) => None,
                    _ => Some(true),
                };
                let or = match (va, vb) {
                    (Some(true), _) | (_, Some(true)) => Some(true),
                    (None, _) | (_, None) => None,
                    _ => Some(false),
                };
                assert_eq!(truth(&format!("{a} AND {b}"), &doc), and, "{a} AND {b}");
                assert_eq!(truth(&format!("{a} OR {b}"), &doc), or, "{a} OR {b}");
            }
        }
    }

    #[test]
    fn an_integer_beyond_a_double_compares_exactly() {
        let filter = parse("id = 9007199254740993");
        assert!(filter.matches(&json!({ "id": 9007199254740993_i64 })));
        assert!(!filter.matches(&json!({ "id": 9007199254740992_i64 })));
        assert!(filter.matches(&json!({ "id": "9007199254740993" })));
        assert!(!filter.matches(&json!({ "id": "9007199254740992" })));
    }

    #[test]
    fn columns_are_listed_once_and_sorted() {
        let filter = parse("zeta = 1 AND alpha IS NULL OR zeta IN (2) AND NOT mid = 'x'");
        assert_eq!(filter.columns(), ["alpha", "mid", "zeta"]);
    }
}
