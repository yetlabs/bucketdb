//! Postgres SQL in, SQLite SQL out: parsed into sqlparser's AST, rewritten where the
//! two differ, printed back. What SQLite already takes stays as it is: `$1`
//! parameters (named parameters to SQLite), RETURNING, ON CONFLICT, quoted names.
//!
//! The rewrites: casts to SQLite's types, ILIKE, the regex operators, COLLATE, a
//! `public.` schema, row locks, the Postgres column types and serials of CREATE
//! TABLE, index methods, the statements that only set or show session state, and
//! the catalog functions psql's `\d` calls.

use crate::types;
use sqlparser::ast::{
    self, CastKind, ColumnOption, DataType, Expr, Ident, ObjectName, ObjectNamePart, Statement, TableConstraint, Value,
    visit_expressions_mut, visit_relations_mut,
};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::ops::ControlFlow;

/// A parameter_columns entry for a parameter that is an integer by its place.
pub const INTEGER_PARAM: &str = "\0integer";

/// What a client statement becomes.
#[derive(Debug, Clone)]
pub enum Plan {
    /// SQL for SQLite, with the tag its CommandComplete carries and the columns each
    /// `$n` stands for (for the parameter types).
    Sql {
        sql: String,
        tag: Tag,
        params: usize,
        param_columns: Vec<Option<String>>,
    },
    Begin,
    Commit,
    Rollback,
    /// SET name = value: kept for SHOW, otherwise a no-op.
    Set {
        name: String,
        value: String,
    },
    Show {
        name: String,
    },
    /// Accepted and ignored (DISCARD, CREATE EXTENSION, LISTEN...).
    Noop {
        tag: String,
    },
    Empty,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tag {
    Select,
    Insert,
    Update,
    Delete,
    Other(&'static str),
}

/// See the module documentation.
///
/// # Errors
///
/// When the text is not Postgres SQL that bucketdb can run.
pub fn parse(sql: &str) -> Result<Vec<Plan>, String> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return Ok(vec![Plan::Empty]);
    }
    let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).map_err(|e| e.to_string())?;
    stmts.into_iter().map(plan).collect()
}

fn ident_text(i: &Ident) -> String {
    i.value.clone()
}

fn object_text(o: &ObjectName) -> String {
    o.0.iter()
        .filter_map(|p| match p {
            ObjectNamePart::Identifier(i) => Some(i.value.clone()),
            ObjectNamePart::Function(_) => None,
        })
        .collect::<Vec<_>>()
        .join(".")
}

fn plan(mut stmt: Statement) -> Result<Plan, String> {
    let tag = match &stmt {
        Statement::StartTransaction { .. } => return Ok(Plan::Begin),
        Statement::Commit { .. } => return Ok(Plan::Commit),
        Statement::Rollback { savepoint: None, .. } => return Ok(Plan::Rollback),
        Statement::Set(set) => return Ok(plan_set(set)),
        Statement::ShowVariable { variable } => {
            return Ok(Plan::Show {
                name: variable.iter().map(ident_text).collect::<Vec<_>>().join(" "),
            });
        }
        Statement::Discard { .. } => {
            return Ok(Plan::Noop {
                tag: "DISCARD ALL".into(),
            });
        }
        Statement::CreateExtension(_) => {
            return Ok(Plan::Noop {
                tag: "CREATE EXTENSION".into(),
            });
        }
        Statement::Query(_) => Tag::Select,
        Statement::Insert(_) => Tag::Insert,
        Statement::Update(_) => Tag::Update,
        Statement::Delete(_) => Tag::Delete,
        Statement::CreateTable(_) => Tag::Other("CREATE TABLE"),
        Statement::CreateIndex(_) => Tag::Other("CREATE INDEX"),
        Statement::CreateView { .. } => Tag::Other("CREATE VIEW"),
        Statement::Drop { object_type, .. } => match object_type {
            ast::ObjectType::Table => Tag::Other("DROP TABLE"),
            ast::ObjectType::Index => Tag::Other("DROP INDEX"),
            ast::ObjectType::View => Tag::Other("DROP VIEW"),
            _ => Tag::Other("DROP"),
        },
        Statement::AlterTable(_) => {
            // ADD CONSTRAINT ... UNIQUE (cols) is a unique index in SQLite.
            if let Statement::AlterTable(at) = &stmt
                && let [
                    ast::AlterTableOperation::AddConstraint {
                        constraint: TableConstraint::Unique(u),
                        ..
                    },
                ] = at.operations.as_slice()
            {
                let name = u
                    .name
                    .as_ref()
                    .map_or_else(|| "unique".to_string(), ToString::to_string);
                let cols: Vec<String> = u.columns.iter().map(ToString::to_string).collect();
                let sql = format!("CREATE UNIQUE INDEX {name} ON {} ({})", at.name, cols.join(", "));
                return Ok(Plan::Sql {
                    sql,
                    tag: Tag::Other("ALTER TABLE"),
                    params: 0,
                    param_columns: vec![],
                });
            }
            if let Statement::AlterTable(at) = &mut stmt
                && alter_table(at)?
            {
                return Ok(Plan::Noop {
                    tag: "ALTER TABLE".into(),
                });
            }
            Tag::Other("ALTER TABLE")
        }
        Statement::Savepoint { .. } => Tag::Other("SAVEPOINT"),
        Statement::ReleaseSavepoint { .. } => Tag::Other("RELEASE"),
        Statement::Rollback { .. } => Tag::Other("ROLLBACK"),
        Statement::Explain { .. } => Tag::Select,
        _ => Tag::Other("OK"),
    };
    rewrite(&mut stmt)?;
    let param_columns = param_columns(&stmt);
    let sql = stmt.to_string();
    let params = max_placeholder(&sql);
    let mut param_columns = param_columns;
    param_columns.resize(params, None);
    // LIMIT $n and OFFSET $n count rows: integers.
    for kw in ["LIMIT $", "OFFSET $"] {
        let mut rest = sql.as_str();
        while let Some(i) = rest.find(kw) {
            rest = &rest[i + kw.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if let Ok(n) = digits.parse::<usize>()
                && (1..=params).contains(&n)
                && param_columns[n - 1].is_none()
            {
                param_columns[n - 1] = Some(INTEGER_PARAM.to_string());
            }
        }
    }
    Ok(Plan::Sql {
        sql,
        tag,
        params,
        param_columns,
    })
}

fn plan_set(set: &ast::Set) -> Plan {
    let text = set.to_string();
    // SET [SESSION|LOCAL] name {=|TO} value, and SET TIME ZONE value.
    let body = text
        .trim_start_matches("SET ")
        .trim_start_matches("SESSION ")
        .trim_start_matches("LOCAL ");
    if let Some(v) = body.strip_prefix("TIME ZONE ") {
        return Plan::Set {
            name: "TimeZone".into(),
            value: unquote(v),
        };
    }
    let (name, value) = body
        .split_once(" = ")
        .or_else(|| body.split_once(" TO "))
        .or_else(|| body.split_once('='))
        .unwrap_or((body, ""));
    Plan::Set {
        name: name.trim().to_string(),
        value: unquote(value.trim()),
    }
}

fn unquote(v: &str) -> String {
    v.trim().trim_matches('\'').to_string()
}

/// The highest `$n` in the SQL: how many parameters the statement takes.
fn max_placeholder(sql: &str) -> usize {
    let b = sql.as_bytes();
    let mut max = 0;
    let mut i = 0;
    let mut in_str = false;
    while i < b.len() {
        match b[i] {
            b'\'' => in_str = !in_str,
            b'$' if !in_str => {
                let mut j = i + 1;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j > i + 1 {
                    max = max.max(sql[i + 1..j].parse().unwrap_or(0));
                }
                i = j;
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    max
}

fn parse_expr(sql: &str) -> Result<Expr, String> {
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .and_then(|mut p| p.parse_expr())
        .map_err(|e| e.to_string())
}

/// The SQLite type a cast goes to, None to keep the value as it is.
fn cast_target(dt: &DataType) -> Option<&'static str> {
    let t = dt.to_string().to_ascii_uppercase();
    let base = t.split('(').next().unwrap_or("").trim();
    match base {
        "INT" | "INT2" | "INT4" | "INT8" | "INTEGER" | "BIGINT" | "SMALLINT" => Some("INTEGER"),
        "REAL" | "FLOAT" | "FLOAT4" | "FLOAT8" | "DOUBLE PRECISION" | "DOUBLE" | "NUMERIC" | "DECIMAL" => Some("REAL"),
        "TEXT" | "VARCHAR" | "CHARACTER VARYING" | "CHAR" | "CHARACTER" | "BPCHAR" | "NAME" => Some("TEXT"),
        "BYTEA" => Some("BLOB"),
        "BOOL" | "BOOLEAN" => Some("BOOLEAN"),
        _ => None,
    }
}

/// The name Postgres gives an unnamed select-list expression: a function's name, a
/// cast's operand's, else ?column?. SQLite would name it by its text.
fn pg_name(e: &Expr) -> Option<String> {
    match e {
        Expr::Identifier(_) | Expr::CompoundIdentifier(_) => None,
        Expr::Function(f) => Some(match f.name.0.last() {
            Some(ObjectNamePart::Identifier(i)) => i.value.to_ascii_lowercase(),
            _ => "?column?".into(),
        }),
        Expr::Cast { expr, data_type, .. } => Some(match expr.as_ref() {
            Expr::Identifier(i) => i.value.clone(),
            Expr::CompoundIdentifier(v) => v.last().map(|i| i.value.clone()).unwrap_or_default(),
            inner => pg_name(inner).unwrap_or_else(|| data_type.to_string().to_ascii_lowercase()),
        }),
        Expr::Nested(inner) => pg_name(inner),
        _ => Some("?column?".into()),
    }
}

fn name_columns(q: &mut ast::Query) {
    if let ast::SetExpr::Select(sel) = q.body.as_mut() {
        for item in &mut sel.projection {
            if let ast::SelectItem::UnnamedExpr(e) = item
                && let Some(name) = pg_name(e)
            {
                *item = ast::SelectItem::ExprWithAlias {
                    expr: e.clone(),
                    alias: Ident::with_quote('"', name),
                };
            }
        }
    }
}

/// A select-list `x::regclass` shows the relation's name; elsewhere (`oid =
/// '16385'::regclass`) the cast keeps the OID.
fn regclass_names(q: &mut ast::Query) -> Result<(), String> {
    if let ast::SetExpr::Select(sel) = q.body.as_mut() {
        for item in &mut sel.projection {
            if let ast::SelectItem::UnnamedExpr(e) | ast::SelectItem::ExprWithAlias { expr: e, .. } = item
                && let Expr::Cast { expr, data_type, .. } = e
                && data_type.to_string().to_ascii_lowercase().ends_with("regclass")
            {
                *e = parse_expr(&format!("pg_regclass({expr})"))?;
            }
        }
    }
    Ok(())
}

/// `~`, `~*`, `!~`, `!~*`, also as `OPERATOR(pg_catalog.~)`: (negated, case-insensitive).
fn regex_op(op: &ast::BinaryOperator) -> Option<(bool, bool)> {
    use ast::BinaryOperator as B;
    match op {
        B::PGRegexMatch => Some((false, false)),
        B::PGRegexIMatch => Some((false, true)),
        B::PGRegexNotMatch => Some((true, false)),
        B::PGRegexNotIMatch => Some((true, true)),
        B::PGCustomBinaryOperator(v) => match v.last()?.as_str() {
            "~" => Some((false, false)),
            "~*" => Some((false, true)),
            "!~" => Some((true, false)),
            "!~*" => Some((true, true)),
            _ => None,
        },
        _ => None,
    }
}

/// The catalog functions psql calls, as SQL over the catalog views (funcs::CATALOG);
/// `pg_catalog.f` is f. None leaves the call as it is.
fn catalog_call(f: &mut ast::Function) -> Option<String> {
    if f.name.0.len() == 2
        && matches!(&f.name.0[0], ObjectNamePart::Identifier(s) if s.value.eq_ignore_ascii_case("pg_catalog"))
    {
        f.name.0.remove(0);
    }
    let ObjectNamePart::Identifier(name) = f.name.0.last()? else {
        return None;
    };
    let args: Vec<String> = match &f.args {
        ast::FunctionArguments::List(l) => l.args.iter().map(ToString::to_string).collect(),
        _ => vec![],
    };
    let arg = |i: usize| args.get(i).cloned().unwrap_or_else(|| "NULL".into());
    Some(match name.value.to_ascii_lowercase().as_str() {
        "pg_table_is_visible" | "pg_relation_is_publishable" => "1".into(),
        "pg_get_userbyid" => "'postgres'".into(),
        "pg_encoding_to_char" => "'UTF8'".into(),
        "pg_get_expr" | "pg_partition_ancestors" => arg(0),
        "pg_get_indexdef" => format!(
            "pg_lookup('SELECT indexdef FROM pg_bucketdb_index WHERE oid = ?', {})",
            arg(0)
        ),
        "pg_get_constraintdef" => format!(
            "pg_lookup('SELECT condef FROM pg_constraint WHERE oid = ?', {})",
            arg(0)
        ),
        "obj_description" | "col_description" | "shobj_description" => "NULL".into(),
        // There are no arrays: psql joins role arrays of row policies, which are none.
        "array_to_string" | "pg_get_statisticsobjdef_columns" | "pg_get_triggerdef" => "NULL".into(),
        _ => return None,
    })
}

/// A table is never partitioned: `pg_partition_ancestors(x) [WITH ORDINALITY] AS a
/// (relid, depth)` in a FROM, as psql writes it, is an empty table.
struct NoPartitions;

impl ast::VisitorMut for NoPartitions {
    type Break = ();

    fn post_visit_table_factor(&mut self, tf: &mut ast::TableFactor) -> ControlFlow<()> {
        if let ast::TableFactor::Table {
            name,
            args: Some(_),
            alias,
            ..
        } = tf
            && matches!(name.0.last(), Some(ObjectNamePart::Identifier(i)) if i.value == "pg_partition_ancestors")
        {
            let mut alias = alias.take();
            let cols: Vec<String> = alias
                .as_mut()
                .map(|a| {
                    std::mem::take(&mut a.columns)
                        .into_iter()
                        .map(|c| c.name.to_string())
                        .collect()
                })
                .unwrap_or_default();
            let cols = if cols.is_empty() {
                vec!["relid".to_string()]
            } else {
                cols
            };
            let select = cols
                .iter()
                .map(|c| format!("NULL AS {c}"))
                .collect::<Vec<_>>()
                .join(", ");
            if let Ok(q) = Parser::new(&PostgreSqlDialect {})
                .try_with_sql(&format!("SELECT {select} WHERE 0"))
                .and_then(|mut p| p.parse_query())
            {
                *tf = ast::TableFactor::Derived {
                    lateral: false,
                    subquery: q,
                    alias,
                    sample: None,
                };
            }
        }
        ControlFlow::Continue(())
    }
}

fn rewrite(stmt: &mut Statement) -> Result<(), String> {
    let _ = ast::VisitMut::visit(stmt, &mut NoPartitions);
    if let Statement::Query(q) = stmt {
        name_columns(q);
        regclass_names(q)?;
    }
    let _ = visit_relations_mut(stmt, |name: &mut ObjectName| {
        if name.0.len() == 2
            && let (ObjectNamePart::Identifier(schema), ObjectNamePart::Identifier(rel)) = (&name.0[0], &name.0[1])
        {
            let schema = schema.value.to_ascii_lowercase();
            let rel = rel.value.to_ascii_lowercase();
            match schema.as_str() {
                // public.t is the database's t; SQLite's own schema is main.
                "public" => {
                    name.0.remove(0);
                }
                // The catalogs are per-connection views (funcs::CATALOG).
                "information_schema" => {
                    *name = ObjectName::from(vec![Ident::new(format!("information_schema_{rel}"))]);
                }
                "pg_catalog" => *name = ObjectName::from(vec![Ident::new(rel)]),
                _ => {}
            }
        }
        ControlFlow::<()>::Continue(())
    });

    let mut err = None;
    let _ = visit_expressions_mut(stmt, |e: &mut Expr| {
        match e {
            Expr::Cast {
                kind, expr, data_type, ..
            } => match cast_target(data_type) {
                Some("BOOLEAN") => match parse_expr(&format!("pg_bool({expr})")) {
                    Ok(x) => *e = x,
                    Err(m) => err = Some(m),
                },
                Some(t) => {
                    *kind = CastKind::Cast;
                    *data_type = DataType::Custom(ObjectName::from(vec![Ident::new(t)]), vec![]);
                }
                None => *e = Expr::Nested(expr.clone()),
            },
            Expr::ILike {
                negated,
                expr,
                pattern,
                escape_char,
                ..
            } => {
                let not = if *negated { "NOT " } else { "" };
                let esc = escape_char.as_ref().map(|c| format!(" ESCAPE {c}")).unwrap_or_default();
                match parse_expr(&format!("lower({expr}) {not}LIKE lower({pattern}){esc}")) {
                    Ok(x) => *e = x,
                    Err(m) => err = Some(m),
                }
            }
            // SQLite's collations are not Postgres': the default it is.
            Expr::Collate { expr, .. } => *e = Expr::Nested(expr.clone()),
            Expr::BinaryOp { left, op, right } => {
                if let Some((negated, ci)) = regex_op(op) {
                    let not = if negated { "NOT " } else { "" };
                    match parse_expr(&format!("{not}pg_regex({left}, {right}, {})", i32::from(ci))) {
                        Ok(x) => *e = x,
                        Err(m) => err = Some(m),
                    }
                }
            }
            // x = ANY(list): the catalogs keep lists as text ('{a,b}', int2vector's '1 2').
            Expr::AnyOp {
                left,
                compare_op: ast::BinaryOperator::Eq,
                right,
                ..
            } => match parse_expr(&format!("pg_any({left}, {right})")) {
                Ok(x) => *e = x,
                Err(m) => err = Some(m),
            },
            Expr::Function(f) => {
                if let Some(sql) = catalog_call(f) {
                    match parse_expr(&sql) {
                        Ok(x) => *e = Expr::Nested(Box::new(x)),
                        Err(m) => err = Some(m),
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    if let Some(m) = err {
        return Err(m);
    }

    match stmt {
        Statement::Query(q) => {
            q.locks.clear();
        }
        Statement::CreateTable(ct) => create_table(ct),
        Statement::CreateIndex(ci) => {
            ci.using = None;
            ci.concurrently = false;
            ci.include.clear();
            ci.with.clear();
            ci.nulls_distinct = None;
        }
        _ => {}
    }
    Ok(())
}

/// Postgres column types to the decls bucketdb keeps, and a serial key to SQLite's
/// INTEGER PRIMARY KEY, which takes the next rowid when no value is given.
fn create_table(ct: &mut ast::CreateTable) {
    let single_pk: Option<String> = ct.constraints.iter().find_map(|c| match c {
        TableConstraint::PrimaryKey(pk) if pk.columns.len() == 1 => {
            Some(pk.columns[0].to_string().trim_matches('"').to_string())
        }
        _ => None,
    });
    let mut moved_pk = false;
    for col in &mut ct.columns {
        let pg = col.data_type.to_string();
        let serial = types::is_serial(&pg);
        let col_pk = col
            .options
            .iter()
            .any(|o| matches!(o.option, ColumnOption::PrimaryKey(_)));
        let table_pk = single_pk.as_deref() == Some(col.name.value.as_str());
        if serial && (col_pk || table_pk) || (table_pk && is_integer(&pg)) {
            col.data_type = DataType::Custom(ObjectName::from(vec![Ident::new("INTEGER")]), vec![]);
            if !col_pk
                && let Some(TableConstraint::PrimaryKey(pk)) = ct
                    .constraints
                    .iter()
                    .find(|c| matches!(c, TableConstraint::PrimaryKey(_)))
                    .cloned()
            {
                col.options.push(ast::ColumnOptionDef {
                    name: None,
                    option: ColumnOption::PrimaryKey(pk),
                });
                moved_pk = true;
            }
        } else {
            map_column(col);
        }
        parenthesize_default(col);
    }
    if moved_pk {
        ct.constraints.retain(|c| !matches!(c, TableConstraint::PrimaryKey(_)));
    }
}

fn map_column(col: &mut ast::ColumnDef) {
    let pg = col.data_type.to_string();
    col.data_type = DataType::Custom(ObjectName::from(vec![Ident::new(types::sqlite_decl(&pg))]), vec![]);
}

/// SQLite takes an expression default only in parentheses.
fn parenthesize_default(col: &mut ast::ColumnDef) {
    for opt in &mut col.options {
        if let ColumnOption::Default(d) = &mut opt.option
            && !matches!(d, Expr::Value(_))
        {
            *d = Expr::Nested(Box::new(d.clone()));
        }
    }
}

/// ALTER TABLE: SQLite adds, renames and drops columns. A column's type or default
/// has no ALTER in SQLite; its type is only the name its OID comes from, so those
/// become no-ops. NOT NULL changes would change what the table accepts: refused.
fn alter_table(at: &mut ast::AlterTable) -> Result<bool, String> {
    let mut all_noop = !at.operations.is_empty();
    for op in &mut at.operations {
        match op {
            // SQLite has no ADD CONSTRAINT: a foreign key or a check added afterwards is
            // not enforced (one in CREATE TABLE is).
            ast::AlterTableOperation::AddConstraint { constraint, .. }
                if matches!(constraint, TableConstraint::ForeignKey(_) | TableConstraint::Check(_)) =>
            {
                log::warn!(
                    "bucketdb: {constraint} on {} is not enforced: SQLite adds constraints only in CREATE TABLE",
                    at.name
                );
            }
            ast::AlterTableOperation::AddColumn { column_def, .. } => {
                all_noop = false;
                map_column(column_def);
                parenthesize_default(column_def);
            }
            ast::AlterTableOperation::AlterColumn { column_name, op } => match op {
                ast::AlterColumnOperation::SetDataType { .. }
                | ast::AlterColumnOperation::SetDefault { .. }
                | ast::AlterColumnOperation::DropDefault => {
                    log::debug!("bucketdb: ALTER COLUMN {column_name} {op} kept as a no-op");
                }
                _ => return Err(format!("ALTER COLUMN {column_name} {op} is not supported")),
            },
            _ => all_noop = false,
        }
    }
    Ok(all_noop)
}

fn is_integer(pg: &str) -> bool {
    matches!(
        pg.to_ascii_uppercase().as_str(),
        "INT" | "INTEGER" | "INT4" | "BIGINT" | "INT8"
    )
}

/// The column each `$n` meets: `col op $n`, `col IN ($n, ...)`, `col BETWEEN $n AND
/// $m`, an INSERT's column list against its VALUES, an UPDATE's SET.
fn param_columns(stmt: &Statement) -> Vec<Option<String>> {
    let mut found: HashMap<usize, String> = HashMap::new();
    let ph = |e: &Expr| -> Option<usize> {
        match e {
            Expr::Value(v) => match &v.value {
                Value::Placeholder(p) => p.strip_prefix('$')?.parse().ok(),
                _ => None,
            },
            _ => None,
        }
    };
    let col = |e: &Expr| -> Option<String> {
        match e {
            Expr::Identifier(i) => Some(i.value.clone()),
            Expr::CompoundIdentifier(v) => v.last().map(|i| i.value.clone()),
            _ => None,
        }
    };
    let mut s = stmt.clone();
    let _ = visit_expressions_mut(&mut s, |e: &mut Expr| {
        match e {
            Expr::BinaryOp { left, right, .. } => {
                if let (Some(c), Some(n)) = (col(left), ph(right)) {
                    found.entry(n).or_insert(c);
                } else if let (Some(n), Some(c)) = (ph(left), col(right)) {
                    found.entry(n).or_insert(c);
                }
            }
            Expr::In {
                expr,
                target: ast::InTarget::List(list),
                ..
            } => {
                if let Some(c) = col(expr) {
                    for item in list.iter() {
                        if let Some(n) = ph(item) {
                            found.entry(n).or_insert_with(|| c.clone());
                        }
                    }
                }
            }
            Expr::Between { expr, low, high, .. } => {
                if let Some(c) = col(expr) {
                    for b in [low, high] {
                        if let Some(n) = ph(b) {
                            found.entry(n).or_insert_with(|| c.clone());
                        }
                    }
                }
            }
            _ => {}
        }
        ControlFlow::<()>::Continue(())
    });
    match stmt {
        Statement::Insert(ins) => {
            let cols: Vec<String> = ins.columns.iter().map(object_text).collect();
            if let Some(src) = &ins.source
                && let ast::SetExpr::Values(v) = src.body.as_ref()
            {
                for row in &v.rows {
                    for (i, e) in row.iter().enumerate() {
                        if let (Some(c), Some(n)) = (cols.get(i), ph(e)) {
                            found.entry(n).or_insert_with(|| c.clone());
                        }
                    }
                }
            }
        }
        Statement::Update(up) => {
            for a in &up.assignments {
                if let ast::AssignmentTarget::ColumnName(name) = &a.target
                    && let Some(n) = ph(&a.value)
                {
                    let c = object_text(name);
                    found.entry(n).or_insert(c.rsplit('.').next().unwrap_or(&c).to_string());
                }
            }
        }
        _ => {}
    }
    let max = found.keys().copied().max().unwrap_or(0);
    (1..=max).map(|n| found.get(&n).cloned()).collect()
}

/// The tables a statement names, for the declared types of its columns.
pub fn tables(sql: &str) -> Vec<String> {
    let Ok(mut stmts) = Parser::parse_sql(&PostgreSqlDialect {}, sql) else {
        return vec![];
    };
    let mut out = Vec::new();
    for s in &mut stmts {
        let _ = visit_relations_mut(s, |name: &mut ObjectName| {
            out.push(object_text(name));
            ControlFlow::<()>::Continue(())
        });
    }
    out
}
