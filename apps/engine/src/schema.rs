//! Schema types (deserialized from the control-plane JSON, mirroring `@electric-circuits/protocol`)
//! and their compiled, positional runtime form, plus the **schema fingerprint** the engine compares
//! against what Postgres now reports for a table (ADR-0005).

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

use crate::table_ref::TableRef;
use crate::value::{Row, Value};

/// The compiled table schemas, shared (and atomically swappable) between the `Engine`, the
/// sequencer and the replication ingestor. A std lock: reads are brief and never held across an
/// await. Schema drift replaces one entry in place, so every holder sees the new schema at once
/// (ADR-0005) — nobody keeps a private immutable copy.
pub type SharedTables = Arc<std::sync::RwLock<HashMap<TableRef, TableSchema>>>;

/// `pg_class.relreplident` for `REPLICA IDENTITY FULL` — the only value the engine can serve: an
/// UPDATE/DELETE must carry the full old row for the delta algebra to retract it.
pub const REPLICA_IDENTITY_FULL: u8 = b'f';

/// One column exactly as Postgres reports it — `pg_attribute` at introspection, the pgoutput
/// `Relation` message on the wire. `name`/`type_oid`/`typmod` are what decides whether a compiled
/// schema still *describes* the relation; the position in [`SchemaFingerprint::columns`] is the
/// `attnum` order, which is also the order pgoutput tuples arrive in.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FingerprintColumn {
    pub name: String,
    /// `pg_attribute.atttypid`.
    pub type_oid: u32,
    /// `pg_attribute.atttypmod` (`varchar(n)` length, `numeric(p,s)`, …; `-1` when unqualified).
    pub typmod: i32,
}

/// What Postgres says a table looks like *right now*: live columns in `attnum` order, the
/// relation's replica identity, and its primary key. Compared against the compiled copy on every
/// `Relation` message and on the reconciler's tick; a difference is schema drift and retires the
/// table's dependents.
///
/// Deliberately independent of [`TableSchema::columns`] (which is sorted, coarse-typed and carries
/// the engine's own positional order): the fingerprint is Postgres's view, not the engine's.
///
/// **`pk` is only known to the catalog half.** A fingerprint read from `pg_index` carries
/// `Some(cols)`; one derived from a pgoutput `Relation` message carries `None`, because under
/// `REPLICA IDENTITY FULL` every column is flagged as part of the identity key, so the wire says
/// nothing about the primary key. [`still_serves`](Self::still_serves) therefore compares `pk` only
/// when BOTH sides have it — which means a primary-key change is caught by the reconciler (and by
/// any re-introspection), not by the `R` compare.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFingerprint {
    pub columns: Vec<FingerprintColumn>,
    /// `pg_class.relreplident` (`f` = FULL, `d` = default/pk, `n` = nothing, `i` = index).
    #[serde(with = "replident_serde")]
    pub replident: u8,
    /// Primary-key column names in key order (`pg_index.indkey`), or `None` when the source cannot
    /// know them — see the type docs.
    #[serde(default)]
    pub pk: Option<Vec<String>>,
}

/// Render `relreplident` as the one-character string Postgres spells it with, so a durable audit
/// record reads `"f"` rather than `102`.
mod replident_serde {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &u8, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&(*v as char).to_string())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u8, D::Error> {
        let s = String::deserialize(d)?;
        Ok(s.as_bytes().first().copied().unwrap_or(b'?'))
    }
}

impl SchemaFingerprint {
    /// True iff the compiled schema still describes `observed` **and** the replica identity is
    /// still FULL. Both halves matter: identical columns with a regressed identity means updates
    /// and deletes in between were mis-applied, which is drift just as much as a new column is.
    ///
    /// The primary key is compared only when both sides know it (see the type docs).
    pub fn still_serves(&self, observed: &SchemaFingerprint) -> bool {
        self.columns == observed.columns && observed.replident == REPLICA_IDENTITY_FULL && self.pk_matches(observed)
    }

    /// The primary keys agree, or one of the two sides does not know its own.
    fn pk_matches(&self, observed: &SchemaFingerprint) -> bool {
        match (&self.pk, &observed.pk) {
            (Some(a), Some(b)) => a == b,
            _ => true,
        }
    }

    /// Column names in `attnum` order (what a pgoutput tuple's cells zip against).
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    /// A 64-bit digest of this fingerprint: FNV-1a over its JSON encoding (ADR-0010).
    ///
    /// This is what the ingestor stamps onto every change-log envelope, so the sequencer can tell an
    /// envelope its compiled schema DESCRIBES from one encoded under a schema a drift has since
    /// replaced. Determinism is the whole property: the struct's field order and the `Vec`'s column
    /// order fix the bytes, so two processes that introspected the same table compute the same digest.
    ///
    /// It is a fence, not a security boundary — a collision would let a pre-drift envelope be decoded
    /// as current, which fails loudly rather than silently (the decode is strict), so 64 bits with no
    /// cryptographic strength is the right trade.
    ///
    /// **Changing the encoding — this function, the JSON form, or [`SchemaFingerprint`]'s fields —
    /// changes every stored digest**, so every envelope already on the change log would read as
    /// pre-drift and be skipped. An upgrade that does that needs an epoch reset (ADR-0010).
    pub fn digest(&self) -> u64 {
        // Never fails: every field is a plain scalar/`Vec`/`Option` (`replident` serialises as a
        // one-character string). An encoder error would mean the type changed, and a digest of
        // nothing would compare equal across DIFFERENT schemas — so fall back to a value no
        // fingerprint can produce rather than to 0.
        let Ok(bytes) = serde_json::to_vec(self) else { return u64::MAX };
        fnv1a64(&bytes)
    }
}

/// FNV-1a, 64-bit. Inline (ten lines) rather than a dependency: the digest is a fence over a few
/// dozen bytes of JSON, and the constants are part of the durable format either way.
fn fnv1a64(bytes: &[u8]) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x1000_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for b in bytes {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

/// A schema digest as it travels on the wire: 16 lowercase hex characters.
///
/// A STRING, not a number, because the change log is JSON and the conformance harness reads it with
/// `JSON.parse`: an integer above 2^53 does not survive that.
pub fn digest_hex(digest: u64) -> String {
    format!("{digest:016x}")
}

/// The digest a wire stamp names, or `None` when the stamp is not one.
///
/// Anything that is not EXACTLY what [`digest_hex`] writes — 16 lowercase hex characters — is read as
/// ABSENT rather than as a different schema: "absent" decodes the envelope strictly (a failure is
/// loud), while "different" would skip it, and silently skipping a change is the one outcome the stamp
/// exists to prevent. So the format is checked rather than left to `from_str_radix`, which happily
/// accepts a truncated stamp, a sign, or uppercase — each of which would parse as some OTHER digest
/// and be skipped as pre-drift.
pub fn digest_from_hex(hex: &str) -> Option<u64> {
    if hex.len() != 16 || !hex.bytes().all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Name what changed between the compiled fingerprint and the observed one, for the drift log.
/// Empty iff nothing did — so it is also the pure "are these the same table?" predicate
/// [`SchemaFingerprint::still_serves`] is built on (this one additionally reports a non-FULL
/// replica identity, which is drift even when the columns match).
pub fn describe_drift(compiled: &SchemaFingerprint, observed: &SchemaFingerprint) -> Vec<String> {
    let mut out = Vec::new();
    let old: BTreeMap<&str, &FingerprintColumn> = compiled.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    let new: BTreeMap<&str, &FingerprintColumn> = observed.columns.iter().map(|c| (c.name.as_str(), c)).collect();
    for name in new.keys().filter(|n| !old.contains_key(*n)) {
        out.push(format!("column '{name}' added"));
    }
    for name in old.keys().filter(|n| !new.contains_key(*n)) {
        out.push(format!("column '{name}' dropped"));
    }
    for (name, c) in &new {
        let Some(o) = old.get(name) else { continue };
        if o.type_oid != c.type_oid || o.typmod != c.typmod {
            out.push(format!(
                "column '{name}' retyped (oid {}/typmod {} -> oid {}/typmod {})",
                o.type_oid, o.typmod, c.type_oid, c.typmod
            ));
        }
    }
    // Same columns, different attnum order (a drop+add cycle): the tuple cells no longer line up
    // with the names the decoder zips them against, so it is drift even though nothing was gained
    // or lost.
    if out.is_empty() && compiled.column_names() != observed.column_names() {
        out.push(format!(
            "columns reordered ({} -> {})",
            compiled.column_names().join(","),
            observed.column_names().join(",")
        ));
    }
    if let (Some(before), Some(after)) = (&compiled.pk, &observed.pk)
        && before != after
    {
        out.push(format!("primary key changed ({} -> {})", before.join(","), after.join(",")));
    }
    if observed.replident != REPLICA_IDENTITY_FULL {
        out.push(format!("replica identity '{}' (expected 'f' = FULL)", observed.replident as char));
    }
    out
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    Int,
    Text,
    Bool,
    Float,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ColumnDef {
    #[serde(rename = "type")]
    pub ty: ColumnType,
    /// The raw Postgres type name (`pg_type.typname` / `udt_name`, e.g. `uuid`, `timestamptz`) captured
    /// at introspection. `None` in library mode (schema defined via JSON, which only carries the coarse
    /// [`ColumnType`]). Used to cast bound text params to the native type in backfill SQL so uuid/int
    /// comparisons stay index-eligible (see `sql.rs`).
    #[serde(default, skip)]
    pub pg_type: Option<String>,
    /// Whether Postgres auto-supplies this column's value when omitted — i.e. it's an IDENTITY column
    /// or carries a `DEFAULT`. Captured at introspection; `false` in library mode. Lets the visualizer
    /// mark the column optional in its add-row form.
    #[serde(default, skip)]
    pub has_default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TableDef {
    // BTreeMap gives a deterministic (sorted) column order regardless of JSON key order, which
    // we use as the positional Row order. Only internal consistency matters.
    pub columns: BTreeMap<String, ColumnDef>,
    // Accepts a single column name (`"id"`) or an ordered list (`["a","b"]`) for composite keys.
    #[serde(rename = "primaryKey", deserialize_with = "de_primary_key")]
    pub primary_key: Vec<String>,
    /// What Postgres reported for the table at introspection (ADR-0005). `None` in **library
    /// mode** — the JSON schema carries no catalog detail, there is no Postgres to compare
    /// against, and drift checking is therefore off for that table.
    #[serde(default, skip)]
    pub fingerprint: Option<SchemaFingerprint>,
}

fn de_primary_key<'de, D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum One {
        Many(Vec<String>),
        Single(String),
    }
    Ok(match One::deserialize(d)? {
        One::Single(s) => vec![s],
        One::Many(v) => v,
    })
}

#[derive(Debug, Clone, Deserialize)]
pub struct Schema {
    /// Keyed by table reference: a bare key is the `public.<name>` sugar, canonicalised by
    /// [`TableRef`]'s `Deserialize` (the single parse rule) as the library-mode `POST /schema` body
    /// is read.
    pub tables: BTreeMap<TableRef, TableDef>,
}

/// Compiled, positional view of one table: sorted columns, a name->index map, and the pk index.
#[derive(Debug, Clone)]
pub struct TableSchema {
    /// The table's identity; its canonical `schema.name` `Display` is what goes on the wire.
    pub table: TableRef,
    pub columns: Vec<(String, ColumnType)>,
    pub index: HashMap<String, usize>,
    /// First primary-key column index/name/type. For single-PK tables this IS the pk; for composite-PK
    /// tables (only used as subquery inner tables) it's the first key column — prefer `pk_cols`/`key_string`.
    pub pk_index: usize,
    pub pk_name: String,
    pub pk_type: ColumnType,
    /// All primary-key column indices, in order. Length 1 for the common single-PK case.
    pub pk_cols: Vec<usize>,
    /// Raw Postgres type name per column (parallel to [`Self::columns`]); `None` in library mode. Used
    /// to cast bound text params to the native type in backfill SQL (index-eligible comparisons).
    pub pg_types: Vec<Option<String>>,
    /// Whether each column is auto-defaulted (IDENTITY or `DEFAULT`), parallel to [`Self::columns`];
    /// all `false` in library mode. Surfaced by `GET /table/{name}/schema` so the add-row form can
    /// treat these columns as optional.
    pub has_defaults: Vec<bool>,
    /// Postgres's own view of the table when this schema was compiled (ADR-0005). Compared with
    /// every pgoutput `Relation` message and with the reconciler's catalog read; a difference
    /// retires the table's dependents. `None` in library mode ⇒ no drift checking.
    pub fingerprint: Option<SchemaFingerprint>,
    /// [`SchemaFingerprint::digest`] of [`Self::fingerprint`], computed once here (ADR-0010): the
    /// ingestor stamps it on every envelope it decodes under this schema, and the sequencer compares
    /// it against the schema it is about to decode with. `None` in library mode — there is no
    /// fingerprint and no drift, so there is nothing to fence.
    pub schema_digest: Option<u64>,
}

/// Separator joining composite-key column values into the durable-stream `key` string. Chosen to not
/// collide with real id text (the standard schema's ids are `l1-1` etc.).
pub const PK_SEP: char = '\u{1f}';

/// Escape one component of a **composite** key so the joined string is an injective encoding of the
/// tuple: `\` becomes `\\` and the separator itself becomes `\x1f`. An escaped component therefore
/// contains no bare separator and no bare backslash, so splitting the result on U+001F recovers
/// exactly the original components — and two distinct Postgres key tuples can never spell the same
/// native key. (`(x, "y\u{1f}z")` and `("x\u{1f}y", z)` used to collide, and `translate_output`
/// de-duplicates by this string, so one of the two rows was lost.)
///
/// Single-column keys are NOT escaped: their key string is the value itself, which is what every
/// client-visible key, `Value::from_key_string`, and the routing/dedup paths already assume. The
/// encoding is greenfield — clients resync at cutover, so there is no legacy form to accept.
pub fn escape_key_component(part: &str) -> String {
    if !part.contains('\\') && !part.contains(PK_SEP) {
        return part.to_string();
    }
    let mut out = String::with_capacity(part.len() + 8);
    for c in part.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            PK_SEP => out.push_str("\\x1f"),
            _ => out.push(c),
        }
    }
    out
}

/// Join already-stringified key components into the composite key string (escaping each). The one
/// implementation, shared by [`TableSchema::key_string`] and the replication decoder's
/// `key_from_obj`, so the backfill and the live path spell an identical key for the same row.
pub fn join_key_components<I: IntoIterator<Item = S>, S: AsRef<str>>(parts: I) -> String {
    let mut out = String::new();
    for (i, part) in parts.into_iter().enumerate() {
        if i > 0 {
            out.push(PK_SEP);
        }
        out.push_str(&escape_key_component(part.as_ref()));
    }
    out
}

impl TableSchema {
    pub fn from_def(table: &TableRef, def: &TableDef) -> Result<Self> {
        let columns: Vec<(String, ColumnType)> = def.columns.iter().map(|(c, d)| (c.clone(), d.ty)).collect();
        let pg_types: Vec<Option<String>> = def.columns.values().map(|d| d.pg_type.clone()).collect();
        let has_defaults: Vec<bool> = def.columns.values().map(|d| d.has_default).collect();
        let index: HashMap<String, usize> = columns.iter().enumerate().map(|(i, (c, _))| (c.clone(), i)).collect();
        if def.primary_key.is_empty() {
            anyhow::bail!("table '{table}' has no primary key");
        }
        let pk_cols: Vec<usize> = def
            .primary_key
            .iter()
            .map(|c| index.get(c).copied().ok_or_else(|| anyhow::anyhow!("primaryKey '{c}' is not a column")))
            .collect::<Result<_>>()?;
        let pk_index = pk_cols[0];
        let pk_type = columns[pk_index].1;
        Ok(TableSchema {
            table: table.clone(),
            columns,
            index,
            pk_index,
            pk_name: def.primary_key[0].clone(),
            pk_type,
            pk_cols,
            pg_types,
            has_defaults,
            schema_digest: def.fingerprint.as_ref().map(SchemaFingerprint::digest),
            fingerprint: def.fingerprint.clone(),
        })
    }

    /// Is this column a **collation-dependent text** column — one whose SQL ordering depends on the
    /// database's collation AND whose value reaches a client as its own text?
    ///
    /// The engine compares text in **code-point order** everywhere it evaluates a predicate itself
    /// (Rust `String` ordering), and the subset client can only compare the strings it received. So
    /// every SQL site that ORDERS or RANGE-COMPARES such a column spells `COLLATE "C"` explicitly,
    /// making Postgres, the engine and the client agree whatever the database's default collation
    /// is (`pg::order_term`, `sql::build`).
    ///
    /// Only genuinely collatable types qualify, and only when the Postgres type is **known**: our
    /// coarse `Text` also covers `uuid`, `timestamptz` and friends, whose order is
    /// collation-independent and which REFUSE a collation outright (`collations are not supported by
    /// type uuid`). `None` — a schema declared through `POST /schema` rather than introspected, which
    /// is possible in every mode because that route overwrites introspected schemas — is therefore
    /// NOT collated: the declaration says "text", but the column underneath may be a `uuid`, and
    /// emitting `COLLATE "C"` for it would turn every subset page on that table into an error.
    /// The consequence is worth stating plainly: **the code-point ordering guarantee needs
    /// introspection.** A JSON-declared text column is ordered and range-compared in the database's
    /// default collation, which the subset client cannot reproduce.
    pub fn is_collatable_text(&self, col: usize) -> bool {
        if self.columns.get(col).map(|(_, ty)| *ty) != Some(ColumnType::Text) {
            return false;
        }
        match self.pg_types.get(col).and_then(|o| o.as_deref()) {
            None => false,
            Some(t) => matches!(t, "text" | "varchar" | "bpchar" | "char" | "name" | "citext"),
        }
    }

    /// The raw Postgres type name of a column by name (for casting bound params to the native type).
    /// `None` in library mode or for an unknown column.
    pub fn pg_type_of(&self, col: &str) -> Option<&str> {
        self.index.get(col).and_then(|&i| self.pg_types.get(i)).and_then(|o| o.as_deref())
    }

    /// The durable-stream event `key` for a row: the single PK value's key-string, or composite PK
    /// column values ESCAPED ([`escape_key_component`]) and joined by [`PK_SEP`]. This is the row
    /// identity used for routing, dedup, and subquery contributor ref-counting — so the encoding has
    /// to be injective, or two legal Postgres tuples fold into one row.
    pub fn key_string(&self, row: &Row) -> Result<String> {
        if self.pk_cols.len() == 1 {
            return Ok(row.get(self.pk_cols[0])?.to_key_string());
        }
        let parts: Vec<String> =
            self.pk_cols.iter().map(|&i| row.get(i).map(Value::to_key_string)).collect::<Result<_>>()?;
        Ok(join_key_components(parts))
    }

    pub fn column_index(&self, col: &str) -> Result<usize> {
        self.index.get(col).copied().ok_or_else(|| anyhow::anyhow!("unknown column '{col}'"))
    }

    pub fn column_type(&self, idx: usize) -> ColumnType {
        self.columns[idx].1
    }

    /// Build a positional `Row` from a JSON object (the envelope `value`).
    pub fn row_from_json(&self, obj: &serde_json::Map<String, serde_json::Value>) -> Result<Row> {
        let mut cols = Vec::with_capacity(self.columns.len());
        let null = serde_json::Value::Null;
        for (cname, cty) in &self.columns {
            let j = obj.get(cname).unwrap_or(&null);
            cols.push(Value::from_json(j, *cty)?);
        }
        Ok(Row(cols))
    }

    /// Serialize a `Row` back to a JSON object keyed by column name.
    pub fn row_to_json(&self, row: &Row) -> serde_json::Value {
        self.row_to_json_cols(row, None)
    }

    /// Serialize a row to a JSON object. With `cols = Some(indices)` only those columns are emitted
    /// (a shape's output projection); `None` emits the full row. Used to keep large unused columns out
    /// of a shape's stream (e.g. the list view never reads `description`).
    pub fn row_to_json_cols(&self, row: &Row, cols: Option<&[usize]>) -> serde_json::Value {
        let mut m = serde_json::Map::with_capacity(cols.map_or(self.columns.len(), <[usize]>::len));
        let mut emit = |i: usize| {
            if let Some((cname, _)) = self.columns.get(i) {
                let v = row.0.get(i).cloned().unwrap_or(Value::Null);
                m.insert(cname.clone(), v.to_json());
            }
        };
        match cols {
            Some(cols) => cols.iter().for_each(|&i| emit(i)),
            None => (0..self.columns.len()).for_each(emit),
        }
        serde_json::Value::Object(m)
    }

    pub fn pk_of<'a>(&self, row: &'a Row) -> Result<&'a Value> {
        row.get(self.pk_index)
    }
}

/// Compile a full schema into per-table `TableSchema`s.
pub fn compile_schema(schema: &Schema) -> Result<HashMap<TableRef, TableSchema>> {
    let mut out = HashMap::new();
    for (table, def) in &schema.tables {
        if def.columns.is_empty() {
            bail!("table '{table}' has no columns");
        }
        out.insert(table.clone(), TableSchema::from_def(table, def)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn users() -> TableSchema {
        let json = serde_json::json!({
            "columns": { "id": {"type":"int"}, "name": {"type":"text"}, "active": {"type":"bool"}, "score": {"type":"float"} },
            "primaryKey": "id"
        });
        let def: TableDef = serde_json::from_value(json).unwrap();
        TableSchema::from_def(&TableRef::parse("users").unwrap(), &def).unwrap()
    }

    #[test]
    fn sorted_columns_and_pk_index() {
        let ts = users();
        // sorted: active, id, name, score
        assert_eq!(ts.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(), ["active", "id", "name", "score"]);
        assert_eq!(ts.pk_index, 1);
        assert_eq!(ts.pk_type, ColumnType::Int);
    }

    #[test]
    fn row_json_roundtrip() {
        let ts = users();
        let obj = serde_json::json!({"id": 7, "name": "Alice", "active": true, "score": 9.5});
        let row = ts.row_from_json(obj.as_object().unwrap()).unwrap();
        assert_eq!(ts.pk_of(&row).unwrap(), &Value::Int(7));
        let back = ts.row_to_json(&row);
        assert_eq!(back, obj);
    }

    /// A library-mode schema (JSON, no Postgres) carries no fingerprint, so nothing drift-checks it —
    /// and therefore no digest to stamp or fence with (ADR-0010).
    #[test]
    fn library_mode_schema_has_no_fingerprint() {
        assert!(users().fingerprint.is_none());
        assert!(users().schema_digest.is_none());
    }

    fn col(name: &str, type_oid: u32, typmod: i32) -> FingerprintColumn {
        FingerprintColumn { name: name.into(), type_oid, typmod }
    }

    /// A catalog-read fingerprint: it knows its primary key.
    fn fp(columns: Vec<FingerprintColumn>) -> SchemaFingerprint {
        SchemaFingerprint { columns, replident: REPLICA_IDENTITY_FULL, pk: Some(vec!["id".into()]) }
    }

    /// A wire (`Relation`-derived) fingerprint: it does not.
    fn wire(columns: Vec<FingerprintColumn>) -> SchemaFingerprint {
        SchemaFingerprint { columns, replident: REPLICA_IDENTITY_FULL, pk: None }
    }

    fn base() -> SchemaFingerprint {
        fp(vec![col("id", 23, -1), col("name", 25, -1)])
    }

    #[test]
    fn identical_fingerprints_are_not_drift() {
        assert!(base().still_serves(&base()));
        assert!(describe_drift(&base(), &base()).is_empty());
    }

    #[test]
    fn an_added_column_is_drift() {
        let observed = fp(vec![col("id", 23, -1), col("name", 25, -1), col("extra", 25, -1)]);
        assert!(!base().still_serves(&observed));
        assert_eq!(describe_drift(&base(), &observed), ["column 'extra' added"]);
    }

    #[test]
    fn a_dropped_column_is_drift() {
        let observed = fp(vec![col("id", 23, -1)]);
        assert!(!base().still_serves(&observed));
        assert_eq!(describe_drift(&base(), &observed), ["column 'name' dropped"]);
    }

    #[test]
    fn a_retyped_column_is_drift() {
        // text -> varchar(10): both the oid and the typmod move.
        let observed = fp(vec![col("id", 23, -1), col("name", 1043, 14)]);
        assert!(!base().still_serves(&observed));
        assert_eq!(
            describe_drift(&base(), &observed),
            ["column 'name' retyped (oid 25/typmod -1 -> oid 1043/typmod 14)"]
        );
        // A typmod-only change (varchar(10) -> varchar(20)) is drift too.
        let a = fp(vec![col("name", 1043, 14)]);
        let b = fp(vec![col("name", 1043, 24)]);
        assert!(!a.still_serves(&b));
        assert_eq!(describe_drift(&a, &b).len(), 1);
    }

    /// Same names and types, different `attnum` order (drop + re-add): the tuple cells no longer
    /// line up with the names they are zipped against, so it must not compare equal.
    #[test]
    fn reordered_columns_are_drift() {
        let observed = fp(vec![col("name", 25, -1), col("id", 23, -1)]);
        assert!(!base().still_serves(&observed));
        assert_eq!(describe_drift(&base(), &observed), ["columns reordered (id,name -> name,id)"]);
    }

    /// Identical columns but a regressed replica identity is drift: the updates and deletes since
    /// the regression were mis-applied.
    /// A primary-key change is drift when both sides know their key (the reconciler's compare, and
    /// any re-introspection) — and is invisible to the `R` compare, which cannot know it.
    #[test]
    fn a_primary_key_change_is_drift_only_where_the_key_is_known() {
        let cols = base().columns;
        let repk = SchemaFingerprint {
            columns: cols.clone(),
            replident: REPLICA_IDENTITY_FULL,
            pk: Some(vec!["name".into()]),
        };
        assert!(!base().still_serves(&repk));
        assert_eq!(describe_drift(&base(), &repk), ["primary key changed (id -> name)"]);

        // The same columns off the wire carry no pk, so the `R` compare passes them.
        assert!(base().still_serves(&wire(cols)));
        assert!(describe_drift(&base(), &wire(base().columns)).is_empty());
    }

    #[test]
    fn a_replica_identity_regression_is_drift() {
        let observed = SchemaFingerprint { columns: base().columns, replident: b'd', pk: base().pk };
        assert!(!base().still_serves(&observed));
        assert_eq!(describe_drift(&base(), &observed), ["replica identity 'd' (expected 'f' = FULL)"]);
    }

    /// Why a publication that does not deliver whole rows is refused at BOOT rather than handled at
    /// runtime (`pg::inspect_publication`): its `Relation` message describes a narrower table than
    /// the catalog holds, on every message, and no re-introspection can ever reconcile the two.
    #[test]
    fn a_catalog_the_wire_does_not_deliver_never_compares_equal() {
        let catalog = fp(vec![col("id", 23, -1), col("name", 25, -1), col("secret", 25, -1)]);
        let wire = wire(vec![col("id", 23, -1), col("name", 25, -1)]);
        assert!(!catalog.still_serves(&wire));
        assert_eq!(describe_drift(&catalog, &wire), ["column 'secret' dropped"]);
        // Stable: re-reading the catalog changes nothing.
        assert!(!catalog.still_serves(&wire));
    }

    /// Composite key identity must be INJECTIVE. The two tuples below are distinct in Postgres and
    /// used to spell the same native key (`x\u{1f}y\u{1f}z`), so `translate_output`'s per-key
    /// de-duplication silently dropped one of the rows.
    #[test]
    fn a_composite_key_encodes_the_tuple_unambiguously() {
        let json = serde_json::json!({
            "columns": { "a": {"type":"text"}, "b": {"type":"text"}, "payload": {"type":"text"} },
            "primaryKey": ["a", "b"]
        });
        let def: TableDef = serde_json::from_value(json).unwrap();
        let ts = TableSchema::from_def(&TableRef::parse("items").unwrap(), &def).unwrap();
        // columns sort to (a, b, payload); pk_cols = [0, 1]
        let row = |a: &str, b: &str| Row(vec![Value::Text(a.into()), Value::Text(b.into()), Value::Text("p".into())]);
        let first = ts.key_string(&row("x", "y\u{1f}z")).unwrap();
        let second = ts.key_string(&row("x\u{1f}y", "z")).unwrap();
        assert_ne!(first, second, "distinct key tuples must not collide");
        assert_eq!(first, "x\u{1f}y\\x1fz");
        assert_eq!(second, "x\\x1fy\u{1f}z");

        // A backslash in a component is escaped too, so the escape itself cannot be forged: a
        // literal `\x1f` and a real separator stay distinct.
        let literal = ts.key_string(&row("x", "y\\x1fz")).unwrap();
        assert_eq!(literal, "x\u{1f}y\\\\x1fz");
        assert_ne!(literal, first);

        // Single-column keys are untouched — the value IS the key string.
        let single = serde_json::json!({
            "columns": { "id": {"type":"text"} }, "primaryKey": "id"
        });
        let sdef: TableDef = serde_json::from_value(single).unwrap();
        let sts = TableSchema::from_def(&TableRef::parse("one").unwrap(), &sdef).unwrap();
        assert_eq!(sts.key_string(&Row(vec![Value::Text("a\u{1f}b".into())])).unwrap(), "a\u{1f}b");
    }

    /// The replication decoder builds envelope keys from the parsed row object, not from a `Row`;
    /// both paths must spell the same key or a live update would never retract its backfilled row.
    #[test]
    fn the_replication_decoder_agrees_with_key_string() {
        assert_eq!(join_key_components(["x", "y\u{1f}z"]), "x\u{1f}y\\x1fz");
        assert_eq!(join_key_components(["x\u{1f}y", "z"]), "x\\x1fy\u{1f}z");
    }

    /// **The digest is a durable format.** It is stamped onto every change-log envelope, and an
    /// envelope whose stamp does not match the schema the sequencer holds is treated as pre-drift and
    /// SKIPPED (ADR-0010) — so changing the encoding (this value, the JSON form, or the struct's
    /// fields) makes every envelope already on the log unreadable-and-skipped. An upgrade that does
    /// that needs an epoch reset. Hence the pin.
    #[test]
    fn the_fingerprint_digest_is_pinned_to_an_exact_value() {
        assert_eq!(base().digest(), 0x204b_32de_951b_413d);
        assert_eq!(digest_hex(base().digest()), "204b32de951b413d");
        assert_eq!(digest_from_hex("204b32de951b413d"), Some(base().digest()));

        // Deterministic across constructions, and different for a fingerprint that differs in any
        // field the drift compare looks at.
        assert_eq!(base().digest(), base().digest());
        let added = fp(vec![col("id", 23, -1), col("name", 25, -1), col("extra", 25, -1)]);
        let retyped = fp(vec![col("id", 23, -1), col("name", 1043, 14)]);
        let reordered = fp(vec![col("name", 25, -1), col("id", 23, -1)]);
        let repk = SchemaFingerprint { pk: Some(vec!["name".into()]), ..base() };
        let reident = SchemaFingerprint { replident: b'd', ..base() };
        let mut digests = vec![base(), added, retyped, reordered, repk, reident]
            .iter()
            .map(SchemaFingerprint::digest)
            .collect::<Vec<_>>();
        digests.sort_unstable();
        let distinct = digests.len();
        digests.dedup();
        assert_eq!(digests.len(), distinct, "every fingerprint difference must move the digest");
    }

    /// A stamp that is not a digest at all reads as ABSENT, which decodes the envelope strictly.
    /// Reading it as "some other schema" would skip a change without a word (ADR-0010) — so a stamp
    /// that is merely MANGLED (truncated on the wire, signed, uppercased) must not parse either: each
    /// of those is a valid `from_str_radix` input naming a digest nothing has.
    #[test]
    fn an_unparseable_stamp_is_no_stamp() {
        assert_eq!(digest_from_hex(""), None);
        assert_eq!(digest_from_hex("not-a-digest"), None);
        assert_eq!(digest_from_hex("0x5b4fbe71a1f49a9f"), None);
        assert_eq!(digest_from_hex("204b32de951b41"), None, "truncated");
        assert_eq!(digest_from_hex("+204b32de951b413d"), None, "signed");
        assert_eq!(digest_from_hex("204B32DE951B413D"), None, "uppercase is not what digest_hex writes");
        assert_eq!(digest_from_hex(" 204b32de951b413d"), None, "padded");
        assert_eq!(digest_from_hex("0204b32de951b413d"), None, "17 characters");
        // …and the full u64 range round-trips, including the top bit (a 16-hex-char stamp).
        for d in [0u64, 1, u64::MAX, 0x8000_0000_0000_0000] {
            assert_eq!(digest_from_hex(&digest_hex(d)), Some(d));
            assert_eq!(digest_hex(d).len(), 16);
        }
    }

    /// The durable audit record spells the identity as Postgres does.
    #[test]
    fn fingerprint_json_round_trips_with_a_readable_replident() {
        let json = serde_json::to_value(base()).unwrap();
        assert_eq!(json["replident"], "f");
        assert_eq!(json["pk"], serde_json::json!(["id"]));
        assert_eq!(serde_json::from_value::<SchemaFingerprint>(json).unwrap(), base());
    }
}
