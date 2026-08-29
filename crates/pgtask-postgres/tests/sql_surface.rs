//! Records the SQL protocol surface so schema changes cannot pass silently.
//!
//! Set `PGTASK_UPDATE_SQL_SURFACE=1` to rewrite `tests/sql_surface.txt` after an intended change,
//! then review the diff as part of the change.

use std::{collections::BTreeMap, fmt::Write as _, str::FromStr};

use pgtask_core::STORAGE_PROTOCOL_MIN_VERSION;
use pgtask_postgres::Store;
use sqlx::{PgPool, Row, postgres::PgConnectOptions};
use uuid::Uuid;

const RECORDED: &str = include_str!("sql_surface.txt");
/// The surface as of the oldest storage protocol this release still supports. Everything in it must
/// still exist, unchanged, or a worker built for that protocol breaks.
const BASELINE: &str = include_str!("sql_surface_baseline.txt");
const OWNER: &str = "{owner}";

const ROLES: [&str; 5] = [
    "pgtask_surface_owner",
    "pgtask_surface_producer",
    "pgtask_surface_worker",
    "pgtask_surface_observer",
    "pgtask_surface_administrator",
];

fn database_url() -> Option<String> {
    std::env::var("PGTASK_DATABASE_URL").ok()
}

/// Rewrites the object owner so the record does not depend on the connecting role.
fn normalize(entries: Vec<String>, owner: &str) -> String {
    if entries.is_empty() {
        return "(none)".to_string();
    }
    let mut rendered: Vec<String> = entries
        .into_iter()
        .map(|entry| {
            let (grantee, rest) = entry.split_once('=').unwrap_or(("", &entry));
            let (privileges, grantor) = rest.rsplit_once('/').unwrap_or((rest, ""));
            let grantee = if grantee == owner { OWNER } else { grantee };
            let grantor = if grantor == owner { OWNER } else { grantor };
            format!("{grantee}={privileges}/{grantor}")
        })
        .collect();
    rendered.sort();
    rendered.join(", ")
}

async fn record_surface(store: &Store, owner: &str) -> String {
    let mut surface = String::new();
    let mut connection = store.pool().acquire().await.unwrap();
    // Postgres qualifies type names against the search path, which otherwise resolves the pgtask
    // schema whenever the connecting role happens to be named after it.
    sqlx::query("SET search_path = pg_catalog")
        .execute(&mut *connection)
        .await
        .unwrap();

    let schema = sqlx::query("SELECT coalesce(nspacl::text[], '{}') FROM pg_namespace WHERE nspname = 'pgtask'")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    writeln!(surface, "schema pgtask").unwrap();
    writeln!(surface, "  grants: {}", normalize(schema.get(0), owner)).unwrap();

    let functions = sqlx::query(
        r"
        SELECT
            p.proname,
            pg_get_function_identity_arguments(p.oid) AS arguments,
            pg_get_function_result(p.oid) AS result,
            CASE p.provolatile WHEN 'i' THEN 'IMMUTABLE' WHEN 's' THEN 'STABLE' ELSE 'VOLATILE' END AS volatility,
            p.prosecdef,
            coalesce(array_to_string(p.proconfig, ' '), '') AS settings,
            coalesce(p.proacl::text[], '{}') AS acl
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'pgtask'
        ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)
        ",
    )
    .fetch_all(&mut *connection)
    .await
    .unwrap();
    for function in functions {
        let name: String = function.get("proname");
        let arguments: String = function.get("arguments");
        let result: String = function.get("result");
        let volatility: String = function.get("volatility");
        let settings: String = function.get("settings");
        let security = if function.get::<bool, _>("prosecdef") {
            " SECURITY DEFINER"
        } else {
            ""
        };
        let settings = if settings.is_empty() {
            String::new()
        } else {
            format!(" SET {settings}")
        };
        writeln!(
            surface,
            "\nfunction {name}({arguments}) -> {result} {volatility}{security}{settings}"
        )
        .unwrap();
        writeln!(surface, "  grants: {}", normalize(function.get("acl"), owner)).unwrap();
    }

    let relations = sqlx::query(
        r"
        SELECT
            c.relkind::text AS kind,
            c.relname,
            coalesce(c.relacl::text[], '{}') AS acl,
            coalesce((
                SELECT string_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod), ', ' ORDER BY a.attnum)
                FROM pg_attribute a
                WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
            ), '') AS columns
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'pgtask' AND c.relkind IN ('r', 'v')
        ORDER BY c.relkind, c.relname
        ",
    )
    .fetch_all(&mut *connection)
    .await
    .unwrap();
    for relation in relations {
        let kind: String = relation.get("kind");
        let name: String = relation.get("relname");
        let columns: String = relation.get("columns");
        if kind == "v" {
            writeln!(surface, "\nview {name}({columns})").unwrap();
        } else {
            writeln!(surface, "\ntable {name}({columns})").unwrap();
        }
        writeln!(surface, "  grants: {}", normalize(relation.get("acl"), owner)).unwrap();
    }

    surface
}

#[tokio::test]
async fn sql_surface_matches_the_recorded_contract() {
    let Some(database_url) = database_url() else {
        return;
    };
    let database_name = format!("pgtask_surface_{}", Uuid::new_v4().simple());
    let options = PgConnectOptions::from_str(&database_url).unwrap();
    let maintenance = PgPool::connect_with(options.clone().database("postgres"))
        .await
        .unwrap();
    for role in ROLES {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DO $$ BEGIN \
             IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN CREATE ROLE {role}; END IF; \
             END $$"
        )))
        .execute(&maintenance)
        .await
        .unwrap();
    }
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE {database_name}")))
        .execute(&maintenance)
        .await
        .unwrap();

    let store = Store::from_pool(PgPool::connect_with(options.database(&database_name)).await.unwrap());
    store.migrate().await.unwrap();
    let owner: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(store.pool())
        .await
        .unwrap();
    store
        .configure_grants(&owner, ROLES[1], ROLES[2], ROLES[3], ROLES[4])
        .await
        .unwrap();
    let surface = record_surface(&store, &owner).await;

    drop(store);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP DATABASE {database_name} WITH (FORCE)"
    )))
    .execute(&maintenance)
    .await
    .unwrap();

    if std::env::var("PGTASK_UPDATE_SQL_SURFACE").is_ok() {
        std::fs::write(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sql_surface.txt"), &surface).unwrap();
        return;
    }
    assert_backward_compatible(&surface);

    if surface != RECORDED {
        let recorded = blocks(RECORDED);
        let current = blocks(&surface);
        let mut report = String::from("the SQL surface changed\n");
        for (object, detail) in &recorded {
            match current.get(object) {
                None => writeln!(report, "  removed {object}").unwrap(),
                Some(current) if current != detail => {
                    writeln!(
                        report,
                        "  changed {object}\n    recorded: {detail}\n    current:  {current}"
                    )
                    .unwrap();
                }
                Some(_) => {}
            }
        }
        for object in current.keys().filter(|object| !recorded.contains_key(*object)) {
            writeln!(report, "  added {object}").unwrap();
        }
        report.push_str("\nrerun with PGTASK_UPDATE_SQL_SURFACE=1 when the change is intended");
        panic!("{report}");
    }
}

/// Splits the record into one entry per object so a changed grant names the object it belongs to.
fn blocks(surface: &str) -> BTreeMap<String, String> {
    surface
        .split("\n\n")
        .filter_map(|block| {
            let block = block.trim();
            let (object, detail) = block.split_once('\n')?;
            Some((object.trim().to_string(), detail.trim().to_string()))
        })
        .collect()
}

/// Fails when the schema stopped offering something the baseline protocol depends on.
///
/// Additions are fine, because a worker built for the older protocol simply does not call them.
/// Removals and changes are not: the same worker still calls what it always called. Semantic changes
/// behind an unchanged signature cannot be caught here and are what a protocol bump is for.
fn assert_backward_compatible(surface: &str) {
    let (declared, baseline) = BASELINE
        .split_once("\n\n")
        .expect("the baseline starts with a protocol header");
    let declared: u32 = declared
        .rsplit(':')
        .next()
        .expect("the header ends with a version")
        .trim()
        .parse()
        .expect("the header records a protocol number");

    if declared != STORAGE_PROTOCOL_MIN_VERSION {
        assert!(
            std::env::var("PGTASK_UPDATE_SQL_BASELINE").is_ok(),
            "the baseline records protocol {declared} and the crate now supports {STORAGE_PROTOCOL_MIN_VERSION} \
             as its minimum. Dropping support is a contract step: rerun with \
             PGTASK_UPDATE_SQL_BASELINE=1 to rebase the baseline on the new minimum."
        );
        std::fs::write(
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/sql_surface_baseline.txt"),
            format!("# Storage protocol minimum: {STORAGE_PROTOCOL_MIN_VERSION}\n\n{surface}"),
        )
        .unwrap();
        return;
    }

    let current = blocks(surface);
    let mut broken = String::new();
    for (object, detail) in blocks(baseline) {
        match current.get(&object) {
            None => writeln!(broken, "  removed {object}").unwrap(),
            Some(now) if *now != detail => {
                writeln!(
                    broken,
                    "  changed {object}\n    baseline: {detail}\n    now:      {now}"
                )
                .unwrap();
            }
            Some(_) => {}
        }
    }
    assert!(
        broken.is_empty(),
        "the schema is no longer backward compatible with storage protocol {STORAGE_PROTOCOL_MIN_VERSION}\n\
         {broken}\n\
         A worker built for that protocol still calls these. Keep them, or raise \
         STORAGE_PROTOCOL_MIN_VERSION to drop support deliberately."
    );
}
