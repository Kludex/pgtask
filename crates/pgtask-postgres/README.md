# pgtask-postgres

The PostgreSQL storage layer for `pgtask`: migrations, enqueue, claim, complete, schedules, and the views
you read from.

Every operation is a call to a versioned SQL function, so the database is the contract rather than this
crate. Clients in other languages call the same functions, and both the schema and each client declare a
storage protocol range that has to overlap before a worker will start.

Depend on it directly when you produce tasks but never run a worker. Otherwise take
[`pgtask`](https://crates.io/crates/pgtask), which re-exports this as `pgtask::postgres`.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
