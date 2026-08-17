# pgtask-cli

The `pgtask` command: migrate the schema, configure queues and roles, and act on work that is stuck.

```console
cargo install pgtask-cli
```

The database URL comes from `PGTASK_DATABASE_URL`, so every command below takes no connection flags:

```console
pgtask migrate
pgtask health
pgtask queue put reports --max-outstanding-tasks 10000
pgtask cancel 0198f0b8-6f5f-7a6b-9c3d-2f5a1c9e4d10
```

Run `pgtask migrate` before deploying workers, and only from one place at a time if you like. It takes an
advisory lock, so ten concurrent runs apply the schema once and the rest wait.

`pgtask configure-grants` creates the owner, producer, worker, observer, and administrator roles. Each one
reaches the database only through functions and security-barrier views, so a producer cannot claim a task
or read another queue's payloads.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
