# pgtask-web

A read-only web interface for a `pgtask` database: queues, tasks, attempts, checkpoints, signals,
schedules, occurrences, workers, and handler capabilities.

```console
cargo install pgtask-web
PGTASK_DATABASE_URL=postgresql://pgtask_observer:secret@localhost/pgtask \
    PGTASK_WEB_ADDRESS=127.0.0.1:8080 \
    pgtask-web
```

Give it the observer role. The service reads security-barrier observer views and never touches the tables
or the mutation functions, so pointing it at a production database cannot change anything in it.

Set `PGTASK_WEB_ADMINISTRATOR=true` to add cancel, retry, and schedule pause actions. Then name the
identity header your proxy injects with `PGTASK_WEB_ADMINISTRATOR_ACTOR_HEADER`, and make sure that proxy
strips client-supplied copies of it. Every mutation is recorded in `pgtask.administrator_audit` with its
actor.

## Documentation

[kludex.github.io/pgtask](https://kludex.github.io/pgtask/)
