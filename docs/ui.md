# Web interface

## Run the observer service

```console
PGTASK_DATABASE_URL=postgresql://pgtask_observer:secret@localhost/pgtask \
    PGTASK_WEB_ADDRESS=127.0.0.1:8080 \
    pgtask-web
```

Open `http://127.0.0.1:8080`. The service shows queues, tasks, attempts, checkpoints, signals, schedules, occurrences, workers, and handler capabilities. Task search accepts a task name or exact task ID.

Use the observer database role created through `pgtask.configure_grants`. The service queries only security-barrier observer views. It does not need access to the underlying tables or mutation functions.

The UI is read-only by default. Set `PGTASK_WEB_ADMINISTRATOR=true` to add POST actions for task cancellation, task retry, and schedule pause or resume. Configure `PGTASK_WEB_ADMINISTRATOR_ACTOR_HEADER` with the identity header injected by your authenticating proxy. The proxy must remove client-provided copies of that header. Every successful mutation and actor is stored in `pgtask.administrator_audit`.
