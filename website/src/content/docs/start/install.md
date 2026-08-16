---
title: Install
description: Install the Python package and prepare a database.
---

You need PostgreSQL 17 or newer and a way to run a worker process.

## Install the package

```console
uv sync --project sdks/python --group dev
```

## Point it at a database

Every tool reads the connection string from `PGTASK_DATABASE_URL`.

```console
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:5432/pgtask
```

## Create the schema

The schema does not exist until you migrate. Connect first, then migrate:

```python
from pgtask import Client

client = await Client.connect(database_url)
await client.migrate()
```

`migrate()` takes an advisory lock, so it is safe to call from every process at startup. Running it concurrently from
ten workers applies the schema once.

:::note[Connecting before the schema exists is fine]
`connect()` succeeds against an empty database. It only checks the storage protocol once the schema is there, which is
what lets you bootstrap with `connect()` followed by `migrate()`.
:::

Everything lives in a `pgtask` schema. Nothing is installed into `public`, and no PostgreSQL extension is required.

## Set up a development environment

The repository ships a Tilt environment that installs the Helm chart with a disposable PostgreSQL.

Your cluster must advertise a local registry. Without one, Tilt pushes the development image to Docker Hub and the build
fails with `denied: requested access to the resource is denied`.

```console
k3d cluster create pgtask --registry-create pgtask-registry:0.0.0.0:5111
tilt up
export PGTASK_DATABASE_URL=postgresql://pgtask:pgtask@localhost:54329/pgtask
```

Tilt forwards PostgreSQL to port `54329`. Run `tilt down` to remove the release.

## Next

Write a handler and run it in [Your first task](/pgtask/start/first-task/).
