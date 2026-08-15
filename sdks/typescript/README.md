# `@pgtask/client`

```typescript
import { Client, defineTask } from "@pgtask/client";

type Request = { reportId: string };
type Result = { rendered: string };

const render = defineTask<Request, Result>("reports.render", { queueName: "reports" });
const client = await Client.connect(process.env.PGTASK_DATABASE_URL!);
try {
  const task = await client.enqueue(render.request({ reportId: "report-123" }));
  const result = await task.result({ timeoutMs: 30_000 });
  console.log(result?.result?.rendered);
} finally {
  await client.close();
}
```

This is the typed TypeScript producer and result client for pgtask. See the
[TypeScript SDK documentation](https://github.com/Kludex/pgtask/blob/main/docs/typescript.md) for transactions,
OpenTelemetry propagation, signals, and cancellation.
