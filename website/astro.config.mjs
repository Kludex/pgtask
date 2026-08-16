import starlight from "@astrojs/starlight";
import { defineConfig } from "astro/config";

export default defineConfig({
  site: "https://kludex.github.io",
  base: "/pgtask",
  integrations: [
    starlight({
      title: "pgtask",
      description: "A durable task and workflow engine for PostgreSQL.",
      social: [
        { icon: "github", label: "GitHub", href: "https://github.com/Kludex/pgtask" },
      ],
      editLink: {
        baseUrl: "https://github.com/Kludex/pgtask/edit/main/website/",
      },
      customCss: ["./src/styles/custom.css"],
      sidebar: [
        {
          label: "Start here",
          items: [
            { label: "What pgtask is", link: "/start/what-pgtask-is/" },
            { label: "Install", link: "/start/install/" },
            { label: "Your first task", link: "/start/first-task/" },
          ],
        },
        {
          label: "Architecture",
          items: [
            { label: "The shape of the system", link: "/architecture/" },
            { label: "How a task runs", link: "/architecture/task-lifecycle/" },
            { label: "Durable execution", link: "/architecture/durability/" },
            { label: "Scheduling without a leader", link: "/architecture/scheduling/" },
            { label: "The storage boundary", link: "/architecture/storage/" },
            { label: "Scaling and deployment", link: "/architecture/scaling/" },
          ],
        },
        {
          label: "Concepts",
          items: [
            { label: "Queues", link: "/concepts/queues/" },
            { label: "Retries", link: "/concepts/retries/" },
            { label: "Idempotency", link: "/concepts/idempotency/" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "SQL protocol", link: "/reference/sql-protocol/" },
            { label: "Schema compatibility", link: "/reference/schema-compatibility/" },
          ],
        },
      ],
    }),
  ],
});
