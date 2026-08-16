# Website

The documentation site, built with [Astro](https://astro.build) and [Starlight](https://starlight.astro.build).

```console
npm install
npm run dev
```

`npm run build` writes a static site to `dist/`. The site is served under the `/pgtask` base path.

Architecture diagrams are inline SVG in the page that uses them. They take their colours from the tokens in
`src/styles/custom.css`, so they follow the reader's light or dark theme. There is no diagram build step and no client
JavaScript.
