# askc-ts

TypeScript client for AskService.

## Proto source
Proto definitions are sourced from `askaway-proto` (git submodule).

## Codegen

Run this first before development:

```bash
npm run codegen:proto
```

Then you can regenerate all generated files when needed:

```bash
npm run codegen
```

## Run demo (recommended)

```bash
npm run dev
```

The demo requires a reachable AskService actor and a valid `Actr.toml` in this directory
or an `ACTR_CONFIG` environment variable pointing to your config file.
