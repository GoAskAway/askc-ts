# askc-ts

TypeScript client for AskService.

## Proto source
Proto definitions are sourced from `askaway-proto` (git submodule).

## Codegen

```bash
npm run codegen
```

## Run demo (recommended)

```bash
npm run dev
```

The demo requires a reachable AskService actor and a valid `Actr.toml` in this directory
or an `ACTR_CONFIG` environment variable pointing to your config file.
