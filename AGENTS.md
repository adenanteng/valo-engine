# AGENTS.md

## What this is

Standalone Go microservice ("Valo Engine"): WhatsApp bot/messaging API built on Fiber v3, GORM + PostgreSQL, and whatsmeow (WhatsApp client). Not a monorepo — single module `valo-engine`.

## Commands

```bash
go run .          # start server (port 8083)
go build ./...    # compile check
go vet ./...
go test ./...     # service-level tests (normalizer, send pacer)
```

- `go build ./... && go vet ./... && go test ./...` is the verification step.
- Requires a reachable PostgreSQL and a `.env` (copy `.env.example`). The app fatals at startup without a DB connection.
- Startup runs GORM `AutoMigrate` and `CREATE EXTENSION "uuid-ossp"` — the DB user needs permission to create that extension (pre-install it otherwise).

## Structure

- `main.go` — entrypoint; Swagger metadata annotations live here.
- `config/` — env loading (godotenv) + DB init.
- `routes/routes.go` — all route registration. Routes are dual-mounted at both `/valo` and `/api/valo`; keep both in sync when editing.
- `internal/controllers` — HTTP handlers (admin vs public).
- `internal/middlewares` — `AdminAuthMiddleware` (`X-Valo-Admin-Key` header, `VALO_ADMIN_KEY` env) guards accounts/apikeys routes; `ApiKeyMiddleware` (`X-Valo-Key` header) guards `/valo/messages/send`.
- `internal/services/valo_service.go` — WhatsApp client lifecycle. whatsmeow session/device store lives in the same Postgres DB (`sqlstore`); it panics on init failure.
  - Account status enum: `CONNECTED`, `DISCONNECTED` (transient, auto-reconnect), `LOGGED_OUT` (dead session, must re-pair), `TEMP_BANNED` (wait out the expiry). Status is written from whatsmeow events and reconciled with live client state in `SyncStatuses` (called by `ListAccounts`).
  - whatsmeow `PermanentDisconnect` events (`LoggedOut`, `TemporaryBan`, `StreamReplaced`, `ClientOutdated`, `ConnectFailure`) must stay handled — dropping one reintroduces the stale-CONNECTED bug.
  - Public API sends are paced per account (`sendMinGap`/`sendJitter`, 4–10s) to avoid spam flags; conversational Aria replies use `sendNow` to bypass pacing. There is deliberately no any-client fallback in `resolveClient`.
- `docs/` — generated Swagger output (swag CLI). Do not hand-edit; regenerate with `swag init` after changing annotations, and it's imported side-effect (`_ "valo-engine/docs"`) by main.go.

## Gotchas

- Go 1.25 required (go.mod); local toolchain may be newer.
- WhatsApp connections can route through a SOCKS5 proxy: per-account `valo_accounts.proxy_url` overrides the global `WHATSAPP_PROXY_URL` (empty = direct). Don't route all accounts through one shared datacenter IP — that's a spam flag.
- Outbound chatbot traffic integrates with the Grupia API (`GRUPIA_API_URL`, `GRUPIA_WEBHOOK_KEY`).
- `.env`, keys, and certs are gitignored — never commit credentials.
