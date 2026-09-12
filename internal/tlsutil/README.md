# `internal/tlsutil/` — mTLS credential loading

Small helper package that loads **client-side TLS configurations** from a standard Turnstone home directory layout. Used by `cmd/turnstone` (CLI, bench) and `client` package.

## API

| Function / type | Purpose |
| --- | --- |
| `Role` | `RoleClient` or `RoleAdmin` |
| `CertPaths(home, role)` | Returns `(ca.crt, <role>.crt, <role>.key)` under `<home>/certs/` |
| `LoadMTLS(ca, cert, key)` | Build `tls.Config` with client cert + root CA pool |
| `LoadFromHome(home, role)` | Convenience wrapper |

## Expected cert layout

```
<home>/certs/
  ca.crt
  client.crt, client.key    # Organization: client
  admin.crt, admin.key      # Organization: admin
```

Server certs are loaded separately in `cmd/turnstone/server.go` from `turnstone.json` paths.

## TLS config behavior

`LoadMTLS` sets:

- `RootCAs` — trust anchor for verifying server cert
- `Certificates` — client cert presented during handshake

It does **not** set `ServerName` (SNI) — callers connecting by IP should ensure server certs include SANs from `turnstone init --ip`.

## Educational focus

### Client vs admin roles

Only the certificate **Organization** differs at the TLS layer; the server maps O= to authorization roles. Switching roles is `LoadFromHome(home, RoleAdmin)` — no separate protocol login.

### Error messages

Errors wrap `os.ReadFile` / `tls.LoadX509KeyPair` failures with paths — important for operator debugging when `init` was run with a different `--home`.

## Review checklist

- [ ] New roles get constants, cert generation in `config`, and server RBAC mapping.
- [ ] Minimum TLS version policy applied at server, not here (client inherits Go defaults).
- [ ] No private keys logged or echoed.

## Suggestions

- Optional `InsecureSkipVerify` only in test builds behind explicit flag.
- Helper to reload certs on file change for long-lived CLI sessions.
