# `config/` — Home directory bootstrap and validation

The `config` package owns **on-disk configuration** for a TurnstoneDB node: JSON schema, path resolution relative to `--home`, validation, and one-time artifact generation during `turnstone init`.

## `Config` struct

| Field | JSON key | Default (init) | Meaning |
| --- | --- | --- | --- |
| `ID` | `id` | hostname-based | Node identifier in replication handshakes |
| `Port` | `port` | `:6379` | mTLS listen address |
| `Debug` | `debug` | `false` | Verbose logging toggle |
| `MaxConns` | `max_conns` | `1000` | Server-wide connection semaphore |
| `TLSCertFile` | `tls_cert_file` | `certs/server.crt` | Server certificate |
| `TLSKeyFile` | `tls_key_file` | `certs/server.key` | Server private key |
| `TLSCAFile` | `tls_ca_file` | `certs/ca.crt` | CA for verifying clients and peers |
| `TLSClientCertFile` | `tls_client_cert_file` | `certs/server.crt` | Outbound replication client cert |
| `TLSClientKeyFile` | `tls_client_key_file` | `certs/server.key` | Outbound replication client key |
| `MetricsAddr` | `metrics_addr` | `:9090` | Prometheus HTTP listener |
| `NumberOfDatabases` | `number_of_databases` | `4` | Count of isolated keyspaces `0 … N-1` |
| `LogRetention` | `log_retention` | `replication` | `replication` or `none` |
| `MaxDiskUsagePercent` | `max_disk_usage_percent` | `90` | Write rejection threshold; `0` disables |

Paths in JSON are **relative to `--home`** unless absolute; use `ResolvePath(home, path)` consistently.

## Key functions

| Function | Purpose |
| --- | --- |
| `ResolvePath` | Join relative paths to home directory |
| `ValidateConfig` | Fail fast on invalid values (e.g. disk % outside 0–100) |
| `GenerateConfigArtifacts` | `init`: directories, RSA CA, server/client/admin certs, sample JSON |
| `LoadConfig` / `SaveConfig` | Read/write `turnstone.json` |

## PKI layout after `init`

```
<home>/
  turnstone.json
  certs/
    ca.crt, ca.key
    server.crt, server.key
    client.crt, client.key
    admin.crt, admin.key
  <node-id>/
    0/  1/  2/  ...    # per-database data directories
```

Certificates include SANs from `--ip` (comma-separated hostnames/IPs) so replication and CLI work across interfaces.

## RBAC model

Server-side authorization inspects the TLS peer certificate **Organization** field:

- `client` — data commands on databases in `PRIMARY` or read paths on replicas.
- `admin` — failover commands (`promote`, `stepdown`, `replicaof`, `flushdb`).
- `server` — replication streams between nodes.

The `config` package generates certs with these O= values; changing RBAC requires regenerating or reissuing certificates.

## Educational focus

### Why validate `MaxDiskUsagePercent`?

Invalid values previously **silently disabled** the disk monitor (negative) or the write guard (above 100). `ValidateConfig` turns misconfiguration into a startup error — a pattern worth copying for other numeric guardrails.

### `log_retention`

- `replication` — background retention respects replica acks and leader safe points.
- `none` — disables automatic WAL pruning policy at the database layer (use with care).

### Node ID vs database names

`ID` identifies the **server instance** on disk (`<home>/<id>/0/`). Logical database names in the protocol are `"0"`, `"1"`, etc.

## Review checklist

- [ ] New config fields have JSON tags, defaults in `initcmd.go`, and README table entries.
- [ ] `ValidateConfig` covers any field that could fail silently.
- [ ] `GenerateConfigArtifacts` is idempotent or clearly documents overwrite behavior.
- [ ] Cert generation uses adequate key size and validity window for your deployment policy.

## Suggestions

- Add config hot-reload only for safe fields (e.g. `debug`) — never for TLS paths without restart.
- Schema versioning field in JSON for forward-compatible migrations.
- `turnstone init --dry-run` to print planned paths without writing.
