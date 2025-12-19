# Turnstone Database

Turnstone is a KV store and supports transactions (Snapshot Isolation with dependency tracking), change tracking support (CDC). It uses append-only log structure to store and fetch values from disk and optimized for heavy writes. Append-only structures needs a way to clean up, On demand compaction helps to keep things in check. Turnstone uses binary protocol to communicate with clients.

Turnstone supports following operations.

1. `GET <key>` to fetch key.
2. `SET <key> <value>` to set/update value.
3. `DEL <key>` to delete the key.

Above operations should be called inside transactions. More than one operations within transaction is allowed.

1. `BEGIN` to start a transaction.
2. `COMMIT` to commit a running transaction.
3. `ABORT` to abort a running transaction.

There are non-transactional commands as follows

1. `AUTH <code>` to authenticate the current connection.
2. `PING` simple health check, return PONG
3. `STAT` returns server stats.
4. `QUIT` terminate current client connection.
5. `COMPACT` starts compaction on server.

## How does it work?

Turnstone database follows single file and append-only design to deliver its features.

File Format:

`......File Header.....|....................Data.......................
<checksum><generation><checksum><key_length><value_length><key><value>`


