# Turnstone Database

Turnstone is a KV store and supports transactions (Snapshot Isolation with dependency tracking), change tracking support (CDC). It uses append-only log structure to store and fetch values from disk and optimized for heavy writes. Append-only structures needs a way to clean up, On demand compaction helps to keep things in check. Turnstone uses binary protocol to communicate with clients.

Turnstone supports following operations.

1. `GET <key>` to fetch key.
2. `SET <key> <value>` to set/update value.
3. `DEL <key>` to delete the key.

Above operations should be called inside transactions. More than one operations within transaction is allowed.

`BEGIN` to start a transaction.
`COMMIT` to commit a running transaction.
`ABORT` to abort a running transaction.

There are non-transactional commands as follows

1. `PING` simple health check, return PONG
2. `STAT` returns server stats.
3. `QUIT` terminate current client connection.
4. `COMPACT` starts compaction on server.

## How does it work?

Turnstone database follows single file and append-only design to deliver its features.

File Format:

......File Header.....|....................Data.......................<br>
`<checksum><generation><checksum><key_length><value_length><key><value>`


