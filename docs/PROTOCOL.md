# Protocol Reference

Kestrel speaks a subset of **RESP2** (REdis Serialization Protocol, v2) over TCP, which is what makes it interoperable with `redis-cli` and standard Redis client libraries for testing. This doc specifies exactly what's implemented, so scope stays explicit as phases land.

## Wire Format (RESP2 primitives)

| Type | Prefix | Example |
|---|---|---|
| Simple String | `+` | `+OK\r\n` |
| Error | `-` | `-ERR unknown command\r\n` |
| Integer | `:` | `:1000\r\n` |
| Bulk String | `$` | `$5\r\nhello\r\n` |
| Null Bulk String | `$-1` | `$-1\r\n` |
| Array | `*` | `*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n` |

Client requests are always sent as an Array of Bulk Strings (this is how real Redis clients encode commands, e.g. `SET foo bar` → `*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n`).

## Command Reference (v1 scope — Phase 1)

### Strings
| Command | Args | Returns | Notes |
|---|---|---|---|
| `SET` | `key value` | `+OK` | overwrites existing key |
| `GET` | `key` | bulk string or `$-1` | |
| `DEL` | `key [key ...]` | `:N` (count deleted) | |
| `EXPIRE` | `key seconds` | `:1` / `:0` | sets TTL |
| `TTL` | `key` | `:seconds` / `:-1` no TTL / `:-2` no key | |

### Hashes
| Command | Args | Returns |
|---|---|---|
| `HSET` | `key field value` | `:1` (new) / `:0` (updated) |
| `HGET` | `key field` | bulk string or `$-1` |
| `HDEL` | `key field` | `:1` / `:0` |

### Lists
| Command | Args | Returns |
|---|---|---|
| `LPUSH` / `RPUSH` | `key value` | `:len` after push |
| `LPOP` / `RPOP` | `key` | bulk string or `$-1` |

### Sorted Sets
| Command | Args | Returns |
|---|---|---|
| `ZADD` | `key score member` | `:1` / `:0` |
| `ZRANGE` | `key start stop` | array of members |
| `ZSCORE` | `key member` | bulk string score or `$-1` |

### Connection
| Command | Args | Returns |
|---|---|---|
| `PING` | — | `+PONG` |

**Explicitly out of scope for v1:** `EXPIREAT`, `MULTI/EXEC` transactions, pub/sub (`SUBSCRIBE`), Lua (`EVAL`), `SCAN`. See [`DESIGN.md §4`](DESIGN.md#4-non-goals).

## Replication Protocol (Phase 3+)

Internal, not client-facing. A follower opens a dedicated TCP connection to the leader and receives a stream of already-committed write commands in AOF order, each tagged with a monotonically increasing **replication offset**. Followers apply entries strictly in order and expose their current offset so lag can be queried and benchmarked. Full RPC framing to be finalized during Phase 3 implementation and documented here once stable.

## Raft RPCs (Phase 4+)

Standard Raft RPCs (`RequestVote`, `AppendEntries`) via `hashicorp/raft`'s transport layer. This section will be filled in with the concrete network transport choice (raw TCP vs. gRPC) and any Kestrel-specific state-machine `Apply()` semantics once Phase 4 begins — see [`DESIGN.md §6 Phase 4`](DESIGN.md#phase-4--consensus--failover-raft).
