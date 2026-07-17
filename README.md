# Backend at Scale

Runnable reference code for a series of articles on building professional
backend systems that hold up under load. Each folder is a **self-contained,
`docker compose up`-able** example that backs one article — clone it, run it,
break it, understand it.

No shared build tooling, no monorepo framework. Each example pins its own
current-LTS stack so you can read one folder in isolation.

## Patterns

| Pattern | Folder | Status |
|---------|--------|--------|
| **Transactional Outbox** — atomic DB write + message publish; no lost events | [`outbox-pattern/`](./outbox-pattern) | ✅ |
| Idempotency keys — safely retryable writes | _idempotency-keys/_ | planned |
| Idempotent actions with Compare-And-Swap | _cas-actions/_ | planned |
| Race conditions & TOCTOU | _toctou/_ | planned |
| Database partitioning | _partitioning/_ | planned |
| Sharding | _sharding/_ | planned |
| Hot spots and how to cool them | _hot-spots/_ | planned |

## How each example is organized

```
<pattern>/
  README.md            the article's runnable companion
  docker-compose.yml   local infra, zero cloud credentials
  package.json         current-LTS stack, one folder = one project
  src/                 the smallest code that tells the story
```

Start with [`outbox-pattern/`](./outbox-pattern).
