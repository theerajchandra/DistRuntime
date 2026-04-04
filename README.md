# DistRuntime

Distributed training runtime.

## Local Dev

### Prerequisites

Install the three local services via Homebrew:

```bash
brew install etcd minio/stable/minio postgresql@18
brew postinstall postgresql@18
```

Add PostgreSQL to your PATH (add to your `~/.zshrc`):

```bash
export PATH="/opt/homebrew/opt/postgresql@18/bin:$PATH"
```

### Starting Services

**etcd**

```bash
brew services start etcd
```

Or run in the foreground:

```bash
etcd
```

Verify: `etcdctl endpoint health`

**MinIO**

```bash
minio server ~/.minio-data --console-address ":9001"
```

- API: http://localhost:9000
- Console: http://localhost:9001
- Default credentials: `minioadmin` / `minioadmin`

**PostgreSQL**

```bash
brew services start postgresql@18
```

Or run in the foreground:

```bash
LC_ALL="en_US.UTF-8" /opt/homebrew/opt/postgresql@18/bin/postgres -D /opt/homebrew/var/postgresql@18
```

Verify: `pg_isready`

### Stopping Services

```bash
brew services stop etcd
brew services stop postgresql@18
```

MinIO: `Ctrl-C` if running in the foreground.

## Build

```bash
make build    # cargo build --workspace
make test     # cargo test --workspace
make lint     # cargo clippy with -D warnings
make fmt      # cargo fmt --all
make check    # fmt check + clippy + tests
```
