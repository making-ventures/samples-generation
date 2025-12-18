# samples-generation

Generate sample data for multiple databases with a unified interface.

## Supported Databases

- **PostgreSQL** - via `postgres` package
- **ClickHouse** - via `@clickhouse/client`
- **SQLite** - via `better-sqlite3`
- **Trino/Iceberg** - via `trino-client`

## Installation

```bash
pnpm install
```

## Quick Start

### Using the Generator API

```typescript
import {
  PostgresDataGenerator,
  ClickHouseDataGenerator,
  SQLiteDataGenerator,
  TrinoDataGenerator,
  type TableConfig,
} from "./src/generator/index.js";

const table: TableConfig = {
  name: "users",
  columns: [
    { name: "id", type: "integer", generator: { kind: "sequence", start: 1 } },
    {
      name: "name",
      type: "string",
      generator: { kind: "randomString", length: 10 },
    },
    {
      name: "score",
      type: "float",
      generator: { kind: "randomFloat", min: 0, max: 100 },
    },
    {
      name: "status",
      type: "string",
      generator: { kind: "choice", values: ["active", "inactive"] },
    },
    { name: "created_at", type: "datetime", generator: { kind: "datetime" } },
  ],
};

// All generators have the same interface
const generator = new PostgresDataGenerator({
  host: "localhost",
  port: 5432,
  database: "appdb",
  username: "postgres",
  password: "postgres",
});

await generator.connect();
const result = await generator.generate({
  table,
  rowCount: 1000,
  truncateFirst: true,
  resumeSequences: true, // Continue sequence from last max value
});
console.log(`Inserted ${result.rowsInserted} rows in ${result.durationMs}ms`);
await generator.disconnect();
```

### Database Configurations

All databases use a consistent `host`/`port` configuration:

```typescript
// PostgreSQL
new PostgresDataGenerator({
  host: "localhost",
  port: 5432,
  database: "appdb",
  username: "postgres",
  password: "postgres",
});

// ClickHouse
new ClickHouseDataGenerator({
  host: "localhost",
  port: 8123,
  database: "default",
  username: "default",
  password: "clickhouse",
});

// SQLite
new SQLiteDataGenerator({
  path: "data/samples.db",
});

// Trino/Iceberg
new TrinoDataGenerator({
  host: "localhost",
  port: 8080,
  catalog: "iceberg",
  schema: "warehouse",
  user: "trino",
});
```

### Column Types

| Type       | PostgreSQL   | ClickHouse | SQLite  | Trino     |
| ---------- | ------------ | ---------- | ------- | --------- |
| `integer`  | INTEGER      | Int32      | INTEGER | INTEGER   |
| `bigint`   | BIGINT       | Int64      | INTEGER | BIGINT    |
| `float`    | NUMERIC      | Float64    | REAL    | DOUBLE    |
| `string`   | VARCHAR(255) | String     | TEXT    | VARCHAR   |
| `boolean`  | BOOLEAN      | Bool       | INTEGER | BOOLEAN   |
| `datetime` | TIMESTAMP    | DateTime   | TEXT    | TIMESTAMP |
| `date`     | DATE         | Date       | TEXT    | DATE      |

### Column Options

Each column can have additional options:

| Option     | Type      | Default | Description                                      |
| ---------- | --------- | ------- | ------------------------------------------------ |
| `nullable` | `boolean` | `false` | If `true`, omits `NOT NULL` constraint on column |

### Value Generators

| Generator      | Kind           | Options                   |
| -------------- | -------------- | ------------------------- |
| `sequence`     | Auto-increment | `start`, `step`           |
| `randomInt`    | Random integer | `min`, `max`              |
| `randomFloat`  | Random decimal | `min`, `max`, `precision` |
| `randomString` | Random string  | `length`                  |
| `choice`       | Pick from list | `values`                  |
| `constant`     | Fixed value    | `value`                   |
| `datetime`     | Random date    | `from`, `to`              |
| `uuid`         | UUID v4        | -                         |

### Generate Options

```typescript
interface GenerateOptions {
  table: TableConfig;
  rowCount: number;
  createTable?: boolean; // Default: true
  dropFirst?: boolean; // Default: false - drop table before generating
  truncateFirst?: boolean; // Default: false
  resumeSequences?: boolean; // Default: true - continue from max value
}
```

### Escape Utilities

For custom queries, use the exported escape functions:

```typescript
import {
  escapePostgresIdentifier,
  escapeClickHouseIdentifier,
  escapeTrinoIdentifier,
} from "./src/generator/index.js";

escapePostgresIdentifier("my-table"); // "my-table"
escapePostgresIdentifier('table"name'); // "table""name"

escapeClickHouseIdentifier("my-table"); // `my-table`
escapeClickHouseIdentifier("table`name"); // `table``name`

escapeTrinoIdentifier("samples"); // "samples"
escapeTrinoIdentifier("samples$files"); // "samples$files" (for metadata tables)
```

### Table Size

Get the size of a table (including indexes):

```typescript
// Get size in bytes
const bytes = await generator.getTableSize("users");
// 1234567

// Get human-readable size
const size = await generator.getTableSizeForHuman("users");
// "1.18 MB"
```

You can also use the `formatBytes` utility directly:

```typescript
import { formatBytes } from "./src/generator/index.js";

formatBytes(1024); // "1.00 KB"
formatBytes(1048576); // "1.00 MB"
```

## Scripts

### Generate Data

```bash
# Generate 1000 rows in all databases (requires docker-compose up)
npx tsx scripts/generate-all.ts

# Generate for specific databases only
GENERATE_SQLITE=1 npx tsx scripts/generate-all.ts
GENERATE_POSTGRES=1 npx tsx scripts/generate-all.ts
GENERATE_CLICKHOUSE=1 npx tsx scripts/generate-all.ts
GENERATE_TRINO=1 npx tsx scripts/generate-all.ts
```

## Docker Compose

Start all databases:

```bash
pnpm compose:up
```

Services available:

| Service    | Port(s)                    | Credentials           |
| ---------- | -------------------------- | --------------------- |
| PostgreSQL | 5432                       | postgres:postgres     |
| ClickHouse | 8123 (HTTP), 9009 (native) | default:clickhouse    |
| Trino      | 8080                       | trino (no password)   |
| MinIO      | 9000 (S3), 9001 (console)  | minioadmin:minioadmin |
| Nessie     | 19120                      | -                     |

## Testing

```bash
# Run tests (SQLite only by default)
pnpm test

# Run tests with specific databases
TEST_POSTGRES=1 pnpm test
TEST_CLICKHOUSE=1 pnpm test
TEST_TRINO=1 pnpm test

# Run all database tests
TEST_POSTGRES=1 TEST_CLICKHOUSE=1 TEST_TRINO=1 pnpm test
```

## Quality Checks

```bash
# Run formatting, linting, type checking, and tests
./check.sh

# Check for security vulnerabilities and outdated dependencies
./health.sh

# Run all checks
./all-checks.sh
```
