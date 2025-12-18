# samples-generation

Generate sample data for multiple databases with a unified interface.

## Objective

We often need to prefill tables during tests, checks, and measurements. These generators support filling tables with random data and applying transformations (template-based construction of values from other columns, lookups in another table, data corruptions, etc). May be used for simple prefill and for controlled corruptions for further testing entity resolution.

## Supported Databases

- **PostgreSQL** - via `postgres` package
- **ClickHouse** - via `@clickhouse/client`
- **SQLite** - via `better-sqlite3`
- **Trino/Iceberg** - via `trino-client`

## Installation

```bash
pnpm install
```

## Measurements of simple generations

Environment: local databases, simple setup, 1 billion rows, 5 columns (id, 10-char string, 0 - 1000 float, string choice out of 3 variants, datetime)

_ClickHouse:_ Generated in 11m 2s (generation: 6m 8s, optimisation: 4m 54s), table size: 23.81 GB

_Trino:_ Generated in 5m 35s (generation: 5m 34s, optimisation: 120ms), table size: 17.41 GB

Same setup but 10 billion rows:

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
console.log(
  `Inserted ${result.rowsInserted} rows in ${result.generateMs}ms (optimize: ${result.optimizeMs}ms)`
);
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

| Type       | PostgreSQL       | ClickHouse | SQLite  | Trino     |
| ---------- | ---------------- | ---------- | ------- | --------- |
| `integer`  | INTEGER          | Int32      | INTEGER | INTEGER   |
| `bigint`   | BIGINT           | Int64      | INTEGER | BIGINT    |
| `float`    | DOUBLE PRECISION | Float64    | REAL    | DOUBLE    |
| `string`   | TEXT             | String     | TEXT    | VARCHAR   |
| `boolean`  | BOOLEAN          | Bool       | INTEGER | BOOLEAN   |
| `datetime` | TIMESTAMP        | DateTime   | TEXT    | TIMESTAMP |
| `date`     | DATE             | Date       | TEXT    | DATE      |

### Column Options

Each column can have additional options:

| Option            | Type      | Default | Description                                      |
| ----------------- | --------- | ------- | ------------------------------------------------ |
| `nullable`        | `boolean` | `false` | If `true`, omits `NOT NULL` constraint on column |
| `nullProbability` | `number`  | `0`     | Probability of NULL values (0-1)                 |

Example with nullable column:

```typescript
{
  name: "middle_name",
  type: "string",
  generator: { kind: "randomString", length: 10 },
  nullable: true,
  nullProbability: 0.3  // 30% of rows will have NULL
}
```

### Value Generators

| Generator        | Kind             | Options                   |
| ---------------- | ---------------- | ------------------------- |
| `sequence`       | Auto-increment   | `start`, `step`           |
| `randomInt`      | Random integer   | `min`, `max`              |
| `randomFloat`    | Random decimal   | `min`, `max`, `precision` |
| `randomString`   | Random string    | `length`                  |
| `choice`         | Pick from list   | `values`                  |
| `choiceByLookup` | Optimized choice | `values` (large arrays)   |
| `constant`       | Fixed value      | `value`                   |
| `datetime`       | Random date      | `from`, `to`              |
| `uuid`           | UUID v4          | -                         |

#### `choiceByLookup` Generator

Use `choiceByLookup` instead of `choice` when selecting from thousands of values. It uses CTEs with arrays for O(1) random selection, making it efficient for billions of rows:

```typescript
{
  name: "last_name",
  type: "string",
  generator: {
    kind: "choiceByLookup",
    values: ["Smith", "Johnson", "Williams", ...] // thousands of values
  }
}
```

- PostgreSQL: CTE with `ARRAY[]` and `array_length()` indexing
- ClickHouse: `WITH` clause with array variable
- SQLite: CTE with JSON array and `json_extract()`
- Trino: CTE with `ARRAY[]` and `element_at()`

### Generate Options

```typescript
interface GenerateOptions {
  table: TableConfig;
  rowCount: number;
  createTable?: boolean; // Default: true
  dropFirst?: boolean; // Default: false - drop table before generating
  truncateFirst?: boolean; // Default: false
  resumeSequences?: boolean; // Default: true - continue from max value
  optimize?: boolean; // Default: true - run VACUUM/OPTIMIZE after insert
}
```

### Transformations

Apply transformations to existing tables. Useful for creating derived columns (like email from first/last name) or introducing realistic data quality issues.

```typescript
// Generate data first
await generator.generate({ table: usersTable, rowCount: 10000 });

// Then apply transformations
await generator.transform("users", [
  {
    description: "Generate email addresses",
    transformations: [
      {
        kind: "template",
        column: "email",
        template: "{first_name}.{last_name}@example.com",
      },
    ],
  },
]);
```

```typescript
interface TransformResult {
  durationMs: number;
  batchesApplied: number;
}
```

#### Transformation Types

**Template Transformation** - Build column values from other columns:

```typescript
{
  kind: "template",
  column: "email",
  template: "{first_name}.{last_name}@example.com",
  lowercase: true  // Optional: convert result to lowercase
}
```

**Mutate Transformation** - Introduce random character mutations:

```typescript
{
  kind: "mutate",
  column: "name",
  probability: 0.1,  // 10% of rows get mutated
  operations: ["replace", "delete", "insert"]  // Random operation selected per row
}
```

**Lookup Transformation** - Assign values from another table via join:

```typescript
{
  kind: "lookup",
  column: "category_name",      // Column to update
  fromTable: "categories",      // Source table
  fromColumn: "name",           // Column to copy value from
  joinOn: {
    targetColumn: "category_id", // Column in target table
    lookupColumn: "id"           // Column in source table to match
  }
}
```

> **Note:** For ClickHouse, lookup transformation uses a table swap approach (CREATE → INSERT SELECT with JOIN → RENAME) since ClickHouse doesn't support correlated subqueries in `ALTER TABLE UPDATE`. This means lookups execute **before** other transformations in the same batch. If order matters, place lookups in a separate batch.

#### Batching Transformations

Transformations are organized in batches for efficiency:

- Each batch becomes a separate UPDATE statement (executed sequentially)
- Transformations within a batch are combined into a single UPDATE
- Batches support optional descriptions for logging and debugging

```typescript
await generator.transform("users", [
  {
    description: "Generate email addresses",
    transformations: [
      {
        kind: "template",
        column: "email",
        template: "{first_name}.{last_name}@example.com",
      },
    ],
  },
  {
    description: "Introduce data quality issues",
    transformations: [
      {
        kind: "mutate",
        column: "email",
        probability: 0.1,
        operations: ["replace"],
      },
    ],
  },
]);
```

With descriptions, you'll see helpful logs:

```
[postgres] Applying transformations: Generate email addresses (1 transformation(s))
[postgres] Applying transformations: Introduce data quality issues (1 transformation(s))
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

### Optimization

By default, `generate()` runs database-specific optimization after inserting rows:

| Database   | Optimization                                                                |
| ---------- | --------------------------------------------------------------------------- |
| PostgreSQL | `VACUUM ANALYZE` - reclaims storage and updates statistics                  |
| ClickHouse | `OPTIMIZE TABLE FINAL` - merges all parts for MergeTree engines             |
| SQLite     | `VACUUM` + `ANALYZE` - rebuilds file and gathers statistics                 |
| Trino      | `rewrite_data_files` + `expire_snapshots` + `remove_orphan_files` - Iceberg |

Disable for quick tests:

```typescript
await generator.generate({
  table,
  rowCount: 1000,
  optimize: false, // Skip VACUUM/OPTIMIZE
});
```

Or call manually:

```typescript
await generator.optimize("users");
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

# Or use the shortcut script
./test-all-dbs.sh
```

## Quality Checks

```bash
# Run formatting, linting, type checking, and tests
./check.sh

# Check for security vulnerabilities and outdated dependencies
./health.sh

# Check for dependency updates (requires npx renovate)
./renovate-check.sh

# Run all checks
./all-checks.sh
```
