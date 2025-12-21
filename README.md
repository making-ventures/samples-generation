# samples-generation

Generate sample data for multiple databases with a unified interface.

## Objective

We often need to prefill tables during tests, checks, and measurements. These generators support filling tables with random data and applying transformations (template-based construction of values from other columns, lookups in another table, data corruptions, etc). May be used for simple prefill and for controlled corruptions for further testing entity resolution.

## Supported Databases

- **PostgreSQL** - via `postgres` package
- **ClickHouse** - via `@clickhouse/client`
- **SQLite** - via `better-sqlite3`
- **Trino** - via `trino-client` (writes to Iceberg tables)

## Installation

```bash
pnpm install
```

## Measurements of generations

Environment: local databases, simple setup, 1 billion rows

### Trivial generation

5 columns (id, 10-char string, 0 - 1000 float, string choice out of 3 variants, datetime)

```bash
npx tsx scripts/generate-all.ts --scenario simple -r 1_000_000_000 --clickhouse --trino
```

#### 1 billion rows

_ClickHouse:_ Generated in 11m 2s (generation: 6m 8s, optimisation: 4m 54s), table size: 23.81 GB

_Trino:_ Generated in 6m 48s (generation: 6m 48s, optimisation: 140ms), table size: 17.41 GB

_PostgreSQL:_ Generated in 54m 26s (generation: 35m 55s, optimisation: 18m 30s), table size: 63.60 GB

#### 10 billion rows

_ClickHouse:_ Generated in 4h 52m 21s (generation: 4h 47m 44s, optimisation: 4m 36s), table size: 248.38 GB

_Trino:_ Generated in 1h 4m 41s (generation: 1h 4m 41s, optimisation: 181ms), table size: 158.62 GB

### Names, then templated email based on generated names

7 columns total.

```bash
npx tsx scripts/generate-all.ts --scenario english-names -r 1_000_000_000 --clickhouse --trino
```

#### 1 billion rows

English names:

_ClickHouse:_ Generated in 41m 48s (generation: 35m 9s, transformation: 3m 55s, optimisation: 2m 42s), table size: 44.97 GB

_Trino:_ Generated in 27m 20s (generation: 4m 52s, transformation: 22m 28s, optimisation: 66ms), table size: 33.93 GB

### Lookup transformation (10K departments → employees)

7 columns in employees table, lookup from department name in 10K-row departments table.

```bash
npx tsx scripts/generate-all.ts --scenario lookup-demo -r 1_000_000_000 --clickhouse --trino
```

#### 1 billion rows

_ClickHouse:_ Generated in 1h 19m 46s (generation: 25m 31s, transformation: 39m 25s, optimisation: 14m 49s), table size: 41.42 GB

_Trino, 20 Gb Ram:_ Generated in 32m 57s (generation: 4m 55s, transformation: 26m 36s, optimisation: 1m 25s), table size: 17.94 GB

_Trino, 16 Gb Ram + spill by ha:_ TBD

### Note

Two configurations are available:
- **Standard (20GB):** `trino` - high memory, no spilling
- **16GB comparison:** `trino-fte` (fault-tolerant execution with disk spilling) and `clickhouse` - both with 16GB limits

See `compose/docker-compose.yml` for container resource limits and `compose/trino/` or `compose/trino-fte/` for Trino-specific settings.

## Quick Start

### Starting Databases

Start all databases:
```bash
pnpm compose:up
```

Or start individual databases:
```bash
pnpm compose:postgres    # PostgreSQL only
pnpm compose:clickhouse  # ClickHouse only
pnpm compose:trino       # Trino 20GB, high memory
pnpm compose:trino-fte   # Trino 16GB, fault-tolerant execution with spill
```

> **Note:** `trino` and `trino-fte` share port 8080 - stop one before starting the other.

Stop and clean up:
```bash
pnpm compose:down   # Stop containers
pnpm compose:reset  # Stop and remove volumes
```

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

| Generator        | Kind             | Options                                |
| ---------------- | ---------------- | -------------------------------------- |
| `sequence`       | Auto-increment   | `start`, `step`                        |
| `randomInt`      | Random integer   | `min`, `max`                           |
| `randomFloat`    | Random float     | `min`, `max`, `precision` (default: 2) |
| `randomString`   | Random string    | `length`                               |
| `choice`         | Pick from list   | `values`                               |
| `choiceByLookup` | Optimized choice | `values` (large arrays)                |
| `constant`       | Fixed value      | `value`                                |
| `datetime`       | Random datetime  | `from`, `to`                           |
| `uuid`           | UUID v4          | -                                      |

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

**Swap Transformation** - Swap values between two columns with probability:

```typescript
{
  kind: "swap",
  column1: "first_name",
  column2: "last_name",
  probability: 0.1  // 10% of rows get swapped
}
```

> **Note:** Both columns use the same random decision per row, ensuring atomic swaps (if column1 gets column2's value, column2 always gets column1's value). For ClickHouse, swap also uses the table swap approach (like lookup) since ClickHouse evaluates each `rand()` call separately. Multiple swaps in the same batch are combined into a single table swap operation for efficiency.

> **Design Note:** PostgreSQL, SQLite, and Trino execute each swap as a separate `UPDATE ... WHERE random() < probability` statement. This is intentionally not batched because UPDATE is a lightweight operation on these databases. ClickHouse batches swaps because each swap would otherwise require a full table copy (CREATE → INSERT → RENAME → DROP), making the overhead significant.

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

### Scenarios

A `Scenario` orchestrates multi-step data generation workflows - create lookup tables, generate main tables, and apply cross-table transformations in sequence:

```typescript
import { PostgresDataGenerator, type Scenario } from "./src/generator/index.js";

const scenario: Scenario = {
  name: "E-commerce data",
  steps: [
    // Step 1: Create lookup table
    {
      table: {
        name: "departments",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "name",
            type: "string",
            generator: {
              kind: "choice",
              values: ["Engineering", "Sales", "HR"],
            },
          },
        ],
      },
      rowCount: 3,
    },
    // Step 2: Generate main table with transformations
    {
      table: {
        name: "employees",
        columns: [
          {
            name: "id",
            type: "bigint",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "first_name",
            type: "string",
            generator: {
              kind: "choice",
              values: ["John", "Jane", "Bob"],
            },
          },
          {
            name: "last_name",
            type: "string",
            generator: { kind: "choice", values: ["Smith", "Jones"] },
          },
          {
            name: "email",
            type: "string",
            generator: { kind: "constant", value: "" },
          },
          {
            name: "department_id",
            type: "integer",
            generator: { kind: "randomInt", min: 1, max: 3 },
          },
          {
            name: "department_name",
            type: "string",
            generator: { kind: "constant", value: "" },
          },
        ],
      },
      rowCount: 1000,
      transformations: [
        {
          description: "Generate email from names",
          transformations: [
            {
              kind: "template",
              column: "email",
              template: "{first_name}.{last_name}@company.com",
              lowercase: true,
            },
          ],
        },
      ],
    },
    // Step 3: Transform-only step - apply cross-table lookup
    {
      tableName: "employees",
      transformations: [
        {
          description: "Lookup department name",
          transformations: [
            {
              kind: "lookup",
              column: "department_name",
              fromTable: "departments",
              fromColumn: "name",
              joinOn: { targetColumn: "department_id", lookupColumn: "id" },
            },
          ],
        },
      ],
    },
  ],
};

const generator = new PostgresDataGenerator({
  /* config */
});
await generator.connect();

const result = await generator.runScenario({
  scenario,
  dropFirst: true,
});

for (const step of result.steps) {
  console.log(`[${step.tableName}] ${step.generate?.rowsInserted ?? 0} rows`);
  if (step.transform) {
    console.log(
      `  Applied ${step.transform.batchesApplied} transformation batch(es)`
    );
  }
}
console.log(
  `Total: ${result.totalRowsInserted} rows in ${result.durationMs}ms`
);

await generator.disconnect();
```

**Step Types:**

| Step Type            | Fields                                  | Description                                     |
| -------------------- | --------------------------------------- | ----------------------------------------------- |
| Generate + Transform | `table`, `rowCount`, `transformations?` | Create table, insert rows, optionally transform |
| Transform only       | `tableName`, `transformations`          | Apply transformations to existing table         |

```typescript
interface ScenarioOptions {
  scenario: Scenario;
  createTable?: boolean; // Default: true
  dropFirst?: boolean; // Default: false
  truncateFirst?: boolean; // Default: false
  resumeSequences?: boolean; // Default: true
  optimize?: boolean; // Default: true (runs once at end for all tables)
}

interface ScenarioResult {
  steps: ScenarioStepResult[];
  totalRowsInserted: number;
  durationMs: number; // Total wall-clock time
  generateMs: number; // Time spent generating rows
  transformMs: number; // Time spent applying transformations
  optimizeMs: number; // Time spent on optimisations
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

### Optimization

By default, `generate()` runs database-specific optimization after inserting rows:

| Database   | Optimization                                                            |
| ---------- | ----------------------------------------------------------------------- |
| PostgreSQL | `VACUUM ANALYZE` - reclaims storage and updates statistics              |
| ClickHouse | `OPTIMIZE TABLE FINAL` - merges all parts for MergeTree engines         |
| SQLite     | `VACUUM` + `ANALYZE` - rebuilds file and gathers statistics             |
| Trino      | `optimize` + `expire_snapshots` + `remove_orphan_files` - Iceberg       |

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

# Specify row count
npx tsx scripts/generate-all.ts --rows 1000
npx tsx scripts/generate-all.ts -r 1_000_000

# Choose scenario
npx tsx scripts/generate-all.ts --scenario simple          # Default: 5 columns
npx tsx scripts/generate-all.ts --scenario english-names   # Names + email template
npx tsx scripts/generate-all.ts --scenario russian-names   # Russian names + email
npx tsx scripts/generate-all.ts --scenario lookup-demo     # Departments + employees lookup

# Generate for specific databases only
npx tsx scripts/generate-all.ts --sqlite
npx tsx scripts/generate-all.ts --postgres
npx tsx scripts/generate-all.ts --clickhouse
npx tsx scripts/generate-all.ts --trino

# Combine options
npx tsx scripts/generate-all.ts -r 10000 --scenario english-names --postgres --clickhouse

# Show help
npx tsx scripts/generate-all.ts --help
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
# Run unit tests (SQL expression generators, escape functions)
pnpm test

# Run e2e tests against SQLite (default, no setup required)
pnpm test:e2e

# Run e2e tests with specific databases (requires docker-compose up)
TEST_POSTGRES=1 pnpm test:e2e
TEST_CLICKHOUSE=1 pnpm test:e2e
TEST_TRINO=1 pnpm test:e2e

# Run e2e tests against all databases
TEST_POSTGRES=1 TEST_CLICKHOUSE=1 TEST_TRINO=1 pnpm test:e2e

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
