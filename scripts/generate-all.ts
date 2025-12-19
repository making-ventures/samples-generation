import { parseArgs } from "node:util";
import {
  PostgresDataGenerator,
  ClickHouseDataGenerator,
  SQLiteDataGenerator,
  TrinoDataGenerator,
  formatDuration,
  type TableConfig,
  type DataGenerator,
} from "../src/generator/index.js";

// Usage:
//   npx tsx scripts/generate-all.ts
//   npx tsx scripts/generate-all.ts --rows 1000
//   npx tsx scripts/generate-all.ts -r 1000 --postgres
//   npx tsx scripts/generate-all.ts --clickhouse --trino
//   npx tsx scripts/generate-all.ts --help

const { values } = parseArgs({
  options: {
    rows: { type: "string", short: "r", default: "1000" },
    sqlite: { type: "boolean", default: false },
    postgres: { type: "boolean", default: false },
    clickhouse: { type: "boolean", default: false },
    trino: { type: "boolean", default: false },
    help: { type: "boolean", short: "h", default: false },
  },
});

if (values.help) {
  console.log(`
Usage: npx tsx scripts/generate-all.ts [options]

Options:
  -r, --rows <count>  Number of rows to generate (default: 1000000000)
  --sqlite            Generate for SQLite only
  --postgres          Generate for PostgreSQL only
  --clickhouse        Generate for ClickHouse only
  --trino             Generate for Trino only
  -h, --help          Show this help message

If no database is specified, all databases are generated.

Examples:
  npx tsx scripts/generate-all.ts --rows 1000
  npx tsx scripts/generate-all.ts -r 10000 --postgres
  npx tsx scripts/generate-all.ts --clickhouse --trino
`);
  process.exit(0);
}

const ROW_COUNT = parseInt(values.rows, 10);

const TABLE_CONFIG: TableConfig = {
  name: "samples",
  columns: [
    { name: "id", type: "integer", generator: { kind: "sequence", start: 1 } },
    {
      name: "name",
      type: "string",
      generator: { kind: "randomString", length: 10 },
    },
    {
      name: "value",
      type: "float",
      generator: { kind: "randomFloat", min: 0, max: 1000 },
    },
    {
      name: "status",
      type: "string",
      generator: { kind: "choice", values: ["active", "pending", "inactive"] },
    },
    { name: "created_at", type: "datetime", generator: { kind: "datetime" } },
  ],
};

interface GeneratorEntry {
  name: string;
  generator: DataGenerator;
  flag: keyof typeof values;
}

function createGenerators(): GeneratorEntry[] {
  return [
    {
      name: "SQLite",
      flag: "sqlite",
      generator: new SQLiteDataGenerator({ path: "data/samples.db" }),
    },
    {
      name: "PostgreSQL",
      flag: "postgres",
      generator: new PostgresDataGenerator({
        host: "localhost",
        port: 5432,
        database: "appdb",
        username: "postgres",
        password: "postgres",
      }),
    },
    {
      name: "ClickHouse",
      flag: "clickhouse",
      generator: new ClickHouseDataGenerator({
        host: "localhost",
        port: 8123,
        database: "default",
        username: "default",
        password: "clickhouse",
      }),
    },
    {
      name: "Trino",
      flag: "trino",
      generator: new TrinoDataGenerator({
        host: "localhost",
        port: 8080,
        user: "trino",
        catalog: "iceberg",
        schema: "warehouse",
      }),
    },
  ];
}

async function generateForDatabase(entry: GeneratorEntry): Promise<void> {
  const { name, generator } = entry;
  console.log(`\n=== ${name} ===`);

  try {
    await generator.connect();
    console.log(`Connected to ${name}`);

    // Drop existing table to ensure clean schema
    await generator.dropTable(TABLE_CONFIG.name);

    const result = await generator.generate({
      table: TABLE_CONFIG,
      rowCount: ROW_COUNT,
    });

    console.log(
      `Generated ${result.rowsInserted.toLocaleString()} rows in ${formatDuration(result.generateMs)} (optimize: ${formatDuration(result.optimizeMs)}, total: ${formatDuration(result.durationMs)})`
    );

    const count = await generator.countRows(TABLE_CONFIG.name);
    console.log(`Verified row count: ${count.toLocaleString()}`);

    const size = await generator.getTableSizeForHuman(TABLE_CONFIG.name);
    if (size !== null) {
      console.log(`Table size: ${size}`);
    }

    console.log(`Disconnected from ${name}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    console.error(`Error with ${name}: ${message}`);
  } finally {
    await generator.disconnect();
  }
}

async function main(): Promise<void> {
  const generators = createGenerators();
  console.log(
    `Generating ${ROW_COUNT.toLocaleString()} rows in each database...`
  );

  // Check which databases to generate for
  const anyDbSelected = generators.some((g) => values[g.flag]);
  const enableAll = !anyDbSelected;

  for (const entry of generators) {
    const enabled = enableAll || values[entry.flag];
    if (enabled) {
      await generateForDatabase(entry);
    } else {
      console.log(`\n=== ${entry.name} === (skipped)`);
    }
  }

  console.log("\n=== Done ===");
}

main().catch(console.error);
