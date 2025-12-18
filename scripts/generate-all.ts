import {
  PostgresDataGenerator,
  ClickHouseDataGenerator,
  SQLiteDataGenerator,
  TrinoDataGenerator,
  type TableConfig,
  type DataGenerator,
} from "../src/generator/index.js";

// Run: npx tsx scripts/generate-all.ts
// Run: GENERATE_CLICKHOUSE=1 GENERATE_TRINO=1 npx tsx scripts/generate-all.ts

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

// const ROW_COUNT = 100_000_000;
const ROW_COUNT = 1_000_000_000;

interface GeneratorEntry {
  name: string;
  generator: DataGenerator;
  envVar: string;
}

function createGenerators(): GeneratorEntry[] {
  return [
    {
      name: "SQLite",
      envVar: "GENERATE_SQLITE",
      generator: new SQLiteDataGenerator({ path: "data/samples.db" }),
    },
    {
      name: "PostgreSQL",
      envVar: "GENERATE_POSTGRES",
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
      envVar: "GENERATE_CLICKHOUSE",
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
      envVar: "GENERATE_TRINO",
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
      `Generated ${result.rowsInserted.toLocaleString()} rows in ${result.durationMs.toLocaleString()}ms`
    );

    const count = await generator.countRows(TABLE_CONFIG.name);
    console.log(`Verified row count: ${count.toLocaleString()}`);

    const size = await generator.getTableSizeForHuman(TABLE_CONFIG.name);
    if (size !== null) {
      console.log(`Table size: ${size}`);
    }

    await generator.disconnect();
    console.log(`Disconnected from ${name}`);
  } catch (err) {
    const message = err instanceof Error ? err.message : "Unknown error";
    console.error(`Error with ${name}: ${message}`);
  }
}

async function main(): Promise<void> {
  const generators = createGenerators();
  console.log(
    `Generating ${ROW_COUNT.toLocaleString()} rows in each database...`
  );

  // Check which databases to generate for
  const enableAll = !generators.some((g) => process.env[g.envVar] === "1");

  for (const entry of generators) {
    const enabled = enableAll || process.env[entry.envVar] === "1";
    if (enabled) {
      await generateForDatabase(entry);
    } else {
      console.log(`\n=== ${entry.name} === (skipped)`);
    }
  }

  console.log("\n=== Done ===");
}

main().catch(console.error);
