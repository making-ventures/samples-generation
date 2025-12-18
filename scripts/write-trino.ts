import { BasicAuth, Trino } from "trino-client";

// Run: npx tsx scripts/write-trino.ts

interface TrinoResult {
  data?: unknown[][];
}

function noop(): void {
  // intentionally empty
}

async function main(): Promise<void> {
  console.log("Connecting to Trino...");

  const trino = Trino.create({
    server: "http://localhost:8080",
    catalog: "iceberg",
    schema: "default",
    auth: new BasicAuth("trino"),
  });

  const createSchema = await trino.query(
    "CREATE SCHEMA IF NOT EXISTS iceberg.samples"
  );
  await createSchema.forEach(noop);
  console.log("Schema 'iceberg.samples' created or already exists");

  const createTable = await trino.query(`
    CREATE TABLE IF NOT EXISTS iceberg.samples.data (
      id BIGINT,
      name VARCHAR,
      value DOUBLE,
      created_at TIMESTAMP
    ) WITH (
      format = 'PARQUET'
    )
  `);
  await createTable.forEach(noop);
  console.log("Table 'iceberg.samples.data' created or already exists");

  const sampleData = [
    { id: 1, name: "sample_a", value: 100.5 },
    { id: 2, name: "sample_b", value: 200.75 },
    { id: 3, name: "sample_c", value: 300.25 },
  ];

  for (const data of sampleData) {
    const insertQuery = await trino.query(`
      INSERT INTO iceberg.samples.data (id, name, value, created_at)
      VALUES (${String(data.id)}, '${data.name}', ${String(data.value)}, current_timestamp)
    `);
    await insertQuery.forEach(noop);
    console.log(`Inserted: ${data.name} = ${String(data.value)}`);
  }

  const selectQuery = await trino.query(
    "SELECT * FROM iceberg.samples.data ORDER BY id LIMIT 10"
  );
  console.log("\nRecent samples:");
  for await (const result of selectQuery) {
    const trinoResult = result as TrinoResult;
    if (trinoResult.data) {
      for (const row of trinoResult.data) {
        const [id, name, value, createdAt] = row as [
          number,
          string,
          number,
          string,
        ];
        console.log(
          `  ${String(id)}: ${name} = ${String(value)} (${createdAt})`
        );
      }
    }
  }

  console.log("\nDone!");
}

main().catch((err: unknown) => {
  console.error("Error:", err);
  process.exit(1);
});
