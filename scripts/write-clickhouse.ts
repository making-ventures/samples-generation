import { createClient } from "@clickhouse/client";

// Run: npx tsx scripts/write-clickhouse.ts

interface Sample {
  id: number;
  name: string;
  value: number;
  created_at: string;
}

async function main(): Promise<void> {
  console.log("Connecting to ClickHouse...");

  const client = createClient({
    url: "http://localhost:8123",
    username: "default",
    password: "clickhouse",
    database: "default",
  });

  await client.command({
    query: `
      CREATE TABLE IF NOT EXISTS samples (
        id UInt64,
        name String,
        value Float64,
        created_at DateTime DEFAULT now()
      ) ENGINE = MergeTree()
      ORDER BY id
    `,
  });
  console.log("Table 'samples' created or already exists");

  const sampleData = [
    { id: 1, name: "sample_a", value: 100.5 },
    { id: 2, name: "sample_b", value: 200.75 },
    { id: 3, name: "sample_c", value: 300.25 },
  ];

  await client.insert({
    table: "samples",
    values: sampleData,
    format: "JSONEachRow",
  });
  console.log("Inserted samples:", sampleData.map((d) => d.name).join(", "));

  const result = await client.query({
    query: "SELECT * FROM samples ORDER BY id LIMIT 10",
    format: "JSONEachRow",
  });
  const rows: Sample[] = await result.json();

  console.log("\nRecent samples:");
  for (const row of rows) {
    console.log(
      `  ${String(row.id)}: ${row.name} = ${String(row.value)} (${row.created_at})`
    );
  }

  await client.close();
  console.log("\nDone!");
}

main().catch((err: unknown) => {
  console.error("Error:", err);
  process.exit(1);
});
