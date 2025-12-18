import postgres, { type Sql } from "postgres";

// Run: npx tsx scripts/write-postgres.ts

interface Sample {
  id: number;
  name: string;
  value: number;
  created_at: Date;
}

const sql: Sql = postgres({
  host: "localhost",
  port: 5432,
  database: "appdb",
  username: "postgres",
  password: "postgres",
});

async function main(): Promise<void> {
  console.log("Connecting to Postgres...");

  await sql`
    CREATE TABLE IF NOT EXISTS samples (
      id SERIAL PRIMARY KEY,
      name VARCHAR(255) NOT NULL,
      value NUMERIC NOT NULL,
      created_at TIMESTAMP DEFAULT NOW()
    )
  `;
  console.log("Table 'samples' created or already exists");

  const sampleData = [
    { name: "sample_a", value: 100.5 },
    { name: "sample_b", value: 200.75 },
    { name: "sample_c", value: 300.25 },
  ];

  for (const data of sampleData) {
    await sql`
      INSERT INTO samples (name, value)
      VALUES (${data.name}, ${data.value})
    `;
    console.log(`Inserted: ${data.name} = ${String(data.value)}`);
  }

  const rows = await sql<
    Sample[]
  >`SELECT * FROM samples ORDER BY id DESC LIMIT 10`;
  console.log("\nRecent samples:");
  for (const row of rows) {
    console.log(
      `  ${String(row.id)}: ${row.name} = ${String(row.value)} (${String(row.created_at)})`
    );
  }

  await sql.end();
  console.log("\nDone!");
}

main().catch((err: unknown) => {
  console.error("Error:", err);
  process.exit(1);
});
