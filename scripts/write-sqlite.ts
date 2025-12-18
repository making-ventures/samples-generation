import Database, { type Database as DatabaseType } from "better-sqlite3";

// Run: npx tsx scripts/write-sqlite.ts

interface Sample {
  id: number;
  name: string;
  value: number;
  created_at: string;
}

function main(): void {
  console.log("Opening SQLite database...");

  const db: DatabaseType = new Database("data/samples.db");

  db.exec(`
    CREATE TABLE IF NOT EXISTS samples (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL,
      value REAL NOT NULL,
      created_at TEXT DEFAULT (datetime('now'))
    )
  `);
  console.log("Table 'samples' created or already exists");

  const insert = db.prepare(`
    INSERT INTO samples (name, value) VALUES (?, ?)
  `);

  const sampleData = [
    { name: "sample_a", value: 100.5 },
    { name: "sample_b", value: 200.75 },
    { name: "sample_c", value: 300.25 },
  ];

  for (const data of sampleData) {
    insert.run(data.name, data.value);
    console.log(`Inserted: ${data.name} = ${String(data.value)}`);
  }

  const rows = db
    .prepare("SELECT * FROM samples ORDER BY id DESC LIMIT 10")
    .all() as Sample[];
  console.log("\nRecent samples:");
  for (const row of rows) {
    console.log(
      `  ${String(row.id)}: ${row.name} = ${String(row.value)} (${row.created_at})`
    );
  }

  db.close();
  console.log("\nDone!");
}

main();
