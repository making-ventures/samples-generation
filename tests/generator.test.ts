import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "fs";
import * as path from "path";
import type { DataGenerator, TableConfig } from "../src/generator/index.js";
import {
  PostgresDataGenerator,
  ClickHouseDataGenerator,
  SQLiteDataGenerator,
  TrinoDataGenerator,
} from "../src/generator/index.js";

// Test table configuration
const testTable: TableConfig = {
  name: "test_samples",
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
      generator: { kind: "randomFloat", min: 0, max: 1000, precision: 2 },
    },
    {
      name: "category",
      type: "string",
      generator: { kind: "choice", values: ["A", "B", "C"] },
    },
    {
      name: "active",
      type: "boolean",
      generator: { kind: "constant", value: true },
    },
  ],
};

// Factory for creating generators based on environment
interface GeneratorFactory {
  name: string;
  create: () => DataGenerator;
  skip?: boolean;
}

const dataDir = path.join(process.cwd(), "data");
if (!fs.existsSync(dataDir)) {
  fs.mkdirSync(dataDir, { recursive: true });
}

const generators: GeneratorFactory[] = [
  {
    name: "sqlite",
    create: () =>
      new SQLiteDataGenerator({ path: path.join(dataDir, "test.db") }),
    skip: false,
  },
  {
    name: "postgres",
    create: () =>
      new PostgresDataGenerator({
        host: "localhost",
        port: 5432,
        database: "appdb",
        username: "postgres",
        password: "postgres",
      }),
    skip: !process.env.TEST_POSTGRES,
  },
  {
    name: "clickhouse",
    create: () =>
      new ClickHouseDataGenerator({
        host: "localhost",
        port: 8123,
        username: "default",
        password: "clickhouse",
        database: "default",
      }),
    skip: !process.env.TEST_CLICKHOUSE,
  },
  {
    name: "trino",
    create: () =>
      new TrinoDataGenerator({
        host: "localhost",
        port: 8080,
        catalog: "iceberg",
        schema: "test",
        user: "trino",
      }),
    skip: !process.env.TEST_TRINO,
  },
];

// Shared test suite that runs against each generator
describe.each(generators.filter((g) => !g.skip))(
  "DataGenerator: $name",
  ({ create }) => {
    let generator: DataGenerator;

    beforeAll(async () => {
      generator = create();
      await generator.connect();
      await generator.dropTable(testTable.name);
    });

    afterAll(async () => {
      await generator.dropTable(testTable.name);
      await generator.disconnect();
    });

    it("should create a table", async () => {
      await generator.createTable(testTable);
      const count = await generator.countRows(testTable.name);
      expect(count).toBe(0);
    });

    it("should generate and insert rows", async () => {
      const result = await generator.generate({
        table: testTable,
        rowCount: 10,
        createTable: false,
      });

      expect(result.rowsInserted).toBe(10);
      expect(result.durationMs).toBeGreaterThanOrEqual(0);
    });

    it("should count rows correctly", async () => {
      const count = await generator.countRows(testTable.name);
      expect(count).toBe(10);
    });

    it("should query rows", async () => {
      const rows = await generator.queryRows(testTable.name, 5);
      expect(rows.length).toBe(5);

      // Check that rows have expected columns
      for (const row of rows) {
        expect(row).toHaveProperty("id");
        expect(row).toHaveProperty("name");
        expect(row).toHaveProperty("value");
        expect(row).toHaveProperty("category");
      }
    });

    it("should truncate table", async () => {
      await generator.truncateTable(testTable.name);
      const count = await generator.countRows(testTable.name);
      expect(count).toBe(0);
    });

    it("should generate multiple rows", async () => {
      const result = await generator.generate({
        table: testTable,
        rowCount: 25,
        createTable: false,
      });

      expect(result.rowsInserted).toBe(25);
      const count = await generator.countRows(testTable.name);
      expect(count).toBe(25);
    });

    it("should respect sequence generator", async () => {
      await generator.truncateTable(testTable.name);
      await generator.generate({
        table: testTable,
        rowCount: 5,
        createTable: false,
      });

      const rows = await generator.queryRows(testTable.name, 10);
      const ids = rows.map((r) => Number(r.id)).sort((a, b) => a - b);
      expect(ids).toEqual([1, 2, 3, 4, 5]);
    });

    it("should respect choice generator", async () => {
      const rows = await generator.queryRows(testTable.name, 10);
      for (const row of rows) {
        expect(["A", "B", "C"]).toContain(row.category);
      }
    });

    it("should resume sequences when resumeSequences is true", async () => {
      await generator.truncateTable(testTable.name);

      // Generate first batch
      await generator.generate({
        table: testTable,
        rowCount: 5,
        createTable: false,
      });

      // Generate second batch with resumeSequences
      await generator.generate({
        table: testTable,
        rowCount: 5,
        createTable: false,
        resumeSequences: true,
      });

      const rows = await generator.queryRows(testTable.name, 20);
      const ids = rows.map((r) => Number(r.id)).sort((a, b) => a - b);
      expect(ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });
  }
);
