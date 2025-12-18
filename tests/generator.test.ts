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
        optimize: false,
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
        optimize: false,
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
        optimize: false,
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

    it("should respect choiceByLookup generator", async () => {
      const choiceByLookupTest: TableConfig = {
        name: "test_choice_from_table",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "last_name",
            type: "string",
            generator: {
              kind: "choiceByLookup",
              values: ["Smith", "Johnson", "Williams", "Brown", "Jones"],
            },
          },
        ],
      };

      await generator.dropTable(choiceByLookupTest.name);
      await generator.createTable(choiceByLookupTest);
      await generator.generate({
        table: choiceByLookupTest,
        rowCount: 50,
        createTable: false,
        optimize: false,
      });

      const rows = await generator.queryRows(choiceByLookupTest.name, 50);
      expect(rows.length).toBe(50);

      // All values should be from the allowed list
      for (const row of rows) {
        expect(["Smith", "Johnson", "Williams", "Brown", "Jones"]).toContain(
          row.last_name
        );
      }

      await generator.dropTable(choiceByLookupTest.name);
    });

    it("should resume sequences when resumeSequences is true", async () => {
      await generator.truncateTable(testTable.name);

      // Generate first batch
      await generator.generate({
        table: testTable,
        rowCount: 5,
        createTable: false,
        optimize: false,
      });

      // Generate second batch with resumeSequences
      await generator.generate({
        table: testTable,
        rowCount: 5,
        createTable: false,
        resumeSequences: true,
        optimize: false,
      });

      const rows = await generator.queryRows(testTable.name, 20);
      const ids = rows.map((r) => Number(r.id)).sort((a, b) => a - b);
      expect(ids).toEqual([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
    });

    it("should generate NULL values based on nullProbability", async () => {
      const nullableTable: TableConfig = {
        name: "test_nullable",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "maybe_null",
            type: "string",
            generator: { kind: "randomString", length: 5 },
            nullable: true,
            nullProbability: 0.5, // 50% should be null
          },
        ],
      };

      await generator.dropTable(nullableTable.name);
      await generator.createTable(nullableTable);
      await generator.generate({
        table: nullableTable,
        rowCount: 100,
        createTable: false,
        optimize: false,
      });

      const rows = await generator.queryRows(nullableTable.name, 100);
      const nullCount = rows.filter((r) => r.maybe_null === null).length;

      // With 50% null percentage and 100 rows, we expect roughly 50 nulls
      // Allow some variance (30-70 range for statistical tolerance)
      expect(nullCount).toBeGreaterThanOrEqual(20);
      expect(nullCount).toBeLessThanOrEqual(80);

      await generator.dropTable(nullableTable.name);
    });

    it("should generate all NULLs when nullProbability is 1", async () => {
      const allNullTable: TableConfig = {
        name: "test_all_null",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "always_null",
            type: "string",
            generator: { kind: "randomString", length: 5 },
            nullable: true,
            nullProbability: 1,
          },
        ],
      };

      await generator.dropTable(allNullTable.name);
      await generator.createTable(allNullTable);
      await generator.generate({
        table: allNullTable,
        rowCount: 10,
        createTable: false,
        optimize: false,
      });

      const rows = await generator.queryRows(allNullTable.name, 10);
      const nullCount = rows.filter((r) => r.always_null === null).length;
      expect(nullCount).toBe(10);

      await generator.dropTable(allNullTable.name);
    });

    it("should generate no NULLs when nullProbability is 0", async () => {
      const noNullTable: TableConfig = {
        name: "test_no_null",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "never_null",
            type: "string",
            generator: { kind: "randomString", length: 5 },
            nullable: true,
            nullProbability: 0,
          },
        ],
      };

      await generator.dropTable(noNullTable.name);
      await generator.createTable(noNullTable);
      await generator.generate({
        table: noNullTable,
        rowCount: 10,
        createTable: false,
        optimize: false,
      });

      const rows = await generator.queryRows(noNullTable.name, 10);
      const nullCount = rows.filter((r) => r.never_null === null).length;
      expect(nullCount).toBe(0);

      await generator.dropTable(noNullTable.name);
    });
  }
);
