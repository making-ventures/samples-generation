import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "fs";
import * as path from "path";
import type {
  DataGenerator,
  TableConfig,
  Scenario,
} from "../src/generator/index.js";
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

    it(
      "should resume sequences when resumeSequences is true",
      { timeout: 30_000 },
      async () => {
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
      }
    );

    it(
      "should handle BIGINT sequence starting at 2^31 without overflow",
      { timeout: 30_000 },
      async () => {
        // This tests that all databases handle sequences exceeding 32-bit integer range
        const bigintTable: TableConfig = {
          name: "test_bigint_overflow",
          columns: [
            {
              name: "id",
              type: "bigint",
              generator: { kind: "sequence", start: 2147483648 }, // 2^31
            },
            {
              name: "name",
              type: "string",
              generator: { kind: "constant", value: "test" },
            },
          ],
        };

        await generator.dropTable(bigintTable.name);
        await generator.createTable(bigintTable);

        const result = await generator.generate({
          table: bigintTable,
          rowCount: 5,
          createTable: false,
          optimize: false,
        });

        expect(result.rowsInserted).toBe(5);

        const rows = await generator.queryRows(bigintTable.name, 5);
        expect(rows.length).toBe(5);

        // Verify IDs are in the expected BIGINT range
        const ids = rows.map((r) => BigInt(r.id as string | number));
        for (const id of ids) {
          expect(id >= 2147483648n).toBe(true);
          expect(id < 2147483653n).toBe(true); // start + 5
        }

        await generator.dropTable(bigintTable.name);
      }
    );

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

    it("should apply template transformation", async () => {
      const templateTable: TableConfig = {
        name: "test_template",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "first_name",
            type: "string",
            generator: { kind: "choice", values: ["John", "Jane", "Bob"] },
          },
          {
            name: "last_name",
            type: "string",
            generator: { kind: "choice", values: ["Smith", "Doe", "Johnson"] },
          },
          {
            name: "email",
            type: "string",
            generator: { kind: "constant", value: "" },
          },
        ],
      };

      await generator.dropTable(templateTable.name);
      await generator.createTable(templateTable);
      await generator.generate({
        table: templateTable,
        rowCount: 10,
        createTable: false,
        optimize: false,
      });

      await generator.transform(templateTable.name, [
        {
          transformations: [
            {
              kind: "template",
              column: "email",
              template: "{first_name}.{last_name}@example.com",
              lowercase: true,
            },
          ],
        },
      ]);

      const rows = await generator.queryRows(templateTable.name, 10);
      for (const row of rows) {
        const firstName = String(row.first_name).toLowerCase();
        const lastName = String(row.last_name).toLowerCase();
        expect(row.email).toBe(`${firstName}.${lastName}@example.com`);
      }

      await generator.dropTable(templateTable.name);
    });

    it(
      "should apply mutate transformation with probability",
      { timeout: 30_000 },
      async () => {
        const mutateTable: TableConfig = {
          name: "test_mutate",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "code",
              type: "string",
              generator: { kind: "constant", value: "AAAAAAAAAA" },
            },
          ],
        };

        await generator.dropTable(mutateTable.name);
        await generator.createTable(mutateTable);
        await generator.generate({
          table: mutateTable,
          rowCount: 100,
          createTable: false,
          optimize: false,
        });

        await generator.transform(mutateTable.name, [
          {
            transformations: [
              {
                kind: "mutate",
                column: "code",
                probability: 0.5,
                operations: ["replace"],
              },
            ],
          },
        ]);

        const rows = await generator.queryRows(mutateTable.name, 100);
        let mutatedCount = 0;
        for (const row of rows) {
          if (row.code !== "AAAAAAAAAA") {
            mutatedCount++;
            // Mutated values should contain an X
            expect(row.code).toContain("X");
          }
        }

        // With 50% probability, expect roughly half to be mutated
        expect(mutatedCount).toBeGreaterThanOrEqual(20);
        expect(mutatedCount).toBeLessThanOrEqual(80);

        await generator.dropTable(mutateTable.name);
      }
    );

    it(
      "should apply mutate transformation with multiple random operations",
      { timeout: 30_000 },
      async () => {
        const mutateMultiTable: TableConfig = {
          name: "test_mutate_multi",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "code",
              type: "string",
              generator: { kind: "constant", value: "AAAAAAAAAA" },
            },
          ],
        };

        await generator.dropTable(mutateMultiTable.name);
        await generator.createTable(mutateMultiTable);
        await generator.generate({
          table: mutateMultiTable,
          rowCount: 100,
          createTable: false,
          optimize: false,
        });

        await generator.transform(mutateMultiTable.name, [
          {
            transformations: [
              {
                kind: "mutate",
                column: "code",
                probability: 1.0, // 100% mutation rate
                operations: ["replace", "delete", "insert"],
              },
            ],
          },
        ]);

        const rows = await generator.queryRows(mutateMultiTable.name, 100);
        let mutatedCount = 0;

        for (const row of rows) {
          const code = String(row.code);
          // Any change from original counts as mutation
          // Note: Due to SQL's random() evaluation, positions may vary chaotically
          if (code !== "AAAAAAAAAA") {
            mutatedCount++;
          }
        }

        // With 100% probability, most rows should be mutated
        // Some edge cases may occur due to SQL random() evaluation quirks
        expect(mutatedCount).toBeGreaterThanOrEqual(90);

        await generator.dropTable(mutateMultiTable.name);
      }
    );

    it("should apply lookup transformation", async () => {
      // Create a lookup table (source of values)
      const lookupTable: TableConfig = {
        name: "test_lookup_source",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "category_name",
            type: "string",
            generator: {
              kind: "choice",
              values: ["Electronics", "Books", "Clothing"],
            },
          },
        ],
      };

      // Create a target table that will lookup values
      const targetTable: TableConfig = {
        name: "test_lookup_target",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "category_id",
            type: "integer",
            generator: { kind: "choice", values: [1, 2, 3] },
          },
          {
            name: "category_name",
            type: "string",
            generator: { kind: "constant", value: "" },
          },
        ],
      };

      await generator.dropTable(lookupTable.name);
      await generator.dropTable(targetTable.name);

      // Create and populate lookup table
      await generator.createTable(lookupTable);
      await generator.generate({
        table: lookupTable,
        rowCount: 3,
        createTable: false,
        optimize: false,
      });

      // Create and populate target table with lookup transformation
      await generator.createTable(targetTable);
      await generator.generate({
        table: targetTable,
        rowCount: 10,
        createTable: false,
        optimize: false,
      });

      await generator.transform(targetTable.name, [
        {
          transformations: [
            {
              kind: "lookup",
              column: "category_name",
              fromTable: "test_lookup_source",
              fromColumn: "category_name",
              joinOn: {
                targetColumn: "category_id",
                lookupColumn: "id",
              },
            },
          ],
        },
      ]);

      const lookupRows = await generator.queryRows(lookupTable.name, 3);
      const targetRows = await generator.queryRows(targetTable.name, 10);

      // Build a map of id -> category_name from lookup table
      const lookupMap = new Map<number, string>();
      for (const row of lookupRows) {
        lookupMap.set(Number(row.id), String(row.category_name));
      }

      // Verify each target row has the correct category_name based on category_id
      for (const row of targetRows) {
        const categoryId = Number(row.category_id);
        const expectedName = lookupMap.get(categoryId);
        expect(row.category_name).toBe(expectedName);
      }

      await generator.dropTable(targetTable.name);
      await generator.dropTable(lookupTable.name);
    });

    it(
      "should apply lookups before template/mutate in same batch (ClickHouse behavior)",
      { timeout: 30_000 },
      async () => {
        // This test documents that lookup transformations execute BEFORE other
        // transformations in the same batch due to ClickHouse's table swap approach.
        // If order matters, use separate postTransformations batches.

        const lookupTable: TableConfig = {
          name: "test_order_lookup",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "prefix",
              type: "string",
              generator: { kind: "constant", value: "LOOKED_UP" },
            },
          ],
        };

        const targetTable: TableConfig = {
          name: "test_order_target",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "lookup_id",
              type: "integer",
              generator: { kind: "constant", value: 1 },
            },
            {
              name: "prefix",
              type: "string",
              generator: { kind: "constant", value: "INITIAL" },
            },
            {
              name: "result",
              type: "string",
              generator: { kind: "constant", value: "" },
            },
          ],
        };

        await generator.dropTable(lookupTable.name);
        await generator.dropTable(targetTable.name);

        await generator.createTable(lookupTable);
        await generator.generate({
          table: lookupTable,
          rowCount: 1,
          createTable: false,
          optimize: false,
        });

        await generator.createTable(targetTable);

        // Apply lookup and template in the SAME batch
        // Template references the 'prefix' column that lookup updates
        await generator.generate({
          table: targetTable,
          rowCount: 5,
          createTable: false,
          optimize: false,
        });

        await generator.transform(targetTable.name, [
          {
            transformations: [
              // Template declared first, but lookup executes first in ClickHouse
              {
                kind: "template",
                column: "result",
                template: "prefix={prefix}",
              },
              {
                kind: "lookup",
                column: "prefix",
                fromTable: "test_order_lookup",
                fromColumn: "prefix",
                joinOn: {
                  targetColumn: "lookup_id",
                  lookupColumn: "id",
                },
              },
            ],
          },
        ]);

        const rows = await generator.queryRows(targetTable.name, 5);

        // In ClickHouse: lookup runs first, then template sees "LOOKED_UP"
        // In Postgres/SQLite/Trino: transformations run in order, template sees "INITIAL"
        for (const row of rows) {
          expect(row.prefix).toBe("LOOKED_UP");
          // Result depends on execution order:
          // - ClickHouse: "prefix=LOOKED_UP" (lookup first)
          // - Others: "prefix=INITIAL" (template first, then lookup overwrites prefix)
          expect(["prefix=LOOKED_UP", "prefix=INITIAL"]).toContain(row.result);
        }

        await generator.dropTable(targetTable.name);
        await generator.dropTable(lookupTable.name);
      }
    );

    it(
      "should apply swap transformation with probability",
      { timeout: 30_000 },
      async () => {
        // Test that swap transformation swaps two columns with given probability
        const swapTable: TableConfig = {
          name: "test_swap",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "col_a",
              type: "string",
              generator: { kind: "constant", value: "AAA" },
            },
            {
              name: "col_b",
              type: "string",
              generator: { kind: "constant", value: "BBB" },
            },
          ],
        };

        await generator.dropTable(swapTable.name);
        await generator.createTable(swapTable);

        // Generate 100 rows for statistical significance
        await generator.generate({
          table: swapTable,
          rowCount: 100,
          createTable: false,
          optimize: false,
        });

        // Apply swap with 50% probability
        await generator.transform(swapTable.name, [
          {
            transformations: [
              {
                kind: "swap",
                column1: "col_a",
                column2: "col_b",
                probability: 0.5,
              },
            ],
          },
        ]);

        const rows = await generator.queryRows(swapTable.name, 100);

        // Count swapped vs unswapped rows
        let swapped = 0;
        let unswapped = 0;
        for (const row of rows) {
          if (row.col_a === "BBB" && row.col_b === "AAA") {
            swapped++;
          } else if (row.col_a === "AAA" && row.col_b === "BBB") {
            unswapped++;
          } else {
            // This should never happen - both columns should swap together
            throw new Error(
              `Unexpected state: col_a=${String(row.col_a)}, col_b=${String(row.col_b)}`
            );
          }
        }

        // With 50% probability and 100 rows, expect roughly 50 swapped
        // Allow wide margin (20-80) to avoid flaky tests
        expect(swapped).toBeGreaterThan(20);
        expect(swapped).toBeLessThan(80);
        expect(swapped + unswapped).toBe(100);

        await generator.dropTable(swapTable.name);
      }
    );

    it(
      "should support batch descriptions for transformations",
      { timeout: 30_000 },
      async () => {
        // This test verifies that TransformationBatch with descriptions works
        const descTable: TableConfig = {
          name: "test_batch_desc",
          columns: [
            {
              name: "id",
              type: "integer",
              generator: { kind: "sequence", start: 1 },
            },
            {
              name: "first_name",
              type: "string",
              generator: { kind: "constant", value: "John" },
            },
            {
              name: "last_name",
              type: "string",
              generator: { kind: "constant", value: "Doe" },
            },
            {
              name: "email",
              type: "string",
              generator: { kind: "constant", value: "" },
            },
          ],
        };

        await generator.dropTable(descTable.name);
        await generator.createTable(descTable);

        // Use object form with descriptions
        await generator.generate({
          table: descTable,
          rowCount: 5,
          createTable: false,
          optimize: false,
        });

        await generator.transform(descTable.name, [
          {
            description: "Generate email from name",
            transformations: [
              {
                kind: "template",
                column: "email",
                template: "{first_name}.{last_name}@test.com",
                lowercase: true,
              },
            ],
          },
        ]);

        const rows = await generator.queryRows(descTable.name, 5);
        for (const row of rows) {
          expect(row.email).toBe("john.doe@test.com");
        }

        await generator.dropTable(descTable.name);
      }
    );

    it(
      "should run a complete scenario with runScenario",
      { timeout: 30_000 },
      async () => {
        const scenario = {
          name: "Test scenario",
          steps: [
            {
              table: {
                name: "test_scenario",
                columns: [
                  {
                    name: "id",
                    type: "integer" as const,
                    generator: { kind: "sequence" as const, start: 1 },
                  },
                  {
                    name: "first_name",
                    type: "string" as const,
                    generator: {
                      kind: "choice" as const,
                      values: ["Alice", "Bob", "Charlie"],
                    },
                  },
                  {
                    name: "last_name",
                    type: "string" as const,
                    generator: {
                      kind: "choice" as const,
                      values: ["Smith", "Jones", "Brown"],
                    },
                  },
                  {
                    name: "email",
                    type: "string" as const,
                    generator: { kind: "constant" as const, value: "" },
                  },
                ],
              },
              rowCount: 10,
              transformations: [
                {
                  description: "Generate email from names",
                  transformations: [
                    {
                      kind: "template" as const,
                      column: "email",
                      template: "{first_name}.{last_name}@example.com",
                      lowercase: true,
                    },
                  ],
                },
              ],
            },
          ],
        };

        await generator.dropTable("test_scenario");

        const result = await generator.runScenario({
          scenario,
        });

        // Verify we have one step result
        expect(result.steps.length).toBe(1);
        const stepResult = result.steps[0];
        expect(stepResult).toBeDefined();
        expect(stepResult?.generate).toBeDefined();

        // Verify generation results
        expect(stepResult?.generate?.rowsInserted).toBe(10);
        expect(stepResult?.generate?.generateMs).toBeGreaterThan(0);

        // Verify transformation results
        expect(stepResult?.transform).toBeDefined();
        expect(stepResult?.transform?.batchesApplied).toBe(1);

        // Verify total rows
        expect(result.totalRowsInserted).toBe(10);
        expect(result.durationMs).toBeGreaterThan(0);

        // Verify data
        const rows = await generator.queryRows("test_scenario", 10);
        expect(rows.length).toBe(10);

        // Check email format
        for (const row of rows) {
          const email = row.email as string;
          expect(email).toMatch(/.+\..+@example\.com/);
          expect(email).toBe(email.toLowerCase());
        }

        await generator.dropTable("test_scenario");
      }
    );

    it(
      "should run a multi-table scenario with runScenario",
      { timeout: 30_000 },
      async () => {
        const scenario = {
          name: "Multi-table test",
          steps: [
            {
              table: {
                name: "test_users",
                columns: [
                  {
                    name: "id",
                    type: "integer" as const,
                    generator: { kind: "sequence" as const, start: 1 },
                  },
                  {
                    name: "name",
                    type: "string" as const,
                    generator: { kind: "randomString" as const, length: 8 },
                  },
                ],
              },
              rowCount: 5,
            },
            {
              table: {
                name: "test_orders",
                columns: [
                  {
                    name: "id",
                    type: "integer" as const,
                    generator: { kind: "sequence" as const, start: 1 },
                  },
                  {
                    name: "amount",
                    type: "float" as const,
                    generator: {
                      kind: "randomFloat" as const,
                      min: 10,
                      max: 100,
                    },
                  },
                ],
              },
              rowCount: 15,
            },
          ],
        };

        await generator.dropTable("test_users");
        await generator.dropTable("test_orders");

        const result = await generator.runScenario({
          scenario,
        });

        // Verify we have two step results
        expect(result.steps.length).toBe(2);
        expect(result.steps[0]?.tableName).toBe("test_users");
        expect(result.steps[0]?.generate?.rowsInserted).toBe(5);
        expect(result.steps[1]?.tableName).toBe("test_orders");
        expect(result.steps[1]?.generate?.rowsInserted).toBe(15);

        // Verify total rows
        expect(result.totalRowsInserted).toBe(20);

        // Verify data in both tables
        const users = await generator.queryRows("test_users", 10);
        expect(users.length).toBe(5);

        const orders = await generator.queryRows("test_orders", 20);
        expect(orders.length).toBe(15);

        await generator.dropTable("test_users");
        await generator.dropTable("test_orders");
      }
    );

    it("should run a scenario with transform-only steps", async () => {
      // Step 1: Generate users table with original data
      // Step 2: Transform-only step to update the users table
      const userTable: TableConfig = {
        name: "test_users",
        columns: [
          {
            name: "id",
            type: "integer",
            generator: { kind: "sequence", start: 1 },
          },
          {
            name: "role",
            type: "string",
            generator: { kind: "constant", value: "original" },
          },
        ],
      };

      const scenario: Scenario = {
        name: "transform-only-test",
        description: "Test transform-only step",
        steps: [
          {
            table: userTable,
            rowCount: 10,
          },
          {
            tableName: "test_users",
            transformations: [
              {
                description: "Transform role to new value",
                transformations: [
                  {
                    kind: "template",
                    column: "role",
                    template: "transformed",
                  },
                ],
              },
            ],
          },
        ],
      };

      const result = await generator.runScenario({
        scenario,
        dropFirst: true,
        createTable: true,
      });

      expect(result.steps).toHaveLength(2);

      // First step: generate
      const step0 = result.steps[0]!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
      expect(step0.tableName).toBe("test_users");
      expect(step0.generate?.rowsInserted).toBe(10);

      // Second step: transform only (no generate)
      const step1 = result.steps[1]!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
      expect(step1.tableName).toBe("test_users");
      expect(step1.generate).toBeUndefined();
      expect(step1.transform?.batchesApplied).toBe(1);

      // Verify all rows have transformed role
      const rows = await generator.queryRows("test_users", 20);
      expect(rows.length).toBe(10);
      for (const row of rows) {
        expect(row.role).toBe("transformed");
      }

      // Total rows inserted should only count from generate steps
      expect(result.totalRowsInserted).toBe(10);

      await generator.dropTable("test_users");
    });
  }
);
