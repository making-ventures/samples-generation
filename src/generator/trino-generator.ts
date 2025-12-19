import { BasicAuth, Trino } from "trino-client";
import type {
  TableConfig,
  GeneratedRow,
  ColumnConfig,
  GeneratorConfig,
  ChoiceByLookupGenerator,
  Transformation,
  MutationOperation,
  SwapTransformation,
} from "./types.js";
import { BaseDataGenerator } from "./base-generator.js";
import { escapeTrinoIdentifier } from "./escape.js";
import { getLookupTableName } from "./utils.js";

export interface TrinoConfig {
  host: string;
  port: number;
  catalog: string;
  schema: string;
  user: string;
}

function columnTypeToTrino(column: ColumnConfig): string {
  switch (column.type) {
    case "integer":
      return "INTEGER";
    case "bigint":
      return "BIGINT";
    case "float":
      return "DOUBLE";
    case "string":
      return "VARCHAR";
    case "boolean":
      return "BOOLEAN";
    case "datetime":
      return "TIMESTAMP";
    case "date":
      return "DATE";
  }
}

/**
 * Wrap an expression to return NULL with given probability (Trino)
 */
function wrapWithNullCheck(expr: string, nullProbability: number): string {
  if (nullProbability <= 0) return expr;
  if (nullProbability >= 1) return "NULL";
  return `CASE WHEN random() < ${String(nullProbability)} THEN NULL ELSE ${expr} END`;
}

/**
 * Convert a generator config to a Trino SQL expression
 * Trino uses 'n' as the row number from UNNEST(sequence(...))
 */
export function generatorToTrinoExpr(
  gen: GeneratorConfig,
  seqExpr: string
): string {
  switch (gen.kind) {
    case "sequence": {
      const start = gen.start ?? 1;
      const step = gen.step ?? 1;
      return `(${String(start)} - 1 + ${seqExpr} * ${String(step)})`;
    }
    case "randomInt":
      // Trino random() returns 0.0 to 1.0
      return `CAST(floor(random() * ${String(gen.max - gen.min + 1)} + ${String(gen.min)}) AS INTEGER)`;
    case "randomFloat": {
      const precision = gen.precision ?? 2;
      return `round(random() * ${String(gen.max - gen.min)} + ${String(gen.min)}, ${String(precision)})`;
    }
    case "randomString": {
      const len = gen.length;
      // Trino doesn't have md5 on random, use substr of uuid
      return `substr(replace(cast(uuid() as varchar), '-', ''), 1, ${String(len)})`;
    }
    case "choice": {
      const values = gen.values;
      const arr = values.map((v) =>
        typeof v === "string" ? `'${v}'` : String(v)
      );
      // Use element_at with 1-based index
      return `element_at(ARRAY[${arr.join(", ")}], CAST(floor(random() * ${String(arr.length)}) + 1 AS INTEGER))`;
    }
    case "choiceByLookup": {
      // Reference the CTE that will be added by generateNative
      const cteName = getLookupTableName(gen.values);
      return `element_at(${cteName}.arr, CAST(floor(random() * cardinality(${cteName}.arr)) + 1 AS INTEGER))`;
    }
    case "constant": {
      const val = gen.value;
      return typeof val === "string" ? `'${val}'` : String(val);
    }
    case "datetime": {
      const from = gen.from ?? new Date("2020-01-01");
      const to = gen.to ?? new Date();
      const fromTs = Math.floor(from.getTime() / 1000);
      const toTs = Math.floor(to.getTime() / 1000);
      // from_unixtime returns timestamp
      return `from_unixtime(${String(fromTs)} + CAST(floor(random() * ${String(toTs - fromTs)}) AS BIGINT))`;
    }
    case "uuid":
      return "uuid()";
  }
}

export class TrinoDataGenerator extends BaseDataGenerator {
  readonly name = "trino";
  private trino: Trino | null = null;
  private escapedCatalog: string;
  private escapedSchema: string;

  constructor(private config: TrinoConfig) {
    super();
    this.escapedCatalog = escapeTrinoIdentifier(config.catalog);
    this.escapedSchema = escapeTrinoIdentifier(config.schema);
  }

  private get fullSchemaPath(): string {
    return `${this.escapedCatalog}.${this.escapedSchema}`;
  }

  async connect(): Promise<void> {
    this.trino = Trino.create({
      server: `http://${this.config.host}:${String(this.config.port)}`,
      catalog: this.config.catalog,
      schema: this.config.schema,
      auth: new BasicAuth(this.config.user),
    });

    // Ensure schema exists
    const createSchema = await this.trino.query(
      `CREATE SCHEMA IF NOT EXISTS ${this.fullSchemaPath}`
    );
    for await (const _ of createSchema) {
      // consume iterator
    }
  }

  disconnect(): Promise<void> {
    this.trino = null;
    return Promise.resolve();
  }

  private getTrino(): Trino {
    if (!this.trino) {
      throw new Error("Not connected to Trino");
    }
    return this.trino;
  }

  private fullTableName(tableName: string): string {
    return `${this.fullSchemaPath}.${escapeTrinoIdentifier(tableName)}`;
  }

  async createTable(table: TableConfig): Promise<void> {
    const trino = this.getTrino();
    const columns = table.columns
      .map((col) => {
        const nullable = col.nullable ? "" : " NOT NULL";
        return `${escapeTrinoIdentifier(col.name)} ${columnTypeToTrino(col)}${nullable}`;
      })
      .join(", ");

    const query = await trino.query(
      `CREATE TABLE IF NOT EXISTS ${this.fullTableName(table.name)} (${columns}) WITH (format = 'PARQUET')`
    );
    for await (const result of query) {
      const trinoResult = result as { error?: { message: string } };
      if (trinoResult.error) {
        throw new Error(
          `Trino createTable failed: ${trinoResult.error.message}`
        );
      }
    }
  }

  async truncateTable(tableName: string): Promise<void> {
    const trino = this.getTrino();
    const query = await trino.query(
      `DELETE FROM ${this.fullTableName(tableName)}`
    );
    for await (const result of query) {
      const trinoResult = result as { error?: { message: string } };
      if (trinoResult.error) {
        throw new Error(
          `Trino truncateTable failed: ${trinoResult.error.message}`
        );
      }
    }
  }

  async dropTable(tableName: string): Promise<void> {
    const trino = this.getTrino();
    const query = await trino.query(
      `DROP TABLE IF EXISTS ${this.fullTableName(tableName)}`
    );
    for await (const result of query) {
      // Check for errors in Trino response
      const trinoResult = result as { error?: { message: string } };
      if (trinoResult.error) {
        throw new Error(`Trino query failed: ${trinoResult.error.message}`);
      }
    }
  }

  protected async generateNative(
    table: TableConfig,
    rowCount: number,
    startSequence: number
  ): Promise<void> {
    const trino = this.getTrino();

    // Collect choiceByLookup generators to create CTEs
    const lookupCtes: string[] = [];
    for (const col of table.columns) {
      if (col.generator.kind === "choiceByLookup") {
        const gen = col.generator;
        const cteName = getLookupTableName(gen.values);
        const valuesLiteral = gen.values
          .map((v) => `'${v.replace(/'/g, "''")}'`)
          .join(", ");
        lookupCtes.push(
          `${cteName} AS (SELECT ARRAY[${valuesLiteral}] AS arr)`
        );
      }
    }

    // Build column list and expressions
    const columns = table.columns.map((c) => escapeTrinoIdentifier(c.name));
    // Trino sequence() has a 10,000 entry limit
    // Use 3-level CROSS JOIN to support up to 1 trillion rows:
    // - level1: 0 to numChunks - 1 (each chunk = 100M rows)
    // - level2: 0 to 9999 (10k values)
    // - level3: 1 to 10000 (10k values)
    // Row number = level1 * 100M + level2 * 10k + level3
    const SEQUENCE_LIMIT = 10_000; // Trino's hard limit per sequence
    const ROWS_PER_CHUNK = SEQUENCE_LIMIT * SEQUENCE_LIMIT; // 100M
    const numChunks = Math.ceil(rowCount / ROWS_PER_CHUNK);

    // The row number expression combines all three levels
    const seqExpr = `(${String(startSequence - 1)} + level1 * ${String(ROWS_PER_CHUNK)} + level2 * ${String(SEQUENCE_LIMIT)} + level3)`;
    const expressions = table.columns.map((col) => {
      let expr = generatorToTrinoExpr(col.generator, seqExpr);
      // Apply null probability if specified
      if (col.nullable && col.nullProbability && col.nullProbability > 0) {
        expr = wrapWithNullCheck(expr, col.nullProbability);
      }
      return expr;
    });

    // Build WITH clause and cross joins for lookup CTEs
    const ctePrefix =
      lookupCtes.length > 0 ? `WITH ${lookupCtes.join(", ")} ` : "";
    const lookupJoins =
      lookupCtes.length > 0
        ? " CROSS JOIN " +
          [
            ...new Set(
              table.columns
                .filter((c) => c.generator.kind === "choiceByLookup")
                .map((c) =>
                  getLookupTableName(
                    (c.generator as ChoiceByLookupGenerator).values
                  )
                )
            ),
          ].join(" CROSS JOIN ")
        : "";

    const insertSql = `
      INSERT INTO ${this.fullTableName(table.name)} (${columns.join(", ")})
      ${ctePrefix}SELECT ${expressions.join(", ")}
      FROM UNNEST(sequence(0, ${String(numChunks - 1)})) AS t1(level1)
      CROSS JOIN UNNEST(sequence(0, ${String(SEQUENCE_LIMIT - 1)})) AS t2(level2)
      CROSS JOIN UNNEST(sequence(1, ${String(SEQUENCE_LIMIT)})) AS t3(level3)${lookupJoins}
      WHERE (level1 * ${String(ROWS_PER_CHUNK)} + level2 * ${String(SEQUENCE_LIMIT)} + level3) <= ${String(rowCount)}
    `;

    const query = await trino.query(insertSql);
    for await (const result of query) {
      const trinoResult = result as { error?: { message: string } };
      if (trinoResult.error) {
        throw new Error(`Trino insert failed: ${trinoResult.error.message}`);
      }
    }
  }

  async queryRows(tableName: string, limit = 100): Promise<GeneratedRow[]> {
    const trino = this.getTrino();
    const query = await trino.query(
      `SELECT * FROM ${this.fullTableName(tableName)} LIMIT ${String(limit)}`
    );

    const results: GeneratedRow[] = [];
    for await (const result of query) {
      const trinoResult = result as {
        columns?: { name: string }[];
        data?: unknown[][];
      };
      if (trinoResult.data && trinoResult.columns) {
        for (const row of trinoResult.data) {
          const obj: GeneratedRow = {};
          trinoResult.columns.forEach((col, i) => {
            obj[col.name] = row[i];
          });
          results.push(obj);
        }
      }
    }
    return results;
  }

  async countRows(tableName: string): Promise<number> {
    const trino = this.getTrino();
    const query = await trino.query(
      `SELECT COUNT(*) as count FROM ${this.fullTableName(tableName)}`
    );

    for await (const result of query) {
      const trinoResult = result as { data?: unknown[][] };
      const firstRow = trinoResult.data?.[0];
      if (firstRow && firstRow.length > 0) {
        return Number(firstRow[0]);
      }
    }
    return 0;
  }

  async getMaxValue(
    tableName: string,
    columnName: string
  ): Promise<number | null> {
    const trino = this.getTrino();
    const query = await trino.query(
      `SELECT MAX(${escapeTrinoIdentifier(columnName)}) as max_val FROM ${this.fullTableName(tableName)}`
    );

    for await (const result of query) {
      const trinoResult = result as { data?: unknown[][] };
      const firstRow = trinoResult.data?.[0];
      if (firstRow && firstRow.length > 0 && firstRow[0] !== null) {
        return Number(firstRow[0]);
      }
    }
    return null;
  }

  async getTableSize(tableName: string): Promise<number | null> {
    const trino = this.getTrino();
    // Query the $files metadata table to get total file sizes
    // The $files suffix must be part of the quoted table name
    const filesTableName = escapeTrinoIdentifier(`${tableName}$files`);
    const query = await trino.query(
      `SELECT COALESCE(SUM(file_size_in_bytes), 0) as size FROM ${this.fullSchemaPath}.${filesTableName}`
    );

    for await (const result of query) {
      const trinoResult = result as { data?: unknown[][] };
      const firstRow = trinoResult.data?.[0];
      if (firstRow && firstRow.length > 0) {
        return Number(firstRow[0]);
      }
    }
    return null;
  }

  async optimize(tableName: string): Promise<void> {
    const trino = this.getTrino();
    const fullTableName = `${this.fullSchemaPath}.${escapeTrinoIdentifier(tableName)}`;

    // Compact small files into larger ones for better read performance
    const rewriteQuery = await trino.query(
      `ALTER TABLE ${fullTableName} EXECUTE rewrite_data_files(min_file_size_bytes => 10485760)`
    );
    // Consume the iterator to complete the query
    for await (const _ of rewriteQuery) {
      // no-op
    }

    // Remove old snapshots older than 1 day to reclaim storage
    const expireQuery = await trino.query(
      `ALTER TABLE ${fullTableName} EXECUTE expire_snapshots(retention_threshold => '1d')`
    );
    for await (const _ of expireQuery) {
      // no-op
    }

    // Remove orphan files not referenced by any snapshot
    const orphanQuery = await trino.query(
      `ALTER TABLE ${fullTableName} EXECUTE remove_orphan_files(retention_threshold => '1d')`
    );
    for await (const _ of orphanQuery) {
      // no-op
    }
  }

  protected async applyTransformations(
    tableName: string,
    transformations: Transformation[]
  ): Promise<void> {
    if (transformations.length === 0) return;

    const trino = this.getTrino();
    const fullTableName = `${this.fullSchemaPath}.${escapeTrinoIdentifier(tableName)}`;
    const setClauses: string[] = [];

    for (const t of transformations) {
      switch (t.kind) {
        case "template": {
          const escapedCol = escapeTrinoIdentifier(t.column);
          // Replace {column_name} with column references
          let expr = `'${t.template.replace(/'/g, "''")}'`;
          const refs = t.template.match(/\{([^}]+)\}/g) ?? [];
          for (const ref of refs) {
            const colName = ref.slice(1, -1);
            const colRef = escapeTrinoIdentifier(colName);
            // Replace {col} with concat function
            expr = expr.replace(
              `'{${colName}}'`,
              `', cast(${colRef} as varchar), '`
            );
            expr = expr.replace(
              `{${colName}}`,
              `', cast(${colRef} as varchar), '`
            );
          }
          // Wrap in concat and clean up
          expr = `concat(${expr})`;
          expr = expr.replace(/concat\('',\s*/g, "concat(");
          expr = expr.replace(/,\s*''\)/g, ")");
          expr = expr.replace(/concat\('([^']+)'\)/g, "'$1'"); // Single literal doesn't need concat
          if (t.lowercase) {
            expr = `lower(${expr})`;
          }
          setClauses.push(`${escapedCol} = ${expr}`);
          break;
        }
        case "mutate": {
          const escapedCol = escapeTrinoIdentifier(t.column);
          // Random string mutation using Trino functions
          const { probability, operations } = t;
          // Trino random() returns double 0-1
          const randomPos = `cast(floor(random() * length(${escapedCol})) + 1 as integer)`;

          // Build mutation expressions for each operation
          const mutationExprs: Record<MutationOperation, string> = {
            replace: `concat(substr(${escapedCol}, 1, ${randomPos} - 1), 'X', substr(${escapedCol}, ${randomPos} + 1))`,
            delete: `concat(substr(${escapedCol}, 1, ${randomPos} - 1), substr(${escapedCol}, ${randomPos} + 1))`,
            insert: `concat(substr(${escapedCol}, 1, ${randomPos}), 'X', substr(${escapedCol}, ${randomPos} + 1))`,
          };

          // Build expression that randomly picks from available operations
          let mutateExpr: string;
          if (operations.length === 1) {
            // We know operations[0] exists since length === 1
            mutateExpr = mutationExprs[operations[0]!]; // eslint-disable-line @typescript-eslint/no-non-null-assertion
          } else {
            // Use CASE with random() to pick operation
            const cases = operations.map((op, i) => {
              const threshold = (i + 1) / operations.length;
              if (i === operations.length - 1) {
                return `ELSE ${mutationExprs[op]}`;
              }
              return `WHEN random() < ${String(threshold)} THEN ${mutationExprs[op]}`;
            });
            mutateExpr = `(CASE ${cases.join(" ")} END)`;
          }

          setClauses.push(
            `${escapedCol} = CASE WHEN random() < ${String(probability)} THEN ${mutateExpr} ELSE ${escapedCol} END`
          );
          break;
        }
        case "lookup": {
          const escapedCol = escapeTrinoIdentifier(t.column);
          // Lookup value from another table via join
          // Trino uses correlated subquery
          const lookupSchema = escapeTrinoIdentifier(this.config.schema);
          const fromTable = escapeTrinoIdentifier(t.fromTable);
          const fromCol = escapeTrinoIdentifier(t.fromColumn);
          const targetJoinCol = escapeTrinoIdentifier(t.joinOn.targetColumn);
          const lookupJoinCol = escapeTrinoIdentifier(t.joinOn.lookupColumn);
          const fullLookupTable = `${this.escapedCatalog}.${lookupSchema}.${fromTable}`;

          setClauses.push(
            `${escapedCol} = (SELECT ${fromCol} FROM ${fullLookupTable} WHERE ${lookupJoinCol} = ${fullTableName}.${targetJoinCol})`
          );
          break;
        }
        case "swap": {
          // Swap handled separately - needs both columns in one UPDATE with same random
          break;
        }
      }
    }

    // Handle swap transformations separately (need atomic swap with same random)
    const swapTransformations = transformations.filter(
      (t): t is SwapTransformation => t.kind === "swap"
    );

    for (const swap of swapTransformations) {
      const col1 = escapeTrinoIdentifier(swap.column1);
      const col2 = escapeTrinoIdentifier(swap.column2);
      const prob = String(swap.probability);

      // Trino: SET a = b, b = a works atomically (uses old values on right side)
      const swapSql = `
        UPDATE ${fullTableName} SET
          ${col1} = ${col2},
          ${col2} = ${col1}
        WHERE random() < ${prob}
      `;
      const swapResult = await trino.query(swapSql);
      for await (const _ of swapResult) {
        // consume result
      }
    }

    if (setClauses.length === 0) return;

    const updateSql = `UPDATE ${fullTableName} SET ${setClauses.join(", ")}`;
    const result = await trino.query(updateSql);
    for await (const _ of result) {
      // consume result
    }
  }
}
