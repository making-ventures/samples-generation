import postgres, { type Sql } from "postgres";
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
import { escapePostgresIdentifier } from "./escape.js";
import { getLookupTableName } from "./utils.js";

export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  username: string;
  password: string;
}

function columnTypeToPostgres(column: ColumnConfig): string {
  switch (column.type) {
    case "integer":
      return "INTEGER";
    case "bigint":
      return "BIGINT";
    case "float":
      return "DOUBLE PRECISION";
    case "string":
      return "TEXT";
    case "boolean":
      return "BOOLEAN";
    case "datetime":
      return "TIMESTAMP";
    case "date":
      return "DATE";
  }
}

/**
 * Wrap an expression to return NULL with given probability (Postgres)
 */
function wrapWithNullCheck(expr: string, nullProbability: number): string {
  if (nullProbability <= 0) return expr;
  if (nullProbability >= 1) return "NULL";
  return `CASE WHEN random() < ${String(nullProbability)} THEN NULL ELSE ${expr} END`;
}

/**
 * Convert a generator config to a Postgres SQL expression
 */
export function generatorToPostgresExpr(
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
      return `floor(random() * (${String(gen.max)} - ${String(gen.min)} + 1) + ${String(gen.min)})::int`;
    case "randomFloat": {
      const precision = gen.precision ?? 2;
      return `round((random() * (${String(gen.max)} - ${String(gen.min)}) + ${String(gen.min)})::numeric, ${String(precision)})`;
    }
    case "randomString": {
      const len = gen.length;
      // Use md5 for random strings, repeat and substr for length
      return `substr(md5(random()::text || ${seqExpr}::text), 1, ${String(len)})`;
    }
    case "choice": {
      const values = gen.values;
      const arr = values.map((v) =>
        typeof v === "string" ? `'${v}'` : String(v)
      );
      return `(ARRAY[${arr.join(", ")}])[floor(random() * ${String(arr.length)} + 1)::int]`;
    }
    case "choiceByLookup": {
      // Reference the CTE that will be added by generateNative
      const cteName = getLookupTableName(gen.values);
      return `${cteName}.arr[floor(random() * array_length(${cteName}.arr, 1) + 1)::int]`;
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
      return `to_timestamp(${String(fromTs)} + floor(random() * ${String(toTs - fromTs)})::int)`;
    }
    case "uuid":
      return "gen_random_uuid()";
  }
}

export class PostgresDataGenerator extends BaseDataGenerator {
  readonly name = "postgres";
  private sql: Sql | null = null;

  constructor(private config: PostgresConfig) {
    super();
  }

  connect(): Promise<void> {
    this.sql = postgres({
      host: this.config.host,
      port: this.config.port,
      database: this.config.database,
      username: this.config.username,
      password: this.config.password,
    });
    return Promise.resolve();
  }

  async disconnect(): Promise<void> {
    if (this.sql) {
      await this.sql.end();
      this.sql = null;
    }
  }

  private getSql(): Sql {
    if (!this.sql) {
      throw new Error("Not connected to Postgres");
    }
    return this.sql;
  }

  async createTable(table: TableConfig): Promise<void> {
    const sql = this.getSql();
    const columnDefs = table.columns
      .map((col) => {
        const colType = columnTypeToPostgres(col);
        const nullable = col.nullable ? "" : " NOT NULL";
        return `${escapePostgresIdentifier(col.name)} ${colType}${nullable}`;
      })
      .join(", ");
    const escapedTableName = escapePostgresIdentifier(table.name);
    await sql.unsafe(
      `CREATE TABLE IF NOT EXISTS ${escapedTableName} (${columnDefs})`
    );
  }

  async truncateTable(tableName: string): Promise<void> {
    const sql = this.getSql();
    await sql`TRUNCATE TABLE ${sql(tableName)}`;
  }

  async dropTable(tableName: string): Promise<void> {
    const sql = this.getSql();
    await sql`DROP TABLE IF EXISTS ${sql(tableName)}`;
  }

  protected async generateNative(
    table: TableConfig,
    rowCount: number,
    startSequence: number
  ): Promise<void> {
    const sql = this.getSql();
    const escapedTableName = escapePostgresIdentifier(table.name);

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
    const columns = table.columns.map((c) => escapePostgresIdentifier(c.name));
    const seqExpr = `(n + ${String(startSequence - 1)})`;
    const expressions = table.columns.map((col) => {
      let expr = generatorToPostgresExpr(col.generator, seqExpr);
      // Cast to proper type if needed
      if (col.type === "integer") expr = `(${expr})::integer`;
      else if (col.type === "bigint") expr = `(${expr})::bigint`;
      else if (col.type === "boolean") expr = `(${expr})::boolean`;
      // Apply null probability if specified
      if (col.nullable && col.nullProbability && col.nullProbability > 0) {
        expr = wrapWithNullCheck(expr, col.nullProbability);
      }
      return expr;
    });

    // Build CTE prefix if we have lookup tables
    const ctePrefix =
      lookupCtes.length > 0 ? `WITH ${lookupCtes.join(", ")} ` : "";
    // Cross join with lookup CTEs to make them available
    const lookupJoins =
      lookupCtes.length > 0
        ? ", " +
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
          ].join(", ")
        : "";

    const insertSql = `
      ${ctePrefix}INSERT INTO ${escapedTableName} (${columns.join(", ")})
      SELECT ${expressions.join(", ")}
      FROM generate_series(1, ${String(rowCount)}) AS n${lookupJoins}
    `;

    await sql.unsafe(insertSql);
  }

  async queryRows(tableName: string, limit = 100): Promise<GeneratedRow[]> {
    const sql = this.getSql();
    const result = await sql`SELECT * FROM ${sql(tableName)} LIMIT ${limit}`;
    return result as GeneratedRow[];
  }

  async countRows(tableName: string): Promise<number> {
    const sql = this.getSql();
    const result = await sql`SELECT COUNT(*) as count FROM ${sql(tableName)}`;
    const firstRow = result[0] as { count: string } | undefined;
    if (!firstRow) {
      return 0;
    }
    return Number(firstRow.count);
  }

  async getMaxValue(
    tableName: string,
    columnName: string
  ): Promise<number | null> {
    const sql = this.getSql();
    const result =
      await sql`SELECT MAX(${sql(columnName)}) as max_val FROM ${sql(tableName)}`;
    const firstRow = result[0] as { max_val: string | null } | undefined;
    if (!firstRow?.max_val) {
      return null;
    }
    return Number(firstRow.max_val);
  }

  async getTableSize(tableName: string): Promise<number | null> {
    const sql = this.getSql();
    const result =
      await sql`SELECT pg_total_relation_size(${tableName}::regclass) as size`;
    const firstRow = result[0] as { size: string } | undefined;
    if (!firstRow) {
      return null;
    }
    return Number(firstRow.size);
  }

  async optimize(tableName: string): Promise<void> {
    const sql = this.getSql();
    // VACUUM reclaims storage and ANALYZE updates statistics
    await sql.unsafe(`VACUUM ANALYZE ${escapePostgresIdentifier(tableName)}`);
  }

  protected async applyTransformations(
    tableName: string,
    transformations: Transformation[]
  ): Promise<void> {
    if (transformations.length === 0) return;

    const sql = this.getSql();

    const escapedTable = escapePostgresIdentifier(tableName);
    const setClauses: string[] = [];

    for (const t of transformations) {
      switch (t.kind) {
        case "template": {
          const escapedCol = escapePostgresIdentifier(t.column);
          // Replace {column_name} with column references
          let expr = `'${t.template.replace(/'/g, "''")}'`;
          const refs = t.template.match(/\{([^}]+)\}/g) ?? [];
          for (const ref of refs) {
            const colName = ref.slice(1, -1);
            const colRef = escapePostgresIdentifier(colName);
            // Replace {col} with concatenation: ... || col || ...
            expr = expr.replace(`'{${colName}}'`, `' || ${colRef} || '`);
            expr = expr.replace(`{${colName}}`, `' || ${colRef} || '`);
          }
          // Clean up empty string concatenations
          expr = expr.replace(/^'' \|\| /, "").replace(/ \|\| ''$/, "");
          expr = expr.replace(/' \|\| '' \|\| '/g, "' || '");
          if (t.lowercase) {
            expr = `lower(${expr})`;
          }
          setClauses.push(`${escapedCol} = ${expr}`);
          break;
        }
        case "mutate": {
          const escapedCol = escapePostgresIdentifier(t.column);
          // Random string mutation
          const { probability, operations } = t;

          // Build mutation expressions for each operation
          const mutationExprs: Record<MutationOperation, string> = {
            replace: `overlay(${escapedCol} placing 'X' from (floor(random() * length(${escapedCol})) + 1)::int for 1)`,
            delete: `left(${escapedCol}, (floor(random() * length(${escapedCol})))::int) || right(${escapedCol}, length(${escapedCol}) - (floor(random() * length(${escapedCol})) + 1)::int)`,
            insert: `left(${escapedCol}, (floor(random() * length(${escapedCol})))::int) || 'X' || right(${escapedCol}, length(${escapedCol}) - (floor(random() * length(${escapedCol})))::int)`,
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
          const escapedCol = escapePostgresIdentifier(t.column);
          // Lookup value from another table via join
          // PostgreSQL supports UPDATE ... FROM ... WHERE syntax
          const fromTable = escapePostgresIdentifier(t.fromTable);
          const fromCol = escapePostgresIdentifier(t.fromColumn);
          const targetJoinCol = escapePostgresIdentifier(t.joinOn.targetColumn);
          const lookupJoinCol = escapePostgresIdentifier(t.joinOn.lookupColumn);

          // Use subquery for simplicity (works with multiple lookups in same batch)
          setClauses.push(
            `${escapedCol} = (SELECT ${fromCol} FROM ${fromTable} WHERE ${lookupJoinCol} = ${escapedTable}.${targetJoinCol})`
          );
          break;
        }
        case "swap": {
          // Swap handled separately - needs both columns in one UPDATE
          // Skip here, will be handled after the loop
          break;
        }
      }
    }

    // Handle swap transformations separately (need atomic swap with same random)
    const swapTransformations = transformations.filter(
      (t): t is SwapTransformation => t.kind === "swap"
    );

    for (const swap of swapTransformations) {
      const col1 = escapePostgresIdentifier(swap.column1);
      const col2 = escapePostgresIdentifier(swap.column2);
      const prob = String(swap.probability);

      // Use a subquery to ensure same random value for both columns
      // PostgreSQL evaluates SET expressions using old values, so a = b, b = a works
      const swapSql = `
        UPDATE ${escapedTable} SET
          ${col1} = ${col2},
          ${col2} = ${col1}
        WHERE random() < ${prob}
      `;
      await sql.unsafe(swapSql);
    }

    if (setClauses.length === 0) return;

    const updateSql = `UPDATE ${escapedTable} SET ${setClauses.join(", ")}`;
    await sql.unsafe(updateSql);
  }
}
