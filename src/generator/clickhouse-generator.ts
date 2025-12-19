import { createClient, type ClickHouseClient } from "@clickhouse/client";
import type {
  TableConfig,
  GeneratedRow,
  ColumnConfig,
  GeneratorConfig,
  Transformation,
  MutationOperation,
  LookupTransformation,
  SwapTransformation,
} from "./types.js";
import { BaseDataGenerator } from "./base-generator.js";
import { escapeClickHouseIdentifier } from "./escape.js";
import { getLookupTableName } from "./utils.js";

export interface ClickHouseConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
}

function columnTypeToClickHouse(column: ColumnConfig): string {
  const baseType = ((): string => {
    switch (column.type) {
      case "integer":
        return "Int32";
      case "bigint":
        return "Int64";
      case "float":
        return "Float64";
      case "string":
        return "String";
      case "boolean":
        return "Bool";
      case "datetime":
        return "DateTime";
      case "date":
        return "Date";
    }
  })();
  return column.nullable ? `Nullable(${baseType})` : baseType;
}

/**
 * Wrap an expression to return NULL with given probability (ClickHouse)
 */
function wrapWithNullCheck(expr: string, nullProbability: number): string {
  if (nullProbability <= 0) return expr;
  if (nullProbability >= 1) return "NULL";
  // rand() returns UInt32 (0 to 4294967295), divide to get 0-1 range
  return `if(rand() / 4294967295.0 < ${String(nullProbability)}, NULL, ${expr})`;
}

/**
 * Convert a generator config to a ClickHouse SQL expression
 */
export function generatorToClickHouseExpr(
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
      return `toInt32(randUniform(${String(gen.min)}, ${String(gen.max + 1)}))`;
    case "randomFloat": {
      const precision = gen.precision ?? 2;
      return `round(randUniform(${String(gen.min)}, ${String(gen.max)}), ${String(precision)})`;
    }
    case "randomString": {
      const len = gen.length;
      return `randomPrintableASCII(${String(len)})`;
    }
    case "choice": {
      const values = gen.values;
      const arr = values.map((v) =>
        typeof v === "string" ? `'${v}'` : String(v)
      );
      return `[${arr.join(", ")}][toUInt32(rand() % ${String(arr.length)}) + 1]`;
    }
    case "choiceByLookup": {
      // For ClickHouse, we'll use a subquery that loads the array once
      // The CTE approach doesn't work well, so we use arrayElement
      const cteName = getLookupTableName(gen.values);
      return `${cteName}_arr[toUInt32(rand() % length(${cteName}_arr)) + 1]`;
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
      return `toDateTime(${String(fromTs)} + rand() % ${String(toTs - fromTs)})`;
    }
    case "uuid":
      return "generateUUIDv4()";
  }
}

export class ClickHouseDataGenerator extends BaseDataGenerator {
  readonly name = "clickhouse";
  private client: ClickHouseClient | null = null;

  constructor(private config: ClickHouseConfig) {
    super();
  }

  connect(): Promise<void> {
    this.client = createClient({
      url: `http://${this.config.host}:${String(this.config.port)}`,
      username: this.config.username,
      password: this.config.password,
      database: this.config.database,
      // Increase timeout for large data generation (6 hours for 10B+ rows)
      request_timeout: 21_600_000,
    });
    return Promise.resolve();
  }

  async disconnect(): Promise<void> {
    if (this.client) {
      await this.client.close();
      this.client = null;
    }
  }

  private getClient(): ClickHouseClient {
    if (!this.client) {
      throw new Error("Not connected to ClickHouse");
    }
    return this.client;
  }

  async createTable(table: TableConfig): Promise<void> {
    const client = this.getClient();
    const columns = table.columns
      .map(
        (col) =>
          `${escapeClickHouseIdentifier(col.name)} ${columnTypeToClickHouse(col)}`
      )
      .join(", ");

    // Find the first column for ORDER BY (ClickHouse requires it for MergeTree)
    const firstColumn = table.columns[0];
    if (!firstColumn) {
      throw new Error("Table must have at least one column");
    }
    const orderByColumn = escapeClickHouseIdentifier(firstColumn.name);
    const escapedTableName = escapeClickHouseIdentifier(table.name);

    await client.command({
      query: `CREATE TABLE IF NOT EXISTS ${escapedTableName} (${columns}) ENGINE = MergeTree() ORDER BY ${orderByColumn}`,
    });
  }

  async truncateTable(tableName: string): Promise<void> {
    const client = this.getClient();
    await client.command({
      query: `TRUNCATE TABLE ${escapeClickHouseIdentifier(tableName)}`,
    });
  }

  async dropTable(tableName: string): Promise<void> {
    const client = this.getClient();
    await client.command({
      query: `DROP TABLE IF EXISTS ${escapeClickHouseIdentifier(tableName)}`,
    });
  }

  protected async generateNative(
    table: TableConfig,
    rowCount: number,
    startSequence: number
  ): Promise<void> {
    const client = this.getClient();
    const escapedTableName = escapeClickHouseIdentifier(table.name);

    // Collect choiceByLookup arrays for WITH clause
    // ClickHouse syntax: WITH [values] AS name
    const arrayDefs: string[] = [];
    for (const col of table.columns) {
      if (col.generator.kind === "choiceByLookup") {
        const gen = col.generator;
        const arrName = `${getLookupTableName(gen.values)}_arr`;
        const valuesLiteral = gen.values
          .map((v) => `'${v.replace(/'/g, "\\'")}'`)
          .join(", ");
        arrayDefs.push(`[${valuesLiteral}] AS ${arrName}`);
      }
    }

    // Build column list and expressions
    const columns = table.columns.map((c) =>
      escapeClickHouseIdentifier(c.name)
    );
    const seqExpr = `(number + ${String(startSequence)})`;
    const expressions = table.columns.map((col) => {
      let expr = generatorToClickHouseExpr(col.generator, seqExpr);
      // Apply null probability if specified
      if (col.nullable && col.nullProbability && col.nullProbability > 0) {
        expr = wrapWithNullCheck(expr, col.nullProbability);
      }
      return expr;
    });

    // Build WITH clause if we have lookup arrays
    const withClause = arrayDefs.length > 0 ? `${arrayDefs.join(", ")} ` : "";

    const insertSql = `
      INSERT INTO ${escapedTableName} (${columns.join(", ")})
      ${withClause.length > 0 ? `WITH ${withClause}` : ""}SELECT ${expressions.join(", ")}
      FROM numbers(${String(rowCount)})
    `;

    await client.command({ query: insertSql });
  }

  async queryRows(tableName: string, limit = 100): Promise<GeneratedRow[]> {
    const client = this.getClient();
    const result = await client.query({
      query: `SELECT * FROM ${escapeClickHouseIdentifier(tableName)} LIMIT {limit:UInt32}`,
      query_params: { limit },
      format: "JSONEachRow",
    });
    return result.json();
  }

  async countRows(tableName: string): Promise<number> {
    const client = this.getClient();
    const result = await client.query({
      query: `SELECT COUNT(*) as count FROM ${escapeClickHouseIdentifier(tableName)}`,
      format: "JSONEachRow",
    });
    const rows: { count: string }[] = await result.json();
    const firstRow = rows[0];
    if (!firstRow) {
      return 0;
    }
    return Number(firstRow.count);
  }

  async getMaxValue(
    tableName: string,
    columnName: string
  ): Promise<number | null> {
    const client = this.getClient();
    const result = await client.query({
      query: `SELECT MAX(${escapeClickHouseIdentifier(columnName)}) as max_val FROM ${escapeClickHouseIdentifier(tableName)}`,
      format: "JSONEachRow",
    });
    const rows: { max_val: string | null }[] = await result.json();
    const firstRow = rows[0];
    if (!firstRow?.max_val) {
      return null;
    }
    return Number(firstRow.max_val);
  }

  async getTableSize(tableName: string): Promise<number | null> {
    const client = this.getClient();
    const result = await client.query({
      query: `SELECT total_bytes FROM system.tables WHERE database = {database:String} AND name = {table:String}`,
      query_params: { database: this.config.database, table: tableName },
      format: "JSONEachRow",
    });
    const rows: { total_bytes: string }[] = await result.json();
    const firstRow = rows[0];
    if (!firstRow) {
      return null;
    }
    return Number(firstRow.total_bytes);
  }

  async optimize(tableName: string): Promise<void> {
    const client = this.getClient();
    // OPTIMIZE TABLE FINAL merges all parts into one for MergeTree engines
    await client.command({
      query: `OPTIMIZE TABLE ${escapeClickHouseIdentifier(tableName)} FINAL`,
      clickhouse_settings: {
        wait_end_of_query: 1,
      },
    });
  }

  protected async applyTransformations(
    tableName: string,
    transformations: Transformation[]
  ): Promise<void> {
    if (transformations.length === 0) return;

    const client = this.getClient();
    const escapedTable = escapeClickHouseIdentifier(tableName);
    const setClauses: string[] = [];

    for (const t of transformations) {
      switch (t.kind) {
        case "template": {
          const escapedCol = escapeClickHouseIdentifier(t.column);
          // Replace {column_name} with column references
          let expr = `'${t.template.replace(/'/g, "\\'")}'`;
          const refs = t.template.match(/\{([^}]+)\}/g) ?? [];
          for (const ref of refs) {
            const colName = ref.slice(1, -1);
            const colRef = escapeClickHouseIdentifier(colName);
            // Replace {col} with concatenation
            expr = expr.replace(
              `'{${colName}}'`,
              `' || toString(${colRef}) || '`
            );
            expr = expr.replace(
              `{${colName}}`,
              `' || toString(${colRef}) || '`
            );
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
          const escapedCol = escapeClickHouseIdentifier(t.column);
          // Random string mutation using ClickHouse functions
          const { probability, operations } = t;
          // rand() returns UInt32, divide to get 0-1
          const randomPos = `toUInt32(floor(rand() / 4294967295.0 * length(${escapedCol}))) + 1`;

          // Build mutation expressions for each operation
          const mutationExprs: Record<MutationOperation, string> = {
            replace: `concat(substring(${escapedCol}, 1, ${randomPos} - 1), 'X', substring(${escapedCol}, ${randomPos} + 1))`,
            delete: `concat(substring(${escapedCol}, 1, ${randomPos} - 1), substring(${escapedCol}, ${randomPos} + 1))`,
            insert: `concat(substring(${escapedCol}, 1, ${randomPos}), 'X', substring(${escapedCol}, ${randomPos} + 1))`,
          };

          // Build expression that randomly picks from available operations
          let mutateExpr: string;
          if (operations.length === 1) {
            // We know operations[0] exists since length === 1
            mutateExpr = mutationExprs[operations[0]!]; // eslint-disable-line @typescript-eslint/no-non-null-assertion
          } else {
            // Use multiIf with rand() to pick operation
            const conditions: string[] = [];
            for (let i = 0; i < operations.length - 1; i++) {
              const threshold = (i + 1) / operations.length;
              // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
              const op = operations[i]!;
              conditions.push(
                `rand() / 4294967295.0 < ${String(threshold)}, ${mutationExprs[op]}`
              );
            }
            // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
            const lastOp = operations[operations.length - 1]!;
            conditions.push(mutationExprs[lastOp]);
            mutateExpr = `multiIf(${conditions.join(", ")})`;
          }

          setClauses.push(
            `${escapedCol} = if(rand() / 4294967295.0 < ${String(probability)}, ${mutateExpr}, ${escapedCol})`
          );
          break;
        }
        case "lookup": {
          // ClickHouse ALTER TABLE UPDATE does not support correlated subqueries
          // We need to use the table swap approach: CREATE AS + INSERT SELECT + RENAME
          // Handle lookup transformations separately
          await this.applyLookupTransformation(tableName, t);
          // Don't add to setClauses - already handled
          break;
        }
        case "swap": {
          // Swap handled separately via applySwapTransformation
          // ClickHouse evaluates each rand() separately, so we need table swap approach
          // to ensure same random value is used for both columns
          break;
        }
      }
    }

    // Handle swap transformations separately for ClickHouse (need same random for both columns)
    const swapTransformations = transformations.filter(
      (t): t is SwapTransformation => t.kind === "swap"
    );

    if (swapTransformations.length > 0) {
      // Batch all swaps into a single table swap operation
      await this.applySwapTransformations(tableName, swapTransformations);
    }

    // Only run ALTER TABLE UPDATE if there are SET clauses
    if (setClauses.length === 0) return;

    // ClickHouse uses ALTER TABLE UPDATE syntax
    const updateSql = `ALTER TABLE ${escapedTable} UPDATE ${setClauses.join(", ")} WHERE 1`;
    await client.command({
      query: updateSql,
      clickhouse_settings: {
        wait_end_of_query: 1,
        mutations_sync: "2", // Wait for all replicas
      },
    });
  }

  /**
   * Apply multiple swap transformations in a single table swap operation.
   * Each swap gets its own random value to ensure independent swap decisions.
   */
  private async applySwapTransformations(
    tableName: string,
    swaps: SwapTransformation[]
  ): Promise<void> {
    const client = this.getClient();
    const escapedTable = escapeClickHouseIdentifier(tableName);

    // Create unique suffix for temp tables
    const uniqueSuffix = `${String(Date.now())}_${Math.random().toString(36).slice(2, 8)}`;
    const tempTable = escapeClickHouseIdentifier(
      `${tableName}_swap_temp_${uniqueSuffix}`
    );
    const oldTable = escapeClickHouseIdentifier(
      `${tableName}_swap_old_${uniqueSuffix}`
    );

    // Get table structure
    const structResult = await client.query({
      query: `SHOW CREATE TABLE ${escapedTable}`,
      format: "TabSeparated",
    });
    // TabSeparated returns escaped newlines as literal \n, need to unescape
    const createStatement = (await structResult.text())
      .replace(/\\n/g, "\n")
      .replace(/\\t/g, "\t");

    // Create temp table with same structure
    // SHOW CREATE returns "CREATE TABLE database.tablename" - replace the full table reference
    const tempCreateSql = createStatement.replace(
      /CREATE TABLE [^\s(]+/,
      `CREATE TABLE ${tempTable}`
    );
    await client.command({ query: tempCreateSql });

    // Get all columns
    const colsResult = await client.query({
      query: `SELECT name FROM system.columns WHERE database = currentDatabase() AND table = '${tableName}'`,
      format: "JSONEachRow",
    });
    const colsData = await colsResult.json<{ name: string }>();
    const allColumns = (colsData as { name: string }[]).map((c) =>
      escapeClickHouseIdentifier(c.name)
    );

    // Build swap info map: column -> { otherColumn, probability, randVar }
    const swapInfo = new Map<
      string,
      { otherCol: string; prob: string; randVar: string }
    >();
    swaps.forEach((swap, i) => {
      const col1 = escapeClickHouseIdentifier(swap.column1);
      const col2 = escapeClickHouseIdentifier(swap.column2);
      const prob = String(swap.probability);
      const randVar = `_swap_rand_${String(i)}`;
      swapInfo.set(col1, { otherCol: col2, prob, randVar });
      swapInfo.set(col2, { otherCol: col1, prob, randVar });
    });

    // Build SELECT with swap logic
    // Use _inner. prefix to avoid ClickHouse resolving column names to aliases
    const selectColumns = allColumns.map((col) => {
      const info = swapInfo.get(col);
      if (info) {
        return `if(${info.randVar} < ${info.prob}, _inner.${info.otherCol}, _inner.${col}) as ${col}`;
      }
      return `_inner.${col}`;
    });

    // Build random variable definitions for subquery
    const randVars = swaps
      .map((_, i) => `rand() / 4294967295.0 as _swap_rand_${String(i)}`)
      .join(", ");

    // Use subquery to compute random values once per row
    // Alias the subquery as _inner to avoid column name resolution issues
    const insertSql = `
      INSERT INTO ${tempTable}
      SELECT ${selectColumns.join(", ")}
      FROM (
        SELECT *, ${randVars}
        FROM ${escapedTable}
      ) AS _inner
    `;
    await client.command({ query: insertSql });

    // Swap tables
    await client.command({
      query: `RENAME TABLE ${escapedTable} TO ${oldTable}, ${tempTable} TO ${escapedTable}`,
    });

    // Drop old table
    await client.command({ query: `DROP TABLE ${oldTable}` });
  }

  /**
   * Apply a lookup transformation using ClickHouse's table swap approach.
   * Since ClickHouse doesn't support correlated subqueries in ALTER TABLE UPDATE,
   * we use: CREATE TABLE new -> INSERT SELECT with JOIN -> RENAME swap
   */
  private async applyLookupTransformation(
    tableName: string,
    t: LookupTransformation
  ): Promise<void> {
    const client = this.getClient();
    const escapedTable = escapeClickHouseIdentifier(tableName);
    const escapedCol = escapeClickHouseIdentifier(t.column);
    const fromTable = escapeClickHouseIdentifier(t.fromTable);
    const fromCol = escapeClickHouseIdentifier(t.fromColumn);
    const targetJoinCol = escapeClickHouseIdentifier(t.joinOn.targetColumn);
    const lookupJoinCol = escapeClickHouseIdentifier(t.joinOn.lookupColumn);

    // Use unique suffix to avoid conflicts with concurrent runs
    const uniqueSuffix = `${String(Date.now())}_${Math.random().toString(36).slice(2, 8)}`;
    const tempTableName = `${tableName}_lookup_temp_${uniqueSuffix}`;
    const oldTableName = `${tableName}_lookup_old_${uniqueSuffix}`;
    const escapedTempTable = escapeClickHouseIdentifier(tempTableName);
    const escapedOldTable = escapeClickHouseIdentifier(oldTableName);

    // 1. Create new table with same structure
    await client.command({
      query: `CREATE TABLE ${escapedTempTable} AS ${escapedTable}`,
      clickhouse_settings: { wait_end_of_query: 1 },
    });

    // 2. INSERT SELECT with LEFT JOIN, using REPLACE to update the lookup column
    // The REPLACE syntax replaces the column value in the SELECT *
    const insertSql = `
      INSERT INTO ${escapedTempTable}
      SELECT
        t.* REPLACE (coalesce(s.${fromCol}, t.${escapedCol}) AS ${escapedCol})
      FROM ${escapedTable} t
      LEFT JOIN ${fromTable} s ON t.${targetJoinCol} = s.${lookupJoinCol}
    `;
    await client.command({
      query: insertSql,
      clickhouse_settings: { wait_end_of_query: 1 },
    });

    // 3. RENAME tables to swap (atomic operation)
    // target -> target_old, target_new -> target
    await client.command({
      query: `RENAME TABLE ${escapedTable} TO ${escapedOldTable}, ${escapedTempTable} TO ${escapedTable}`,
      clickhouse_settings: { wait_end_of_query: 1 },
    });

    // 4. Drop the old table
    await client.command({
      query: `DROP TABLE IF EXISTS ${escapedOldTable}`,
      clickhouse_settings: { wait_end_of_query: 1 },
    });
  }
}
