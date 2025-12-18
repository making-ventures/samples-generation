import { createClient, type ClickHouseClient } from "@clickhouse/client";
import type {
  TableConfig,
  GeneratedRow,
  ColumnConfig,
  GeneratorConfig,
} from "./types.js";
import { BaseDataGenerator } from "./base-generator.js";
import { escapeClickHouseIdentifier } from "./escape.js";

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
      // Increase timeout for large data generation (30 minutes)
      request_timeout: 1_800_000,
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

    const insertSql = `
      INSERT INTO ${escapedTableName} (${columns.join(", ")})
      SELECT ${expressions.join(", ")}
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
}
