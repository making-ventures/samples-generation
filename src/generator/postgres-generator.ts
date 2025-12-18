import postgres, { type Sql } from "postgres";
import type {
  TableConfig,
  GeneratedRow,
  ColumnConfig,
  GeneratorConfig,
} from "./types.js";
import { BaseDataGenerator } from "./base-generator.js";
import { escapePostgresIdentifier } from "./escape.js";

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
      return "NUMERIC";
    case "string":
      return "VARCHAR(255)";
    case "boolean":
      return "BOOLEAN";
    case "datetime":
      return "TIMESTAMP";
    case "date":
      return "DATE";
  }
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

    // Build column list and expressions
    const columns = table.columns.map((c) => escapePostgresIdentifier(c.name));
    const seqExpr = `(n + ${String(startSequence - 1)})`;
    const expressions = table.columns.map((col) => {
      const expr = generatorToPostgresExpr(col.generator, seqExpr);
      // Cast to proper type if needed
      if (col.type === "integer") return `(${expr})::integer`;
      if (col.type === "bigint") return `(${expr})::bigint`;
      if (col.type === "boolean") return `(${expr})::boolean`;
      return expr;
    });

    const insertSql = `
      INSERT INTO ${escapedTableName} (${columns.join(", ")})
      SELECT ${expressions.join(", ")}
      FROM generate_series(1, ${String(rowCount)}) AS n
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
}
