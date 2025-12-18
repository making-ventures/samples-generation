import Database from "better-sqlite3";
import type {
  TableConfig,
  GeneratedRow,
  ColumnConfig,
  GeneratorConfig,
} from "./types.js";
import { BaseDataGenerator } from "./base-generator.js";

export interface SQLiteConfig {
  path: string;
}

function columnTypeToSqlite(column: ColumnConfig): string {
  switch (column.type) {
    case "integer":
    case "bigint":
      return "INTEGER";
    case "float":
      return "REAL";
    case "string":
      return "TEXT";
    case "boolean":
      return "INTEGER";
    case "datetime":
    case "date":
      return "TEXT";
  }
}

/**
 * Wrap an expression to return NULL with given probability (SQLite)
 */
function wrapWithNullCheck(expr: string, nullProbability: number): string {
  if (nullProbability <= 0) return expr;
  if (nullProbability >= 1) return "NULL";
  // abs(random()) / 9223372036854775807.0 gives 0 to 1
  return `CASE WHEN abs(random()) / 9223372036854775807.0 < ${String(nullProbability)} THEN NULL ELSE ${expr} END`;
}

/**
 * Convert a generator config to a SQLite SQL expression
 * SQLite uses 'n' as the row number from the recursive CTE (1-based)
 */
export function generatorToSqliteExpr(
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
      // abs(random()) gives 0 to 2^63-1, % range gives 0 to range-1
      return `(${String(gen.min)} + abs(random()) % (${String(gen.max - gen.min + 1)}))`;
    case "randomFloat": {
      const precision = gen.precision ?? 2;
      // random() / 9223372036854775807.0 gives -1 to 1, use abs and scale
      return `round(${String(gen.min)} + (abs(random()) / 9223372036854775807.0) * ${String(gen.max - gen.min)}, ${String(precision)})`;
    }
    case "randomString": {
      const len = gen.length;
      // SQLite doesn't have md5, use hex(randomblob) and substr
      return `substr(hex(randomblob(${String(Math.ceil(len / 2))})), 1, ${String(len)})`;
    }
    case "choice": {
      const values = gen.values;
      // Build a CASE expression based on random index
      const count = values.length;
      const cases = values
        .map((v, i) => {
          const val = typeof v === "string" ? `'${v}'` : String(v);
          return `WHEN ${String(i)} THEN ${val}`;
        })
        .join(" ");
      return `CASE (abs(random()) % ${String(count)}) ${cases} END`;
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
      // SQLite stores datetime as ISO string
      return `datetime(${String(fromTs)} + abs(random()) % ${String(toTs - fromTs)}, 'unixepoch')`;
    }
    case "uuid":
      // SQLite doesn't have native UUID, generate a pseudo-UUID from random bytes
      return `lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)),2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) || substr(hex(randomblob(2)),2) || '-' || hex(randomblob(6)))`;
  }
}

export class SQLiteDataGenerator extends BaseDataGenerator {
  readonly name = "sqlite";
  private db: Database.Database | null = null;

  constructor(private config: SQLiteConfig) {
    super();
  }

  connect(): Promise<void> {
    this.db = new Database(this.config.path);
    return Promise.resolve();
  }

  disconnect(): Promise<void> {
    if (this.db) {
      this.db.close();
      this.db = null;
    }
    return Promise.resolve();
  }

  private getDb(): Database.Database {
    if (!this.db) {
      throw new Error("Not connected to SQLite");
    }
    return this.db;
  }

  createTable(table: TableConfig): Promise<void> {
    const db = this.getDb();
    const columns = table.columns
      .map(
        (col) =>
          `${col.name} ${columnTypeToSqlite(col)}${col.nullable ? "" : " NOT NULL"}`
      )
      .join(", ");
    db.exec(`CREATE TABLE IF NOT EXISTS ${table.name} (${columns})`);
    return Promise.resolve();
  }

  truncateTable(tableName: string): Promise<void> {
    const db = this.getDb();
    db.exec(`DELETE FROM ${tableName}`);
    return Promise.resolve();
  }

  dropTable(tableName: string): Promise<void> {
    const db = this.getDb();
    db.exec(`DROP TABLE IF EXISTS ${tableName}`);
    return Promise.resolve();
  }

  protected generateNative(
    table: TableConfig,
    rowCount: number,
    startSequence: number
  ): Promise<void> {
    const db = this.getDb();

    // Build column list and expressions
    const columns = table.columns.map((c) => c.name);
    const seqExpr = "n";
    const expressions = table.columns.map((col) => {
      let expr = generatorToSqliteExpr(col.generator, seqExpr);
      // Apply null probability if specified
      if (col.nullable && col.nullProbability && col.nullProbability > 0) {
        expr = wrapWithNullCheck(expr, col.nullProbability);
      }
      return expr;
    });

    // Use recursive CTE to generate sequence
    // n starts from startSequence
    const insertSql = `
      WITH RECURSIVE seq(n) AS (
        SELECT ${String(startSequence)}
        UNION ALL
        SELECT n + 1 FROM seq WHERE n < ${String(startSequence + rowCount - 1)}
      )
      INSERT INTO ${table.name} (${columns.join(", ")})
      SELECT ${expressions.join(", ")}
      FROM seq
    `;

    db.exec(insertSql);
    return Promise.resolve();
  }

  queryRows(tableName: string, limit = 100): Promise<GeneratedRow[]> {
    const db = this.getDb();
    const result = db
      .prepare(`SELECT * FROM ${tableName} LIMIT ?`)
      .all(limit) as GeneratedRow[];
    return Promise.resolve(result);
  }

  countRows(tableName: string): Promise<number> {
    const db = this.getDb();
    const result = db
      .prepare(`SELECT COUNT(*) as count FROM ${tableName}`)
      .get() as { count: number };
    return Promise.resolve(result.count);
  }

  getMaxValue(tableName: string, columnName: string): Promise<number | null> {
    const db = this.getDb();
    const result = db
      .prepare(`SELECT MAX(${columnName}) as max_val FROM ${tableName}`)
      .get() as { max_val: number | null };
    return Promise.resolve(result.max_val);
  }

  getTableSize(tableName: string): Promise<number | null> {
    const db = this.getDb();
    // Use dbstat virtual table to get table + index size
    try {
      const result = db
        .prepare(
          `SELECT SUM(pgsize) as size FROM dbstat WHERE name = ? OR name IN (SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = ?)`
        )
        .get(tableName, tableName) as { size: number | null };
      return Promise.resolve(result.size);
    } catch {
      // dbstat may not be available, fall back to page-based estimate
      const pageCount = db.prepare(`PRAGMA page_count`).get() as {
        page_count: number;
      };
      const pageSize = db.prepare(`PRAGMA page_size`).get() as {
        page_size: number;
      };
      return Promise.resolve(pageCount.page_count * pageSize.page_size);
    }
  }
}
