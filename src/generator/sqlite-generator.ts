import Database from "better-sqlite3";
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
import { getLookupTableName } from "./utils.js";

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
          const val = typeof v === "string" ? `'${v.replace(/'/g, "''")}'` : String(v);
          return `WHEN ${String(i)} THEN ${val}`;
        })
        .join(" ");
      return `CASE (abs(random()) % ${String(count)}) ${cases} END`;
    }
    case "choiceByLookup": {
      // SQLite: use json_extract from the CTE json array
      const cteName = getLookupTableName(gen.values);
      const count = gen.values.length;
      return `json_extract(${cteName}.arr, '$[' || (abs(random()) % ${String(count)}) || ']')`;
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

    // Collect choiceByLookup generators for additional CTEs
    const lookupCtes: string[] = [];
    for (const col of table.columns) {
      if (col.generator.kind === "choiceByLookup") {
        const gen = col.generator;
        const cteName = getLookupTableName(gen.values);
        // Store as JSON array
        const jsonArray = JSON.stringify(gen.values);
        lookupCtes.push(
          `${cteName}(arr) AS (SELECT '${jsonArray.replace(/'/g, "''")}')`
        );
      }
    }

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

    // Cross join with lookup CTEs
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

    // Combine all CTEs
    const allCtes = [
      `seq(n) AS (
        SELECT ${String(startSequence)}
        UNION ALL
        SELECT n + 1 FROM seq WHERE n < ${String(startSequence + rowCount - 1)}
      )`,
      ...lookupCtes,
    ];

    // Use recursive CTE to generate sequence
    const insertSql = `
      WITH RECURSIVE ${allCtes.join(", ")}
      INSERT INTO ${table.name} (${columns.join(", ")})
      SELECT ${expressions.join(", ")}
      FROM seq${lookupJoins}
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

  optimize(_tableName: string): Promise<void> {
    const db = this.getDb();
    // VACUUM rebuilds the database file to reclaim space
    db.exec(`VACUUM`);
    // ANALYZE gathers statistics for the query planner
    db.exec(`ANALYZE`);
    return Promise.resolve();
  }

  protected applyTransformations(
    tableName: string,
    transformations: Transformation[]
  ): Promise<void> {
    if (transformations.length === 0) return Promise.resolve();

    const db = this.getDb();
    // SQLite identifier escaping with double quotes
    const escapeId = (name: string): string => `"${name.replace(/"/g, '""')}"`;
    const escapedTable = escapeId(tableName);
    const setClauses: string[] = [];

    for (const t of transformations) {
      switch (t.kind) {
        case "template": {
          const escapedCol = escapeId(t.column);
          // Replace {column_name} with column references
          let expr = `'${t.template.replace(/'/g, "''")}'`;
          const refs = t.template.match(/\{([^}]+)\}/g) ?? [];
          for (const ref of refs) {
            const colName = ref.slice(1, -1);
            const colRef = escapeId(colName);
            // Replace {col} with concatenation
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
          const escapedCol = escapeId(t.column);
          // Random string mutation using SQLite functions
          const { probability, operations } = t;
          // SQLite random() returns int64, abs+modulo to get position
          const randomPos = `(abs(random()) % max(length(${escapedCol}), 1)) + 1`;

          // Build mutation expressions for each operation
          const mutationExprs: Record<MutationOperation, string> = {
            replace: `substr(${escapedCol}, 1, ${randomPos} - 1) || 'X' || substr(${escapedCol}, ${randomPos} + 1)`,
            delete: `substr(${escapedCol}, 1, ${randomPos} - 1) || substr(${escapedCol}, ${randomPos} + 1)`,
            insert: `substr(${escapedCol}, 1, ${randomPos}) || 'X' || substr(${escapedCol}, ${randomPos} + 1)`,
          };

          // Build expression that randomly picks from available operations
          let mutateExpr: string;
          if (operations.length === 1) {
            // We know operations[0] exists since length === 1
            mutateExpr = mutationExprs[operations[0]!]; // eslint-disable-line @typescript-eslint/no-non-null-assertion
          } else {
            // Use modulo to pick operation index, then CASE to select expression
            const opIndex = `(abs(random()) % ${String(operations.length)})`;
            const cases = operations.map((op, i) => {
              if (i === operations.length - 1) {
                return `ELSE ${mutationExprs[op]}`;
              }
              return `WHEN ${opIndex} = ${String(i)} THEN ${mutationExprs[op]}`;
            });
            mutateExpr = `(CASE ${cases.join(" ")} END)`;
          }

          // Use <= for probability to handle edge case where probability = 1.0
          setClauses.push(
            `${escapedCol} = CASE WHEN abs(random()) / 9223372036854775807.0 <= ${String(probability)} THEN ${mutateExpr} ELSE ${escapedCol} END`
          );
          break;
        }
        case "lookup": {
          const escapedCol = escapeId(t.column);
          // Lookup value from another table via join
          // SQLite uses correlated subquery
          const fromTable = escapeId(t.fromTable);
          const fromCol = escapeId(t.fromColumn);
          const targetJoinCol = escapeId(t.joinOn.targetColumn);
          const lookupJoinCol = escapeId(t.joinOn.lookupColumn);

          setClauses.push(
            `${escapedCol} = (SELECT ${fromCol} FROM ${fromTable} WHERE ${lookupJoinCol} = ${escapedTable}.${targetJoinCol})`
          );
          break;
        }
        case "swap": {
          // Swap handled separately - needs both columns in one UPDATE
          break;
        }
      }
    }

    // Handle swap transformations separately (need atomic swap with same random)
    const swapTransformations = transformations.filter(
      (t): t is SwapTransformation => t.kind === "swap"
    );

    for (const swap of swapTransformations) {
      const col1 = escapeId(swap.column1);
      const col2 = escapeId(swap.column2);
      const prob = String(swap.probability);

      // SQLite: random() returns int64, divide by max int64 for 0-1 range
      // SET a = b, b = a works atomically in SQLite (uses old values on right side)
      const swapSql = `
        UPDATE ${escapedTable} SET
          ${col1} = ${col2},
          ${col2} = ${col1}
        WHERE abs(random()) / 9223372036854775807.0 < ${prob}
      `;
      db.exec(swapSql);
    }

    if (setClauses.length === 0) return Promise.resolve();

    const updateSql = `UPDATE ${escapedTable} SET ${setClauses.join(", ")}`;
    db.exec(updateSql);
    return Promise.resolve();
  }
}
