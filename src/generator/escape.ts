import format from "pg-format";

/**
 * Escape identifier for PostgreSQL using double quotes.
 * Uses pg-format library for battle-tested escaping.
 */
export function escapePostgresIdentifier(name: string): string {
  return format.ident(name);
}

/**
 * Escape string literal for PostgreSQL.
 * Uses pg-format library for battle-tested escaping.
 */
export function escapePostgresLiteral(value: string): string {
  return format.literal(value);
}

/**
 * Escape identifier for ClickHouse using backticks.
 * Doubles any existing backticks in the name.
 */
export function escapeClickHouseIdentifier(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
}

/**
 * Escape string literal for ClickHouse.
 * Uses backslash escaping for single quotes.
 */
export function escapeClickHouseLiteral(value: string): string {
  return `'${value.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
}

/**
 * Escape identifier for Trino using double quotes.
 * Always quotes the identifier (unlike Postgres, Trino's $ suffix for
 * metadata tables requires quoting).
 * Doubles any existing double quotes in the name.
 */
export function escapeTrinoIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}

/**
 * Escape string literal for Trino.
 * Doubles single quotes.
 */
export function escapeTrinoLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}

/**
 * Escape string literal for SQLite.
 * Doubles single quotes.
 */
export function escapeSqliteLiteral(value: string): string {
  return `'${value.replace(/'/g, "''")}'`;
}
