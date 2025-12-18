import format from "pg-format";

/**
 * Escape identifier for PostgreSQL using double quotes.
 * Uses pg-format library for battle-tested escaping.
 */
export function escapePostgresIdentifier(name: string): string {
  return format.ident(name);
}

/**
 * Escape identifier for ClickHouse using backticks.
 * Doubles any existing backticks in the name.
 */
export function escapeClickHouseIdentifier(name: string): string {
  return `\`${name.replace(/`/g, "``")}\``;
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
