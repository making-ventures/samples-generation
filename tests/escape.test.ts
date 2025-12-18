import { describe, it, expect } from "vitest";
import {
  escapePostgresIdentifier,
  escapeClickHouseIdentifier,
  escapeTrinoIdentifier,
} from "../src/generator/escape.js";

describe("escapePostgresIdentifier", () => {
  it("should return simple identifiers unquoted", () => {
    expect(escapePostgresIdentifier("users")).toBe("users");
  });

  it("should quote identifiers with special characters", () => {
    expect(escapePostgresIdentifier("my-table")).toBe('"my-table"');
    expect(escapePostgresIdentifier("table name")).toBe('"table name"');
    expect(escapePostgresIdentifier("table.name")).toBe('"table.name"');
  });

  it("should escape double quotes by doubling them", () => {
    expect(escapePostgresIdentifier('table"name')).toBe('"table""name"');
  });

  it("should handle multiple double quotes", () => {
    expect(escapePostgresIdentifier('a"b"c')).toBe('"a""b""c"');
  });

  it("should quote reserved words", () => {
    // pg-format quotes reserved words
    expect(escapePostgresIdentifier("select")).toBe('"select"');
    expect(escapePostgresIdentifier("from")).toBe('"from"');
  });
});

describe("escapeClickHouseIdentifier", () => {
  it("should wrap identifier in backticks", () => {
    expect(escapeClickHouseIdentifier("users")).toBe("`users`");
  });

  it("should escape backticks by doubling them", () => {
    expect(escapeClickHouseIdentifier("my`table")).toBe("`my``table`");
  });

  it("should handle multiple backticks", () => {
    expect(escapeClickHouseIdentifier("a`b`c")).toBe("`a``b``c`");
  });

  it("should handle empty string", () => {
    expect(escapeClickHouseIdentifier("")).toBe("``");
  });

  it("should handle special characters", () => {
    expect(escapeClickHouseIdentifier("table-name")).toBe("`table-name`");
    expect(escapeClickHouseIdentifier("table name")).toBe("`table name`");
    expect(escapeClickHouseIdentifier("table.name")).toBe("`table.name`");
  });

  it("should handle reserved words", () => {
    expect(escapeClickHouseIdentifier("select")).toBe("`select`");
    expect(escapeClickHouseIdentifier("from")).toBe("`from`");
  });
});

describe("escapeTrinoIdentifier", () => {
  it("should wrap identifier in double quotes", () => {
    expect(escapeTrinoIdentifier("users")).toBe('"users"');
  });

  it("should escape double quotes by doubling them", () => {
    expect(escapeTrinoIdentifier('my"table')).toBe('"my""table"');
  });

  it("should handle multiple double quotes", () => {
    expect(escapeTrinoIdentifier('a"b"c')).toBe('"a""b""c"');
  });

  it("should handle empty string", () => {
    expect(escapeTrinoIdentifier("")).toBe('""');
  });

  it("should handle $ suffix for metadata tables", () => {
    expect(escapeTrinoIdentifier("samples$files")).toBe('"samples$files"');
    expect(escapeTrinoIdentifier("samples$snapshots")).toBe(
      '"samples$snapshots"'
    );
  });

  it("should handle reserved words", () => {
    expect(escapeTrinoIdentifier("select")).toBe('"select"');
    expect(escapeTrinoIdentifier("from")).toBe('"from"');
  });
});
