import { describe, it, expect } from "vitest";
import type { GeneratorConfig } from "../src/generator/types.js";
import { generatorToPostgresExpr } from "../src/generator/postgres-generator.js";
import { generatorToClickHouseExpr } from "../src/generator/clickhouse-generator.js";
import { generatorToSqliteExpr } from "../src/generator/sqlite-generator.js";
import { generatorToTrinoExpr } from "../src/generator/trino-generator.js";

// Test data for each generator type
const generatorConfigs: {
  name: string;
  config: GeneratorConfig;
  seqExpr: string;
}[] = [
  {
    name: "sequence with defaults",
    config: { kind: "sequence" },
    seqExpr: "n",
  },
  {
    name: "sequence with custom start and step",
    config: { kind: "sequence", start: 100, step: 10 },
    seqExpr: "n",
  },
  {
    name: "randomInt",
    config: { kind: "randomInt", min: 1, max: 100 },
    seqExpr: "n",
  },
  {
    name: "randomFloat",
    config: { kind: "randomFloat", min: 0.0, max: 1.0, precision: 4 },
    seqExpr: "n",
  },
  {
    name: "randomFloat with default precision",
    config: { kind: "randomFloat", min: 10, max: 50 },
    seqExpr: "n",
  },
  {
    name: "randomString",
    config: { kind: "randomString", length: 16 },
    seqExpr: "n",
  },
  {
    name: "choice with strings",
    config: { kind: "choice", values: ["A", "B", "C"] },
    seqExpr: "n",
  },
  {
    name: "choice with numbers",
    config: { kind: "choice", values: [1, 2, 3, 4, 5] },
    seqExpr: "n",
  },
  {
    name: "constant string",
    config: { kind: "constant", value: "test_value" },
    seqExpr: "n",
  },
  {
    name: "constant number",
    config: { kind: "constant", value: 42 },
    seqExpr: "n",
  },
  {
    name: "datetime with range",
    config: {
      kind: "datetime",
      from: new Date("2020-01-01"),
      to: new Date("2024-01-01"),
    },
    seqExpr: "n",
  },
  {
    name: "uuid",
    config: { kind: "uuid" },
    seqExpr: "n",
  },
  {
    name: "choiceFromTable",
    config: {
      kind: "choiceFromTable",
      values: ["Smith", "Johnson", "Williams"],
    },
    seqExpr: "n",
  },
];

describe("generatorToPostgresExpr", () => {
  it.each(generatorConfigs)(
    "should generate valid SQL for $name",
    ({ config, seqExpr }) => {
      const result = generatorToPostgresExpr(config, seqExpr);
      expect(typeof result).toBe("string");
      expect(result.length).toBeGreaterThan(0);
    }
  );

  it("should generate correct sequence expression", () => {
    expect(
      generatorToPostgresExpr({ kind: "sequence", start: 100, step: 5 }, "n")
    ).toBe("(100 - 1 + n * 5)");
  });

  it("should generate randomInt expression", () => {
    expect(
      generatorToPostgresExpr({ kind: "randomInt", min: 1, max: 10 }, "n")
    ).toBe("floor(random() * (10 - 1 + 1) + 1)::int");
  });

  it("should generate randomFloat expression", () => {
    expect(
      generatorToPostgresExpr(
        { kind: "randomFloat", min: 0, max: 100, precision: 3 },
        "n"
      )
    ).toBe("round((random() * (100 - 0) + 0)::numeric, 3)");
  });

  it("should generate randomString expression", () => {
    expect(
      generatorToPostgresExpr({ kind: "randomString", length: 10 }, "n")
    ).toBe("substr(md5(random()::text || n::text), 1, 10)");
  });

  it("should generate choice expression", () => {
    expect(
      generatorToPostgresExpr({ kind: "choice", values: ["X", "Y", "Z"] }, "n")
    ).toBe("(ARRAY['X', 'Y', 'Z'])[floor(random() * 3 + 1)::int]");
  });

  it("should generate uuid expression", () => {
    expect(generatorToPostgresExpr({ kind: "uuid" }, "n")).toBe(
      "gen_random_uuid()"
    );
  });

  it("should generate datetime expression", () => {
    const from = new Date("2020-01-01T00:00:00.000Z");
    const to = new Date("2021-01-01T00:00:00.000Z");
    const fromTs = Math.floor(from.getTime() / 1000);
    const toTs = Math.floor(to.getTime() / 1000);
    expect(generatorToPostgresExpr({ kind: "datetime", from, to }, "n")).toBe(
      `to_timestamp(${String(fromTs)} + floor(random() * ${String(toTs - fromTs)})::int)`
    );
  });

  it("should generate constant expressions", () => {
    expect(
      generatorToPostgresExpr({ kind: "constant", value: "hello" }, "n")
    ).toBe("'hello'");
    expect(generatorToPostgresExpr({ kind: "constant", value: 42 }, "n")).toBe(
      "42"
    );
    expect(
      generatorToPostgresExpr({ kind: "constant", value: true }, "n")
    ).toBe("true");
    expect(
      generatorToPostgresExpr({ kind: "constant", value: null }, "n")
    ).toBe("null");
  });

  it("should generate choiceFromTable expression", () => {
    const result = generatorToPostgresExpr(
      { kind: "choiceFromTable", values: ["Smith", "Johnson"] },
      "n"
    );
    // Should reference the lookup CTE with array indexing
    expect(result).toMatch(
      /_lookup_[a-f0-9]+\.arr\[floor\(random\(\) \* array_length\(_lookup_[a-f0-9]+\.arr, 1\) \+ 1\)::int\]/
    );
  });
});

describe("generatorToClickHouseExpr", () => {
  it.each(generatorConfigs)(
    "should generate valid SQL for $name",
    ({ config, seqExpr }) => {
      const result = generatorToClickHouseExpr(config, seqExpr);
      expect(typeof result).toBe("string");
      expect(result.length).toBeGreaterThan(0);
    }
  );

  it("should generate correct sequence expression", () => {
    expect(
      generatorToClickHouseExpr(
        { kind: "sequence", start: 50, step: 2 },
        "number"
      )
    ).toBe("(50 - 1 + number * 2)");
  });

  it("should generate randomInt expression", () => {
    expect(
      generatorToClickHouseExpr({ kind: "randomInt", min: 1, max: 10 }, "n")
    ).toBe("toInt32(randUniform(1, 11))");
  });

  it("should generate randomFloat expression", () => {
    expect(
      generatorToClickHouseExpr(
        { kind: "randomFloat", min: 0, max: 100, precision: 3 },
        "n"
      )
    ).toBe("round(randUniform(0, 100), 3)");
  });

  it("should generate randomString expression", () => {
    expect(
      generatorToClickHouseExpr({ kind: "randomString", length: 20 }, "n")
    ).toBe("randomPrintableASCII(20)");
  });

  it("should generate choice expression", () => {
    expect(
      generatorToClickHouseExpr({ kind: "choice", values: ["A", "B"] }, "n")
    ).toBe("['A', 'B'][toUInt32(rand() % 2) + 1]");
  });

  it("should generate uuid expression", () => {
    expect(generatorToClickHouseExpr({ kind: "uuid" }, "n")).toBe(
      "generateUUIDv4()"
    );
  });

  it("should generate datetime expression", () => {
    const from = new Date("2020-01-01T00:00:00.000Z");
    const to = new Date("2021-01-01T00:00:00.000Z");
    const fromTs = Math.floor(from.getTime() / 1000);
    const toTs = Math.floor(to.getTime() / 1000);
    expect(generatorToClickHouseExpr({ kind: "datetime", from, to }, "n")).toBe(
      `toDateTime(${String(fromTs)} + rand() % ${String(toTs - fromTs)})`
    );
  });

  it("should generate constant expressions", () => {
    expect(
      generatorToClickHouseExpr({ kind: "constant", value: "world" }, "n")
    ).toBe("'world'");
    expect(
      generatorToClickHouseExpr({ kind: "constant", value: 99 }, "n")
    ).toBe("99");
  });

  it("should generate choiceFromTable expression", () => {
    const result = generatorToClickHouseExpr(
      { kind: "choiceFromTable", values: ["Smith", "Johnson"] },
      "n"
    );
    // Should reference the array variable with modulo indexing
    expect(result).toMatch(
      /_lookup_[a-f0-9]+_arr\[toUInt32\(rand\(\) % length\(_lookup_[a-f0-9]+_arr\)\) \+ 1\]/
    );
  });
});

describe("generatorToSqliteExpr", () => {
  it.each(generatorConfigs)(
    "should generate valid SQL for $name",
    ({ config, seqExpr }) => {
      const result = generatorToSqliteExpr(config, seqExpr);
      expect(typeof result).toBe("string");
      expect(result.length).toBeGreaterThan(0);
    }
  );

  it("should generate correct sequence expression", () => {
    expect(
      generatorToSqliteExpr({ kind: "sequence", start: 1000, step: 1 }, "n")
    ).toBe("(1000 - 1 + n * 1)");
  });

  it("should generate randomInt expression", () => {
    expect(
      generatorToSqliteExpr({ kind: "randomInt", min: 5, max: 15 }, "n")
    ).toBe("(5 + abs(random()) % (11))");
  });

  it("should generate randomFloat expression", () => {
    expect(
      generatorToSqliteExpr(
        { kind: "randomFloat", min: 0, max: 100, precision: 2 },
        "n"
      )
    ).toBe("round(0 + (abs(random()) / 9223372036854775807.0) * 100, 2)");
  });

  it("should generate randomString expression", () => {
    expect(
      generatorToSqliteExpr({ kind: "randomString", length: 8 }, "n")
    ).toBe("substr(hex(randomblob(4)), 1, 8)");
  });

  it("should generate choice expression", () => {
    expect(
      generatorToSqliteExpr(
        { kind: "choice", values: ["one", "two", "three"] },
        "n"
      )
    ).toBe(
      "CASE (abs(random()) % 3) WHEN 0 THEN 'one' WHEN 1 THEN 'two' WHEN 2 THEN 'three' END"
    );
  });

  it("should generate uuid expression", () => {
    expect(generatorToSqliteExpr({ kind: "uuid" }, "n")).toBe(
      "lower(hex(randomblob(4)) || '-' || hex(randomblob(2)) || '-4' || substr(hex(randomblob(2)),2) || '-' || substr('89ab', abs(random()) % 4 + 1, 1) || substr(hex(randomblob(2)),2) || '-' || hex(randomblob(6)))"
    );
  });

  it("should generate datetime expression", () => {
    const from = new Date("2020-01-01T00:00:00.000Z");
    const to = new Date("2021-01-01T00:00:00.000Z");
    const fromTs = Math.floor(from.getTime() / 1000);
    const toTs = Math.floor(to.getTime() / 1000);
    expect(generatorToSqliteExpr({ kind: "datetime", from, to }, "n")).toBe(
      `datetime(${String(fromTs)} + abs(random()) % ${String(toTs - fromTs)}, 'unixepoch')`
    );
  });

  it("should generate constant expressions", () => {
    expect(
      generatorToSqliteExpr({ kind: "constant", value: "test" }, "n")
    ).toBe("'test'");
    expect(generatorToSqliteExpr({ kind: "constant", value: 123 }, "n")).toBe(
      "123"
    );
  });

  it("should generate choiceFromTable expression", () => {
    const result = generatorToSqliteExpr(
      { kind: "choiceFromTable", values: ["Smith", "Johnson"] },
      "n"
    );
    // Should reference the lookup CTE with JSON extraction
    expect(result).toMatch(
      /json_extract\(_lookup_[a-f0-9]+\.arr, '\$\[' \|\| \(abs\(random\(\)\) % 2\) \|\| '\]'\)/
    );
  });
});

describe("generatorToTrinoExpr", () => {
  it.each(generatorConfigs)(
    "should generate valid SQL for $name",
    ({ config, seqExpr }) => {
      const result = generatorToTrinoExpr(config, seqExpr);
      expect(typeof result).toBe("string");
      expect(result.length).toBeGreaterThan(0);
    }
  );

  it("should generate correct sequence expression", () => {
    expect(
      generatorToTrinoExpr({ kind: "sequence", start: 500, step: 100 }, "n")
    ).toBe("(500 - 1 + n * 100)");
  });

  it("should generate randomInt expression", () => {
    expect(
      generatorToTrinoExpr({ kind: "randomInt", min: 0, max: 50 }, "n")
    ).toBe("CAST(floor(random() * 51 + 0) AS INTEGER)");
  });

  it("should generate randomFloat expression", () => {
    expect(
      generatorToTrinoExpr(
        { kind: "randomFloat", min: 10, max: 20, precision: 4 },
        "n"
      )
    ).toBe("round(random() * 10 + 10, 4)");
  });

  it("should generate randomString expression", () => {
    expect(
      generatorToTrinoExpr({ kind: "randomString", length: 12 }, "n")
    ).toBe("substr(replace(cast(uuid() as varchar), '-', ''), 1, 12)");
  });

  it("should generate choice expression", () => {
    expect(
      generatorToTrinoExpr(
        { kind: "choice", values: ["alpha", "beta", "gamma"] },
        "n"
      )
    ).toBe(
      "element_at(ARRAY['alpha', 'beta', 'gamma'], CAST(floor(random() * 3) + 1 AS INTEGER))"
    );
  });

  it("should generate uuid expression", () => {
    expect(generatorToTrinoExpr({ kind: "uuid" }, "n")).toBe("uuid()");
  });

  it("should generate datetime expression", () => {
    const from = new Date("2020-01-01T00:00:00.000Z");
    const to = new Date("2021-01-01T00:00:00.000Z");
    const fromTs = Math.floor(from.getTime() / 1000);
    const toTs = Math.floor(to.getTime() / 1000);
    expect(generatorToTrinoExpr({ kind: "datetime", from, to }, "n")).toBe(
      `from_unixtime(${String(fromTs)} + CAST(floor(random() * ${String(toTs - fromTs)}) AS BIGINT))`
    );
  });

  it("should generate constant expressions", () => {
    expect(
      generatorToTrinoExpr({ kind: "constant", value: "value" }, "n")
    ).toBe("'value'");
    expect(generatorToTrinoExpr({ kind: "constant", value: 0 }, "n")).toBe("0");
  });

  it("should generate choiceFromTable expression", () => {
    const result = generatorToTrinoExpr(
      { kind: "choiceFromTable", values: ["Smith", "Johnson"] },
      "n"
    );
    // Should reference the lookup CTE with element_at indexing
    expect(result).toMatch(
      /element_at\(_lookup_[a-f0-9]+\.arr, CAST\(floor\(random\(\) \* cardinality\(_lookup_[a-f0-9]+\.arr\)\) \+ 1 AS INTEGER\)\)/
    );
  });
});

describe("SQL expression generators - edge cases", () => {
  it("should handle sequence with only start", () => {
    expect(generatorToPostgresExpr({ kind: "sequence", start: 10 }, "n")).toBe(
      "(10 - 1 + n * 1)"
    );
  });

  it("should handle sequence with only step", () => {
    expect(generatorToPostgresExpr({ kind: "sequence", step: 5 }, "n")).toBe(
      "(1 - 1 + n * 5)"
    );
  });

  it("should handle empty choice array", () => {
    const config: GeneratorConfig = { kind: "choice", values: [] };
    expect(generatorToPostgresExpr(config, "n")).toBe(
      "(ARRAY[])[floor(random() * 0 + 1)::int]"
    );
    expect(generatorToClickHouseExpr(config, "n")).toBe(
      "[][toUInt32(rand() % 0) + 1]"
    );
    expect(generatorToSqliteExpr(config, "n")).toBe(
      "CASE (abs(random()) % 0)  END"
    );
    expect(generatorToTrinoExpr(config, "n")).toBe(
      "element_at(ARRAY[], CAST(floor(random() * 0) + 1 AS INTEGER))"
    );
  });

  it("should handle single-element choice array", () => {
    const config: GeneratorConfig = { kind: "choice", values: ["only"] };
    expect(generatorToPostgresExpr(config, "n")).toBe(
      "(ARRAY['only'])[floor(random() * 1 + 1)::int]"
    );
  });

  it("should handle choice with numeric values", () => {
    expect(
      generatorToPostgresExpr({ kind: "choice", values: [1, 2, 3] }, "n")
    ).toBe("(ARRAY[1, 2, 3])[floor(random() * 3 + 1)::int]");
  });

  it("should use custom seqExpr in expression", () => {
    expect(generatorToPostgresExpr({ kind: "sequence" }, "row_num")).toBe(
      "(1 - 1 + row_num * 1)"
    );
    expect(generatorToClickHouseExpr({ kind: "sequence" }, "number + 1")).toBe(
      "(1 - 1 + number + 1 * 1)"
    );
    expect(generatorToSqliteExpr({ kind: "sequence" }, "seq.n")).toBe(
      "(1 - 1 + seq.n * 1)"
    );
    expect(generatorToTrinoExpr({ kind: "sequence" }, "t.n")).toBe(
      "(1 - 1 + t.n * 1)"
    );
  });

  it("should use seqExpr in randomString for postgres", () => {
    expect(
      generatorToPostgresExpr({ kind: "randomString", length: 5 }, "row_id")
    ).toBe("substr(md5(random()::text || row_id::text), 1, 5)");
  });
});
