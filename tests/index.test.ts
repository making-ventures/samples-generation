import { describe, it, expect } from "vitest";
import { generateSample } from "../src/index.js";

describe("generateSample", () => {
  it("should return sample data string", () => {
    const result = generateSample();
    expect(result).toBe("Sample data generated");
  });
});
