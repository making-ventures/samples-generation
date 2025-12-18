import type {
  DataGenerator,
  TableConfig,
  GenerateOptions,
  GenerateResult,
  GeneratedRow,
} from "./types.js";
import { formatBytes } from "./utils.js";

export abstract class BaseDataGenerator implements DataGenerator {
  abstract readonly name: string;

  abstract connect(): Promise<void>;
  abstract disconnect(): Promise<void>;
  abstract createTable(table: TableConfig): Promise<void>;
  abstract truncateTable(tableName: string): Promise<void>;
  abstract dropTable(tableName: string): Promise<void>;
  abstract queryRows(
    tableName: string,
    limit?: number
  ): Promise<GeneratedRow[]>;
  abstract countRows(tableName: string): Promise<number>;

  /**
   * Get the maximum value of a column (for resuming sequences)
   */
  abstract getMaxValue(
    tableName: string,
    columnName: string
  ): Promise<number | null>;

  /**
   * Get the total size of a table in bytes (including indexes if applicable)
   */
  abstract getTableSize(tableName: string): Promise<number | null>;

  /**
   * Run database-specific optimization after large inserts
   */
  abstract optimize(tableName: string): Promise<void>;

  /**
   * Generate rows using database-native SQL functions.
   * This is much faster than JavaScript-based generation.
   */
  protected abstract generateNative(
    table: TableConfig,
    rowCount: number,
    startSequence: number
  ): Promise<void>;

  /**
   * Get the total size of a table as a human-readable string
   */
  async getTableSizeForHuman(tableName: string): Promise<string | null> {
    const size = await this.getTableSize(tableName);
    return size === null ? null : formatBytes(size);
  }

  async generate(options: GenerateOptions): Promise<GenerateResult> {
    const {
      table,
      rowCount,
      createTable = true,
      dropFirst = false,
      truncateFirst = false,
      resumeSequences = true,
      optimize = true,
    } = options;
    const startTime = Date.now();

    if (dropFirst) {
      await this.dropTable(table.name);
    }

    if (createTable) {
      await this.createTable(table);
    }

    if (truncateFirst && !dropFirst) {
      await this.truncateTable(table.name);
    }

    let startSequence = 1;
    if (resumeSequences) {
      for (const column of table.columns) {
        if (column.generator.kind === "sequence") {
          const maxVal = await this.getMaxValue(table.name, column.name);
          if (maxVal !== null) {
            const step = column.generator.step ?? 1;
            startSequence = maxVal + step;
          }
          break; // Only check first sequence column
        }
      }
    }

    await this.generateNative(table, rowCount, startSequence);
    const generateMs = Date.now() - startTime;

    let optimizeMs = 0;
    if (optimize) {
      const optimizeStart = Date.now();
      await this.optimize(table.name);
      optimizeMs = Date.now() - optimizeStart;
    }

    return {
      rowsInserted: rowCount,
      durationMs: Date.now() - startTime,
      generateMs,
      optimizeMs,
    };
  }
}
