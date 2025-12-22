import type {
  DataGenerator,
  TableConfig,
  GenerateOptions,
  GenerateResult,
  GeneratedRow,
  Transformation,
  TransformationBatch,
  TransformResult,
  ScenarioOptions,
  ScenarioResult,
  ScenarioStepResult,
  ScenarioStep,
  ScenarioGenerateStep,
} from "./types.js";
import { formatDuration } from "./utils.js";
import { formatBytes } from "./utils.js";

/** Type guard for generate steps */
function isGenerateStep(step: ScenarioStep): step is ScenarioGenerateStep {
  return "table" in step && "rowCount" in step;
}

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
   * Apply a batch of transformations via UPDATE statement
   */
  protected abstract applyTransformations(
    tableName: string,
    transformations: Transformation[]
  ): Promise<void>;

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
      batchSize: requestedBatchSize,
    } = options;
    const startTime = Date.now();

    // Always use batching internally - default to full rowCount if not specified
    const effectiveBatchSize =
      requestedBatchSize &&
      requestedBatchSize > 0 &&
      requestedBatchSize < rowCount
        ? requestedBatchSize
        : rowCount;
    const batchCount = Math.ceil(rowCount / effectiveBatchSize);
    const showBatchProgress = batchCount > 1;

    const tableLabel = table.description
      ? `${table.name} (${table.description})`
      : table.name;

    console.log(
      `[${this.name}] Generating: ${tableLabel} - ${rowCount.toLocaleString()} rows` +
        (showBatchProgress
          ? ` (${String(batchCount)} batches of ${effectiveBatchSize.toLocaleString()})`
          : "")
    );

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

    // Generate in batches (single batch if batchSize not specified)
    let remaining = rowCount;
    let currentSequence = startSequence;
    let batchNum = 0;
    const batchTimes: number[] = [];

    while (remaining > 0) {
      batchNum++;
      const currentBatchSize = Math.min(effectiveBatchSize, remaining);
      const batchStart = Date.now();

      if (showBatchProgress) {
        // Build progress info: last batch time + ETA (skip for first batch)
        let progressInfo = "";
        if (batchTimes.length > 0) {
          const lastBatchMs = batchTimes[batchTimes.length - 1];
          const avgBatchMs =
            batchTimes.reduce((a, b) => a + b, 0) / batchTimes.length;
          const remainingBatches = batchCount - batchNum + 1;
          const etaMs = avgBatchMs * remainingBatches;
          progressInfo = ` (last: ${formatDuration(lastBatchMs ?? 0)}, ETA: ${formatDuration(etaMs)})`;
        }
        console.log(
          `[${this.name}] Batch ${String(batchNum)}/${String(batchCount)}: ${currentBatchSize.toLocaleString()} rows${progressInfo}`
        );
      }

      await this.generateNative(table, currentBatchSize, currentSequence);

      const batchMs = Date.now() - batchStart;
      batchTimes.push(batchMs);

      // Show last batch duration after final batch
      if (showBatchProgress && remaining <= currentBatchSize) {
        console.log(`[${this.name}] Last batch took: ${formatDuration(batchMs)}`);
      }

      remaining -= currentBatchSize;
      currentSequence += currentBatchSize;
    }

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
      batchCount,
    };
  }

  async transform(
    tableName: string,
    batches: TransformationBatch[]
  ): Promise<TransformResult> {
    const startTime = Date.now();
    let batchesApplied = 0;

    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i]!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
      if (batch.transformations.length > 0) {
        const batchLabel = batch.description ?? `batch ${String(i + 1)}`;
        console.log(
          `[${this.name}] Applying transformations: ${batchLabel} (${String(batch.transformations.length)} transformation(s))`
        );
        await this.applyTransformations(tableName, batch.transformations);
        batchesApplied++;
      }
    }

    return {
      durationMs: Date.now() - startTime,
      batchesApplied,
    };
  }

  async runScenario(options: ScenarioOptions): Promise<ScenarioResult> {
    const startTime = Date.now();
    const {
      scenario,
      createTable = true,
      dropFirst = false,
      truncateFirst = false,
      resumeSequences = true,
      optimize = true,
      batchSize,
    } = options;

    if (scenario.name) {
      console.log(`[${this.name}] Running scenario: ${scenario.name}`);
    }

    const stepResults: ScenarioStepResult[] = [];
    let totalRowsInserted = 0;
    let totalGenerateMs = 0;
    let totalTransformMs = 0;

    for (const step of scenario.steps) {
      if (isGenerateStep(step)) {
        // Generate step: create table and insert rows
        if (dropFirst) {
          await this.dropTable(step.table.name);
        }

        // Don't optimize during steps - we'll optimize all tables at the end
        const generateResult = await this.generate({
          table: step.table,
          rowCount: step.rowCount,
          createTable,
          dropFirst: false, // Already handled above
          truncateFirst,
          resumeSequences,
          optimize: false,
          batchSize,
        });

        // Apply transformations if defined
        let transformResult: TransformResult | undefined;
        if (step.transformations && step.transformations.length > 0) {
          transformResult = await this.transform(
            step.table.name,
            step.transformations
          );
        }

        stepResults.push({
          tableName: step.table.name,
          generate: generateResult,
          transform: transformResult,
        });

        totalRowsInserted += generateResult.rowsInserted;
        totalGenerateMs += generateResult.generateMs;
        if (transformResult) {
          totalTransformMs += transformResult.durationMs;
        }
      } else {
        // Transform-only step: just apply transformations to existing table
        const transformResult = await this.transform(
          step.tableName,
          step.transformations
        );

        stepResults.push({
          tableName: step.tableName,
          transform: transformResult,
        });

        totalTransformMs += transformResult.durationMs;
      }
    }

    // Optimize all touched tables once at the end
    let optimizeMs = 0;
    if (optimize) {
      const optimizeStart = Date.now();
      const uniqueTables = [...new Set(stepResults.map((s) => s.tableName))];
      for (const tableName of uniqueTables) {
        await this.optimize(tableName);
      }
      optimizeMs = Date.now() - optimizeStart;
    }

    return {
      steps: stepResults,
      totalRowsInserted,
      durationMs: Date.now() - startTime,
      generateMs: totalGenerateMs,
      transformMs: totalTransformMs,
      optimizeMs,
    };
  }
}
