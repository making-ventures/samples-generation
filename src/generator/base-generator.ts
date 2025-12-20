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
    } = options;
    const startTime = Date.now();

    const tableLabel = table.description
      ? `${table.name} (${table.description})`
      : table.name;
    console.log(
      `[${this.name}] Generating: ${tableLabel} - ${rowCount.toLocaleString()} rows`
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
    } = options;

    if (scenario.name) {
      console.log(`[${this.name}] Running scenario: ${scenario.name}`);
    }

    const stepResults: ScenarioStepResult[] = [];
    let totalRowsInserted = 0;

    for (const step of scenario.steps) {
      if (isGenerateStep(step)) {
        // Generate step: create table and insert rows
        if (dropFirst) {
          await this.dropTable(step.table.name);
        }

        const generateResult = await this.generate({
          table: step.table,
          rowCount: step.rowCount,
          createTable,
          dropFirst: false, // Already handled above
          truncateFirst,
          resumeSequences,
          optimize,
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
      }
    }

    return {
      steps: stepResults,
      totalRowsInserted,
      durationMs: Date.now() - startTime,
    };
  }
}
